'''
Created on 8/02/2011
@version 1.0
@author: paul
'''

from configobj import ConfigObj, ConfigObjError
import sys
import collections
import time
import curses
import MySQLdb as mdb
from math import floor


def nice_date(time_in_seconds):
    time_in_seconds = int(time_in_seconds)

    hours = time_in_seconds / (60 * 60)
    time_in_seconds -= hours * (60 * 60)

    minutes = time_in_seconds / 60
    time_in_seconds -= minutes * (60)

    seconds = time_in_seconds

    output = ''

    if hours:
        output += str(hours) + ' hour'
        output += 's' if hours > 1 else ''
    if minutes:
        output += " " + str(minutes) + ' minute'
        output += 's' if minutes > 1 else ''
    if seconds:
        output += " " + str(seconds) + ' second'
        output += 's' if seconds > 1 else ''
    return output


class Timer():
    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print round(time.time() - self.start, 4)


def calculate_remaining_time(maxDeleteRows, rowsDeleted, rowLimit, avgTime, config):
    rowsLeft = maxDeleteRows - rowsDeleted
    #query time
    rt = int(rowsLeft / int(rowLimit) * avgTime)
    #small wait time
    rt += floor(rowsLeft / int(config['smallTimeoutAfter'])) * float(config['smallTimeoutSecs'])
    #large wait time
    rt += floor(rowsLeft / int(config['bigTimeoutAfter'])) * float(config['bigTimeoutSecs'])
    return rt


def report_progress(stdscr, percent, rowsDeleted, message, avgTime, maxDeleteRows, rowLimit, config):
    remainingTime = calculate_remaining_time(maxDeleteRows, rowsDeleted, rowLimit, avgTime, config)
    #the whitespaces in this string are to clear out old text
    stdscr.addstr(1, 0, "Average Query Time: {0}                              ".format(avgTime))
    stdscr.addstr(2, 0, "Total progress: [{1:100}] {0}%".format(round(percent, 2), "#" * int(percent)))
    stdscr.addstr(3, 0, "Rows Deleted: {0} ".format(rowsDeleted))
    stdscr.addstr(4, 0, "Status: {0}                          ".format(message))
    stdscr.addstr(5, 0, "")
    stdscr.addstr(6, 0, "total to be deleted {0}  ".format(maxDeleteRows))
    stdscr.addstr(7, 0, "Database Delete Time Remaining: {0}            ".format(nice_date(remainingTime)))
    stdscr.addstr(8, 0, "")
    stdscr.refresh()


def bulk_delete(stdscr):
    try:
        #get command line arguments
        arg_names = ['command', 'configFilename']
        args = dict(zip(arg_names, sys.argv))
        Arg_list = collections.namedtuple('Arg_list', arg_names)
        args = Arg_list(*(args.get(arg, None) for arg in arg_names))
        try:
            config = ConfigObj(args.configFilename, file_error=True)
            stdscr.addstr(1, 0, "opened " + args.configFilename)
            stdscr.refresh()
        except (ConfigObjError, IOError), e:
            print 'Could not read "%s": %s. Please pass in a valid filename.job' % (args.configFilename, e)
            exit(1)
        rowsDeleted = 0
        maxDeleteRows = int(config['maxDeleteRows'])
        hostName = config['hostName']
        databaseName = config['databaseName']
        databaseUser = config['databaseUser']
        databasePass = config['databasePass']
        rowLimit = config['rowLimit']

        conn = mdb.connect(hostName, databaseUser, databasePass, databaseName)

        cursor = conn.cursor()

        stdscr.addstr(2, 0, "finding out what to delete")
        stdscr.refresh()
        cursor.execute(config['sqlQueryRecon'])

        deleteProperty = cursor.fetchone()
        maxVal = deleteProperty[0]
        delCount = deleteProperty[1]

        stdscr.addstr(3, 0, 'deleteing using val ' + str(maxVal) + ', num rows ' + str(delCount))
        stdscr.refresh()

        if int(delCount) < int(maxDeleteRows):
            maxDeleteRows = int(delCount)

        seq = []

        time.sleep(int(config['smallTimeoutSecs']))

        with Timer():
            while rowsDeleted < (maxDeleteRows - 1):
                start = time.time()
                cursor.execute(config['sqlQueryDelete'] + " limit " + rowLimit, (maxVal))
                timeTaken = round(time.time() - start, 4)

                seq.append(timeTaken)

                #only need so many items to work out average
                if len(seq) > 1000:
                    seq.pop(0)

                numDeleted = cursor.rowcount
                rowsDeleted += numDeleted

                percent = float(rowsDeleted) / float(maxDeleteRows) * 100
                message = '-'
                wait = 1

                if rowsDeleted % long(config['smallTimeoutAfter']) == 0:
                    message = "small wait"
                    wait += float(config['smallTimeoutSecs'])

                if rowsDeleted % long(config['bigTimeoutAfter']) == 0:
                    message = "big wait "
                    wait += float(config['bigTimeoutSecs'])

                avgTime = round(float(sum(seq)) / len(seq), 4)

                #print percent
                '''
                sys.stdout.write("%3d%%  " % percent)
                sys.stdout.write("%s " % rowsDeleted)
                sys.stdout.write("%s " % message)
                sys.stdout.write("\r\r avg time %s " % avgTime)
                sys.stdout.flush()
                '''
                report_progress(stdscr, percent, rowsDeleted, message, avgTime, maxDeleteRows, rowLimit, config)
                time.sleep(wait)

            curses.endwin()
            cursor.close()
            conn.close()
        print "total Rows deleted : %s " % rowsDeleted
        print "finished"
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)


if __name__ == "__main__":
    #stdscr = curses.initscr()
    curses.wrapper(bulk_delete)
    #curses.noecho()
    #curses.cbreak()