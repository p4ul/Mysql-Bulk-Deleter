'''
Created on 8/02/2011
@version 0.9 
@author: paul
'''

from configobj import ConfigObj, ConfigObjError
import sys
import collections
import time
import curses
import MySQLdb as mdb


class Timer():
   def __enter__(self): self.start = time.time()
   def __exit__(self, *args): print round(time.time() - self.start,4)


def report_progress(stdscr,percent, rowsDeleted, message, avgTime,maxDeleteRows, rowLimit):
   
    stdscr.addstr(1, 0, "Average Query Time: {0}                              ".format(avgTime))
    stdscr.addstr(2, 0, "Total progress: [{1:100}] {0}%".format( round(percent,2), "#" * int(percent)))
    stdscr.addstr(3, 0, "Rows Deleted: {0} ".format(rowsDeleted))
    stdscr.addstr(4, 0, "Status: {0}                          ".format(message))
    stdscr.addstr(5, 0, "")
    stdscr.addstr(6, 0, "total to be deleted {0}  ".format(maxDeleteRows))
    stdscr.addstr(7, 0, "Database Delete Time Remaining: {0} mins           ".format(int( ((maxDeleteRows-rowsDeleted)/ int(rowLimit) * avgTime) / 60)))
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
        maxDeleteRows = config['maxDeleteRows']
        hostName     = config['hostName']
        databaseName = config['databaseName']
        databaseUser = config['databaseUser']
        databasePass = config['databasePass']
        rowLimit     = config['rowLimit']
        
        conn = mdb.connect(hostName,databaseUser, databasePass, databaseName);
        
        cursor = conn.cursor()
        
        cursor.execute(config['sqlQueryRecon'])
        
        stdscr.addstr(1, 0, "performing recon on what to delete")
        stdscr.refresh()
        
        deleteProperty = cursor.fetchone() #.
        maxVal    = deleteProperty[0]
        delCount = deleteProperty[1]
        
        
        stdscr.addstr(1, 0, 'deleteing using val ' + str(maxVal) + ', num rows ' + str(delCount))
        stdscr.refresh()
        
        
        if delCount < maxDeleteRows :
            maxDeleteRows = delCount
        
        seq = []
        
        with Timer():
            while rowsDeleted < maxDeleteRows :
                start = time.time()
                cursor.execute(config['sqlQueryDelete'] + " limit " + rowLimit,(maxVal))
                timeTaken = round(time.time() - start,4)
    
                seq.append(timeTaken)
                
                #only need so many items to work out average
                if len(seq) > 100:
                    seq.pop(0)
                
                numDeleted = cursor.rowcount
                rowsDeleted += numDeleted
                
                percent = float(rowsDeleted) / float(maxDeleteRows) * 100
                message = '-'
                wait = 1
                    
                if rowsDeleted % long(config['smallTimeoutAfter']) == 0 :
                    message = "small wait"
                    wait += float(config['smallTimeoutSecs'])
                    
                if rowsDeleted % long(config['bigTimeoutAfter']) == 0 :
                    message = "big wait "
                    wait += float(config['bigTimeoutSecs'])
                
                avgTime = round(float(sum(seq)) / len(seq),4)
                    
                #print percent
                '''
                sys.stdout.write("%3d%%  " % percent)
                sys.stdout.write("%s " % rowsDeleted)
                sys.stdout.write("%s " % message)
                sys.stdout.write("\r\r avg time %s " % avgTime)
                sys.stdout.flush()
                '''
                
                report_progress(stdscr, percent, rowsDeleted, message, avgTime,maxDeleteRows, rowLimit)
                
                time.sleep(wait)
                            
            
        print "total Rows deleted : %s " % rowsDeleted
        
        cursor.close()
        conn.close()
        print "finished"
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)


if __name__ == "__main__":
    #stdscr = curses.initscr()
    curses.wrapper(bulk_delete)
    #curses.noecho()
    #curses.cbreak()
    

