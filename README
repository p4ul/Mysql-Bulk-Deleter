# MYSQL Bulk Deleter #

Ever wanted to delete a tonne of rows from a live database but 
ended up crying in the corner like a little baby? 

If the answer is yes then this may be the solution for you.

_Please note this has only been tested in Ubuntu_

## To use this tool ## 
1. copy template.job to a new file i.e. good.job
2. fill in database connection details
3. fill in sqlQueryRecon with SQL to find the minimum id of the data you want to keep
4. fill in sqlQueryDelete to accept the id genereated in 3 and delete everything below it  
5. tune timeouts i.e. deleteing in batches of > 5000 kills my server
6. pass the filename of your job into the script `python mysql_bulk_deleter.py good.job`
7. Ctrl + C will cancel this script in linux


## What this tool does ##
Deletes rows from mysql in batches, it has small and big sleeps so that anything accessing the database can 
still hopefully run as normal. Ideal for leaving overnight when you have the parameters tuned correctly and 
millions of rows to delete on a  prod system.

## What this tool doesnt ##
monitor mysql performance, load average etc 

## Notes ##
* When using this tool I highly recommend having mtop or innotop running while you tune your delete parameters.
* Database Delete Time Remaining does not currently include big and small wait times.



