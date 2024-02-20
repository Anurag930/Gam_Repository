import datetime

# append in the logs in single log file with proper run Timestamps.
def task():
    file=open(r'C:\Users\anurag.sharma\Documents\test_python_scripts\GAM\pythontask','a')
    file.write(f'{datetime.datetime.now()}- The script ran \n')

