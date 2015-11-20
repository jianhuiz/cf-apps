'''
Created on 2014-3-27

@author: 
'''
import getopt
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.append(sys.path[0] + "/../../")
try:
    from script.util.Common import DefaultValue, ClusterCommand, PlatformCommand
    from script.util.GaussLog import GaussLog
    from script.util.OMCommand import CommandThread
except ImportError, e:
    sys.exit("ERROR: Cannot import modules: %s" % str(e))

g_logger = None

def usage():
    """
    python LockCluster -h | --help
    python LockCluster -U user [-T time_out] [-l log]
    """
    
    print usage.__doc__
    
def writePid(user):
    """
    Write pid to file
    """
    pid = "%d" % os.getpid()
    tmpDir = DefaultValue.getTmpDirFromEnv()
    clusterLockFilePath = "%s/%s" % (tmpDir, DefaultValue.CLUSTER_LOCK_PID)
    PlatformCommand.WriteInfoToFile(clusterLockFilePath, pid)

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "T:U:l:h", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
    
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
        
    time_out = 0
    user = ""
    logFile = ""
    
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-T"):
            try:
                time_out = int(value)
            except Exception, e:
                GaussLog.exitWithError("Parameter input error: %s" % str(e))
        elif (key == "-U"):
            user = value
        elif (key == "-l"):
            logFile = value
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)
            
    if (user == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, user, "")
    
    writePid(user)
    global g_logger
    g_logger = GaussLog(logFile)
    
    if (time_out <= 0):
        time_out = 1800
    endTime = datetime.now() + timedelta(seconds=time_out)

    connList = ClusterCommand.readCooConnections(user)
    if (len(connList) == 0):
        raise Exception("There is no coordinator to connect!")
    ip = connList[0][0]
    port = connList[0][1]
    
    sql = "select case (select pgxc_lock_for_backup()) when true then (select pg_sleep(%d)) end;" % time_out
    cmd = "gsql -h %s -p %d postgres -X -c \"%s\"" % (ip, port, sql)
    cmd = "su - %s -c '%s'" % (user, cmd)
    sqlThread = CommandThread(cmd)
    sqlThread.start()
    time.sleep(5)
    while True:
        td = endTime - datetime.now()
        leftSeconds = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
        if (leftSeconds <= 0):
            if (sqlThread.isAlive()):
                g_logger.log("Timeout!Release the cluster!")
                g_logger.closeLog()
                sys.exit(0)
            
        if (not sqlThread.isAlive()):
            if (sqlThread.cmdStauts != 0):
                g_logger.logExit("Lock cluster failed!Output: %s" % sqlThread.cmdOutput)
            else:
                g_logger.log("Execute SQL finished!Release the cluster!")
                g_logger.closeLog()
                sys.exit(0)
        
        g_logger.log("Thread is still alive!Left seconds[%s]" % leftSeconds)
        time.sleep(10)
        
if __name__ == '__main__':
    main()
