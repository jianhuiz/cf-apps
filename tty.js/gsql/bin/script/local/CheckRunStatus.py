import commands
import getopt
import os
import sys

sys.path.append(sys.path[0] + "/../../")
try:
    from script.util.Common import DefaultValue
    from script.util.GaussLog import GaussLog
    from script.util.OMCommand import LocalCommand
except ImportError, e:
    sys.exit("ERROR: Cannot import modules: %s" % str(e))

class CheckRunStatus(LocalCommand):
    """
    Check the instance status on local node
    """
    def __init__(self, logFile, clusterConf, isRunning):
        LocalCommand.__init__(self, logFile, clusterConf)
        self.isRunning = isRunning

    def run(self):
        """
        Check the instance status on local node
        """
        self.logger.log("Begin to check instance status...")
        
        try:
            self.readConfigInfo()
            self.getUserInfo()
            
            for dbInst in self.dbNodeInfo.gtms:
                cmd = "su - %s -c 'gtm_ctl status -D %s'" % (self.user, dbInst.datadir)
                self.logger.debug("Check status cmd:%s" % cmd)
                (status, output) = commands.getstatusoutput(cmd)
                if (self.isRunning != (status == 0)):
                    self.logger.debug("Output:%s" % output)
                    self.logger.log("The status is not expected status!")
                    sys.exit(2)
            
            instances = self.dbNodeInfo.coordinators + self.dbNodeInfo.datanodes
            for dbInst in instances:
                cmd = "su - %s -c 'gs_ctl status -D %s'" % (self.user, dbInst.datadir)
                self.logger.debug("Check status cmd:%s" % cmd)
                (status, output) = commands.getstatusoutput(cmd)
                if (self.isRunning != (status == 0)):
                    self.logger.debug("Output:%s" % output)
                    self.logger.log("The status is not expected status!")
                    sys.exit(2)

        except Exception, e:
            self.logger.logExit("Check instance status failed!Error:%s" % str(e))
            
        self.logger.log("Check instance status finished.")
        self.logger.closeLog()
        
def usage():
    """
Usage:
    python CheckRunStatus.py -h | --help
    python CheckRunStatus.py -X confFile [-l log]
    """
    
    print usage.__doc__
    
def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "X:l:h", ["running", "stopped" ,"help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError(str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    confFile = DefaultValue.CLUSTER_CONFIG_PATH
    logFile = ""
    isRunning = True
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-X"):
            confFile = value
        elif (key == "-l"):
            logFile = value
        elif (key == "--running"):
            isRunning = True
        elif (key == "--stopped"):
            isRunning = False
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)

    if (not os.path.isabs(confFile)):
        GaussLog.exitWithError("Parameter input error, configure file need absolute path.")
        
    if(logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", "", confFile)

    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log file need absolute path.")
        
    checker = CheckRunStatus(logFile, confFile, isRunning)
    checker.run()

    sys.exit(0)

if __name__ == '__main__':
    main()
    