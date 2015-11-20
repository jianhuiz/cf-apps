'''
Created on 2014-3-1

@author: 
'''
import commands
import getopt
import sys
import os

sys.path.append(sys.path[0] + "/../../")
from script.util.Common import ClusterCommand, DefaultValue
from script.util.DbClusterStatus import DbClusterStatus
from script.util.GaussLog import GaussLog
from script.util.OMCommand import LocalCommand

class BackupStatus(LocalCommand):
    """
    Backup or restore the status of instances
    """
    def __init__(self, logFile, clusterConf, bakDir):
        LocalCommand.__init__(self, logFile, clusterConf)
        self.__bakDir = bakDir
        self.__bakStatusFile = os.path.join(bakDir, "node_status.bak")
        self.__curStatusFile = os.path.join(bakDir, "node_status.cur")

    def doBackup(self):
        """
        Backup the status of instances
        """
        self.logger.log("Begin to backup instance status...")
        
        try:
            self.readConfigInfo()
            self.getUserInfo()
            
            # dump status to file
            cmd = ClusterCommand.getQueryStatusCmd(self.user, self.dbNodeInfo.id, self.__bakStatusFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Query local instance status failed!Error: %s" % output)
        except Exception, e:
            self.logger.logExit(str(e))
            
        self.logger.log("Backup instance status successfully.")
        self.logger.closeLog()
            
    def doRestore(self):
        """
        Restore the status of instances
        """
        self.logger.log("Begin to restore instance status...")
        
        try:
            self.readConfigInfo()
            self.getUserInfo()
            
            # dump status to file
            cmd = ClusterCommand.getQueryStatusCmd(self.user, self.dbNodeInfo.id, self.__curStatusFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Query local instance status failed!Error: %s" % output)
                
            bakDbStatus = DbClusterStatus()
            bakDbStatus.initFromFile(self.__bakStatusFile)
            bakNodeStatus = bakDbStatus.getDbNodeStatusById(self.dbNodeInfo.id)
            if (bakNodeStatus is None):
                self.logger.logExit("Get backup status of local node failed!")
            
            curDbStatus = DbClusterStatus()
            curDbStatus.initFromFile(self.__curStatusFile)
            curNodeStatus = curDbStatus.getDbNodeStatusById(self.dbNodeInfo.id)
            if (curNodeStatus is None):
                self.logger.logExit("Get current status of local node failed!")
            if (not curNodeStatus.isNodeHealthy()):
                self.logger.logExit("Current status of node is not healthy!")
            
            # Compare the status and restore it
            bakInstances = bakNodeStatus.datanodes + bakNodeStatus.gtms
            for bakInst in bakInstances:
                curInst = curNodeStatus.getInstanceByDir(bakInst.datadir)
                if (curInst is None):
                    self.logger.logExit("Get current status of instance failed!DataDir:%s" % bakInst.datadir)
                
                if (bakInst.status == curInst.status):
                    continue
                
                if (bakInst.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                    self.__switchToPrimary(bakInst.datadir)
                elif (bakInst.status == DbClusterStatus.INSTANCE_STATUS_STANDBY):
                    self.__switchToStandby(bakInst.datadir)
            
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.log("Restore instance status successfully.")
        self.logger.closeLog()
        
    def __switchToPrimary(self, datadir):
        """
        Switch local instance to be primary
        """
        cmd = ClusterCommand.getSwitchOverCmd(self.user, self.dbNodeInfo.id, datadir)
        self.logger.debug("Switch to primary: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Switch instance to be primary failed!Datadir %s.\nOutput: %s" % (datadir, output))
        
    def __switchToStandby(self, datadir):
        """
        Switch local instance to be standby
        """
        pass
        
def usage():
    """
Usage:
    python BackupStatus.py --help
    python BackupStatus.py {-b | -r} -P bakDir -X confFile [-l log]
    """
    
    print usage.__doc__
    
def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "P:X:l:brh", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    bakDir = ""
    doBackup = False
    doRestore = False
    confFile = DefaultValue.CLUSTER_CONFIG_PATH
    logFile = ""
    
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-X"):
            confFile = value
        elif (key == "-P"):
            bakDir = value
        elif (key == "-l"):
            logFile = value
        elif (key == "-r"):
            doRestore = True
        elif (key == "-b"):
            doBackup = True
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)

    if (not os.path.isabs(confFile)):
        GaussLog.exitWithError("Parameter input error, configure file need absolute path.")
        
    if (not os.path.isdir(bakDir)):
        GaussLog.exitWithError("Parameter input error, backup directory does not exist.")
        
    if (not doBackup and not doRestore):
        GaussLog.exitWithError("Parameter input error, need '-r' or '-b' parameter.")
    
    if(logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", "", confFile)
            
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log file need absolute path.")
        
    backuper = BackupStatus(logFile, confFile, bakDir)
    if (doBackup):
        backuper.doBackup()
    elif (doRestore):
        backuper.doRestore()
        
    sys.exit(0)

if __name__ == '__main__':
    main()
