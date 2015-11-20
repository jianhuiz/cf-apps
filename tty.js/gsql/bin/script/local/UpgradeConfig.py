'''
Created on 2014-3-8

@author: 
'''

import commands
import getopt
import socket
import sys
import os

sys.path.append(sys.path[0] + "/../../")
from script.util.Common import *
from script.util.DbClusterInfo import *
from script.util.GaussLog import GaussLog
#############################################################################
# Global variables
#############################################################################
g_logger = None
g_clusterUser = ""

class UpgradeConfiguration():
    """
    Upgrade instance configuration on local node
    """
    def __init__(self):
        self.__clusterInfo = None
        self.__dbNodeInfo = None
        self.__user = ""
        self.__group = ""

    def run(self):
        """
        Upgrade instance configuration on local node
        """
        self.__readConfigInfo()
        self.__upgradeInstanceConf()
    
    def __readConfigInfo(self):
        """
        Read config from xml file
        """
        g_logger.log("Read static config file...")
        try:
            self.__clusterInfo = dbClusterInfo()
            self.__clusterInfo.initFromStaticConfig(g_clusterUser)
            g_logger.debug("Get host name...")
            hostName = socket.gethostname()
            g_logger.debug("Get dbNode...")
            self.__dbNodeInfo = self.__clusterInfo.getDbNodeByName(hostName)
            if (self.__dbNodeInfo is None):
                g_logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            g_logger.logExit(str(e))
        
        g_logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))
        
    def __upgradeInstanceConf(self):
        """
        Read new configuration and modify each instance
        """
        pass

def usage():
    """
Usage:
    python UpgradeConfig.py --help
    python UpgradeConfig.py -U user [-l log]
    """
    
    print usage.__doc__
    
def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "U:l:", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

    global g_clusterUser
    logFile = ""
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-l"):
            logFile = value
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)

    # check if user exist and is the right user
    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)

    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")
          
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log file need absolute path.")

    # Init logger
    global g_logger
    g_logger = GaussLog(logFile, "UpgradeConfig")
    try:    
        upgrader = UpgradeConfiguration()
        upgrader.run()
        
        g_logger.log("Upgrade config on node[%s] successfully!" % socket.gethostname())
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.logExit(str(e))

if __name__ == '__main__':
    main()
