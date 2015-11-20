'''
Created on 2014-2-15

@author: 
'''

import commands
import getopt
import socket
import sys
import os

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.Common import DefaultValue, PlatformCommand
#############################################################################
# Global variables
#############################################################################
g_logger = None
g_clusterUser = ""

class initDbNode():
    """
    Init all instance on local node
    """
    def __init__(self, dbParams, gtmParams):
        self.__dbInitParams = dbParams[:]
        self.__gtmInitParams = gtmParams[:]
        self.__clusterInfo = None
        self.__dbNodeInfo = None
        self.__user = ""
        self.__group = ""

    def run(self):
        """
        Init instances
        """
        self.__checkParameters()
        self.__readConfigInfo()
        self.__getUserInfo()
        self.__initNode()
        
    def __checkParameters(self):
        """
        Check parameters for initdb
        """
        g_logger.log("Checking parameters for initdb...")
        for param in self.__dbInitParams:
            if (self.__checkInitdbParams(param.strip()) != 0):
                g_logger.logExit("Parameter input error: %s." % param)
        
        g_logger.log("Checking parameters for initgtm...")
        for param in self.__gtmInitParams:
            if (self.__checkInitdbParams(param.strip()) != 0):
                g_logger.logExit("Parameter input error: %s." % param)
    
    def __checkInitdbParams(self, param):
        """
        Check parameter for initdb
            -D, --pgdata : this has been specified in config file
            -W, --pwprompt: this will block the script
            --pwfile: it is not safe to read password from file
            -A, --auth,--auth-local,--auth-host: They will be used with '--pwfile'
            -c, --enpasswd: this will confuse the default password in script with the password user specified
            -Z: this has been designated internal
            -U --username: use the user specified during install step
        """
        shortInvalidArgs = ("-D", "-W", "-C", "-A", "-Z", "-U", "-X")
        longInvalidArgs = ("--pgdata", "--pwprompt", "--enpasswd", "--pwfile", "--auth", "--auth-host", "--auth-local", "--username", "--xlogdir")
        
        argList = param.split()
        for arg in shortInvalidArgs:
            if (arg in argList):
                return 1
        argList = param.split("=")
        for arg in longInvalidArgs:
            if (arg in argList):
                return 1
                
        return 0
    
    def __readConfigInfo(self):
        """
        Read config from static config file
        """
        try:
            self.__clusterInfo = dbClusterInfo()
            self.__clusterInfo.initFromStaticConfig(g_clusterUser)
            hostName = socket.gethostname()
            self.__dbNodeInfo = self.__clusterInfo.getDbNodeByName(hostName)
            if (self.__dbNodeInfo is None):
                g_logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
                
            g_logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))
        except Exception, e:
            g_logger.logExit(str(e))
    
    def __getUserInfo(self):
        """
        Get user and group
        """
        g_logger.log("Getting user and group for application...")
        cmd = "stat -c '%%U:%%G' %s" % self.__clusterInfo.appPath
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Get user info failed!Error :%s" % output)
        
        userInfo = output.split(":")
        if (len(userInfo) != 2):
            g_logger.logExit("Get user info failed!Error :%s" % output)
        
        self.__user = userInfo[0]
        self.__group = userInfo[1]
        
    def __initNode(self):
        """
        Init instances on local node
        """
        g_logger.log("Init cm servers...")
        for cmsInst in self.__dbNodeInfo.cmservers:
            self.__initInstance(cmsInst)
        
        g_logger.log("Init cm agent...")
        for cmaInst in self.__dbNodeInfo.cmagents:
            self.__initInstance(cmaInst)
        
        g_logger.log("Init gtms...")
        for gtmInst in self.__dbNodeInfo.gtms:
            self.__initInstance(gtmInst)
        
        g_logger.log("Init coordinators...")
        for cooInst in self.__dbNodeInfo.coordinators:
            self.__initInstance(cooInst)
        
        g_logger.log("Init datanodes...")    
        for dnInst in self.__dbNodeInfo.datanodes:
            self.__initInstance(dnInst)

        g_logger.log("save initdb param...") 
        self.__saveInitdbParam()

            
    def __saveInitdbParam(self):
        """
        save initdb params to the file in bin path
        1.form the initdb param string
        2.write the string into the file
        """
        initdbParamString = "##".join(self.__dbInitParams)
        initdbParamFile = "%s/bin/initdb_param" % self.__clusterInfo.appPath

        #clean initdb param file if exist
        if(os.path.exists(initdbParamFile)):
            cmd = "rm -rf %s" % initdbParamFile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("clean initdb param file failed: %s" % output)  

        #write initdb param string to the file
        try:
            fp = open(initdbParamFile, "w")
            fp.write(initdbParamString)
            fp.close()
            cmd = "chmod 640 %s" % initdbParamFile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("change mode of %s failed: %s" % (initdbParamFile, output))
        except Exception, e:
            if(fp):fp.close()
            g_logger.logExit("create initdb param file failed: %s" % output)
    
    def __initInstance(self, dbInst):
        """
        Init a single instance
        """
        if (dbInst.datadir == ""):
            g_logger.logExit("Data directory of instance is invalid!")
        
        if(not os.path.exists(dbInst.datadir)):
            g_logger.logExit("Data directory[%s] does not exist!" % dbInst.datadir)
        
        cmd = " "
        if (dbInst.instanceRole == INSTANCE_ROLE_GTM):
            cmd += "%s/bin/gs_initgtm -D %s -Z gtm %s" % (self.__clusterInfo.appPath, dbInst.datadir, " ".join(self.__gtmInitParams))
        elif (dbInst.instanceRole == INSTANCE_ROLE_CMSERVER):
            cmd += "%s/bin/gs_initcm -Z cm_server -D %s" % (self.__clusterInfo.appPath, dbInst.datadir)
        elif (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            cmd += "%s/bin/gs_initdb --locale=C -D %s --nodename=cn_%d %s -C %s/bin" % (
                    self.__clusterInfo.appPath, dbInst.datadir, dbInst.instanceId, " ".join(self.__dbInitParams), self.__clusterInfo.appPath)
        elif (dbInst.instanceRole == INSTANCE_ROLE_DATANODE):
            peerInsts = self.__clusterInfo.getPeerInstance(dbInst)
            if (len(peerInsts) != 2 and len(peerInsts) != 1):
                g_logger.logExit("Get peer instance failed!")
            masterInst = None
            standbyInst = None
            dummyStandbyInst = None
            nodename = ""
            for i in range(len(peerInsts)):
                if(peerInsts[i].instanceType == MASTER_INSTANCE):
                    masterInst = peerInsts[i]
                elif(peerInsts[i].instanceType == STANDBY_INSTANCE):
                    standbyInst = peerInsts[i]
                elif(peerInsts[i].instanceType == DUMMY_STANDBY_INSTANCE):
                    dummyStandbyInst = peerInsts[i]
                    
            if(dbInst.instanceType == MASTER_INSTANCE):
                masterInst = dbInst
                g_logger.debug("masterInst:%d standbyInst:%d " % (masterInst.instanceId, standbyInst.instanceId))
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(dbInst.instanceType == STANDBY_INSTANCE):
                standbyInst = dbInst
                g_logger.debug("masterInst:%d standbyInst:%d " % (masterInst.instanceId, standbyInst.instanceId))
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(dbInst.instanceType == DUMMY_STANDBY_INSTANCE):
                dummyStandbyInst = dbInst
                g_logger.debug("masterInst:%d dummyStandbyInst:%d" % (masterInst.instanceId, dummyStandbyInst.instanceId))
                nodename = "dn_%d_%d" % (masterInst.instanceId, dummyStandbyInst.instanceId)

            cmd += "%s/bin/gs_initdb --locale=C -D %s --nodename=%s %s -C %s/bin" % (
                    self.__clusterInfo.appPath, dbInst.datadir, nodename, " ".join(self.__dbInitParams), self.__clusterInfo.appPath)
        elif (dbInst.instanceRole == INSTANCE_ROLE_CMAGENT):
            cmd += "%s/bin/gs_initcm -Z cm_agent -D %s" % (self.__clusterInfo.appPath, dbInst.datadir)
        
        g_logger.debug("Init instance cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Init instance failed!Error:\n%s" % output)
    

def usage():
    """
Usage:
    python InitInstance.py -U user [-P "-PARAMETER VALUE" [...]] [-G "-PARAMETER VALUE" [...]] [-v logfile]
    """
    
    print usage.__doc__
    
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:G:l:?", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    global g_clusterUser
    logFile = ""
    dbInitParams = []
    gtmInitParams = []
    
    for (key, value) in opts:
        if (key == "-?" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-P"):
            dbInitParams.append(value)
        elif (key == "-G"):
            gtmInitParams.append(value)
        elif (key == "-l"):
            logFile = os.path.abspath(value)

    # check if user exist and is the right user
    PlatformCommand.checkUser(g_clusterUser)
        
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")

    # Init logger
    global g_logger
    g_logger = GaussLog(logFile, "InitInstance")
    try:
        dbInit = initDbNode(dbInitParams, gtmInitParams)
        dbInit.run()
        
        g_logger.log("Init instances on node[%s] successfully!" % socket.gethostname())
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.logExit(str(e))

if __name__ == '__main__':
    main()
    