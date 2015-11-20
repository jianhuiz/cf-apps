'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import commands
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import * 
from script.util.Common import *

########################################################################
# Global variables define
########################################################################
HBA_CONF_FILE = "pg_hba.conf"
METHOD_TRUST = "trust"
logger = None
g_clusterUser = ""

def Deduplication(list):
    list.sort()
    for i in range(len(list) - 2, -1, -1):
        if list.count(list[i]) > 1:
            del list[i]

class ConfigHba():
    '''
    classdocs
    '''
    def __init__(self):        
        self.__dbNodeInfo = None
        self.__allIps = []
               
    def __readConfigFile(self):
        """
        Read config from static config file
        """
        try:
            clusterInfo = dbClusterInfo()
            clusterInfo.initFromStaticConfig(g_clusterUser)
            hostName = socket.gethostname()
            self.__dbNodeInfo = clusterInfo.getDbNodeByName(hostName)
            if (self.__dbNodeInfo is None):
                logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
                
            nodenames = clusterInfo.getClusterNodeNames()
            for nodename in nodenames:
                nodeinfo = clusterInfo.getDbNodeByName(nodename)
                self.__allIps += nodeinfo.backIps
                self.__allIps += nodeinfo.sshIps
                for inst in nodeinfo.cmservers:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.coordinators:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.datanodes:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.gtms:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.gtmProxys:
                    self.__allIps += inst.haIps 
                    self.__allIps += inst.listenIps
                    
            Deduplication(self.__allIps)
            
            # TODO: instead by cluster_static_config
            cooInstList = []
            for dbNode in clusterInfo.dbNodes:
                cooInstList += dbNode.coordinators
            
            connList = []
            for cooInst in self.__dbNodeInfo.coordinators:
                connList.append(["127.0.0.1", cooInst.port])
            
            for cooInst in cooInstList:
                for ip in cooInst.listenIps:
                    connList.append([ip, cooInst.port])
            
            (user, group) = PlatformCommand.getPathOwner(clusterInfo.appPath)

        except Exception, e:
            logger.logExit(str(e))
        
        logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))

    def __configHba(self):
        '''
        set hba config
        '''
        for datanode in self.__dbNodeInfo.datanodes:
            self.__configAnInstance(datanode, "datanode")
            
        for coornode in self.__dbNodeInfo.coordinators:
            self.__configAnInstance(coornode, "coordinator")
    
    def __configAnInstance(self, instance, instanceRole):
        if (instance.datadir == "" or not os.path.exists(instance.datadir)):
            logger.logExit("could not find data dir of the instance %s" % str(instance))
            
        for ip in self.__allIps:
            self.__addHostToFile(instanceRole, self.__dbNodeInfo.name, instance.datadir, ip, METHOD_TRUST) 
    
    def __addHostToFile(self, dbInstanceRole, nodeName, instanceDataPath, ip, type):
        cmd = "gs_guc set -Z %s -N %s -D %s -h \"host    all             all             %s/32              %s\" " % (dbInstanceRole,
                            nodeName, instanceDataPath, ip, type)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            logger.logExit("add host(%s:%s) to instance(%s) failed:%s" % (ip, type, instanceDataPath, output))


    def run(self):
        '''
        config hba file
        '''        
        self.__readConfigFile()
        self.__configHba()
        
def usage():
    """
    python ConfigHba.py -U user
    """
    
    print usage.__doc__

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:l:", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    logFile = ""
    global g_clusterUser
    
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-l"):
            logFile = value

    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)
        
    if(logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")

    # Init logger
    global logger
    logger = GaussLog(logFile, "ConfigHba")
    try:
        configer = ConfigHba()
        configer.run()
        
        logger.log("Config all instances on node[%s] successfully!" % socket.gethostname())
        logger.closeLog()
        sys.exit(0)
    except Exception, e:
        logger.logExit(str(e))
                 
if __name__ == '__main__':
    main()
