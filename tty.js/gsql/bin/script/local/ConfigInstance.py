'''
Created on 2014-2-17

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
from script.util.Common import *

#############################################################################
# Global variables
#############################################################################
g_logger = None
g_clusterUser = ""

class ConfigInstance():
    """
    config all instance on local node
    """
    CONFIG_PG_FILE = "pg_config"
    CONFIG_GS_FILE = "gs_config"
    CONFIG_ALL_FILE = "all"
    def __init__(self, cooParams, dataParams, confType="", gsConfPath="", alarm_component=""):
        self.__cooGucParams = cooParams[:]
        self.__dataGucParams = dataParams[:]
        self.__configType = confType
        self.__gsStaticConfig = gsConfPath
        self.__alarm_component = alarm_component
        
        self.__clusterInfo = None
        self.__dbNodeInfo = None
        self.__gtmList = []
        self.__cooConfig = {}
        self.__dataConfig = {}
        self.__user = ""
        self.__group = ""
        
    def run(self):
        """
        Init instance on local node
        """
        self.__checkParameters()
        self.__readConfigInfo()
        self.__getUserInfo()
        if (self.__configType in [ConfigInstance.CONFIG_PG_FILE, ConfigInstance.CONFIG_ALL_FILE]):
            self.__modifyConfig()        
        
    def __checkParameters(self):
        """
        Check parameters for instance config
        """
        g_logger.log("Checking parameters for config coordinator and datanode...")
        for param in self.__cooGucParams:
            if (self.__checkconfigParams(param.strip()) != 0):
                g_logger.logExit("Parameter input error: %s." % param)
                
        for param in self.__dataGucParams:
            if (self.__checkconfigParams(param.strip(), False) != 0):
                g_logger.logExit("Parameter input error: %s." % param)
    
    def __checkconfigParams(self, param, isCoo=True):
        """
        Check parameter for postgresql.conf
            port : this is calculated automatically"
        """
        configInvalidArgs = ["port", "alarm_component"]
        
        keyValue = param.split("=")
        if (len(keyValue) != 2):
            return 1
        
        key = keyValue[0].strip()
        value = keyValue[1].strip()
        if (key in configInvalidArgs):
            return 1
        if (isCoo):
            self.__cooConfig[key] = value
        else:
            self.__dataConfig[key] = value
        
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
        except Exception, e:
            g_logger.logExit(str(e))
        
        g_logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))
        
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
    
    def __modifyConfig(self):
        """
        Modify all instances on loacl node
        """
        self.__gtmList = []
        for dbNode in self.__clusterInfo.dbNodes:
            self.__gtmList += dbNode.gtms
            
        g_logger.log("Modify cm_server config...")
        for cmserverInst in self.__dbNodeInfo.cmservers:
            configFile = os.path.join(cmserverInst.datadir, "cm_server.conf")
            self.__modifyConfigItem(INSTANCE_ROLE_CMSERVER, cmserverInst.datadir, configFile, "log_dir", "%s/cm/cm_server" % DefaultValue.getUserLogDirWithUser(self.__user))
        
        g_logger.log("Modify cm_agent config...")
        for agentInst in self.__dbNodeInfo.cmagents:
            configFile = os.path.join(agentInst.datadir, "cm_agent.conf")
            self.__modifyConfigItem(INSTANCE_ROLE_CMAGENT, agentInst.datadir, configFile, "log_dir", "%s/cm/cm_agent" % DefaultValue.getUserLogDirWithUser(self.__user))
            self.__modifyConfigItem(INSTANCE_ROLE_CMAGENT, agentInst.datadir, configFile, "alarm_component", "%s" % self.__alarm_component)
        
        g_logger.log("Modify gtm config...")
        for gtmInst in self.__dbNodeInfo.gtms:
            instList = self.__clusterInfo.getPeerInstance(gtmInst)
            if (len(instList) != 1):
                g_logger.logExit("Get peer gtm failed!")
            peerGtm = instList[0]
            configFile = os.path.join(gtmInst.datadir, "gtm.conf")
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "listen_addresses", "'localhost,%s'" % ",".join(gtmInst.listenIps))
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "port", gtmInst.port)
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "log_directory", "'%s/pg_log/gtm'" % (DefaultValue.getUserLogDirWithUser(self.__user)))
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "local_host", "'%s'" % ",".join(gtmInst.haIps))
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "local_port", gtmInst.haPort)
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "active_host", "'%s'" % ",".join(peerGtm.haIps))
            self.__modifyConfigItem(INSTANCE_ROLE_GTM, gtmInst.datadir, configFile, "active_port", peerGtm.haPort)
        
        g_logger.log("Modify coordinator config...")
        for cooInst in self.__dbNodeInfo.coordinators:
            configFile = os.path.join(cooInst.datadir, "postgresql.conf")
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "listen_addresses", "'localhost,%s'" % ",".join(cooInst.listenIps))
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "port", cooInst.port)
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "pooler_port", cooInst.haPort)
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "log_directory", "'%s/pg_log/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), cooInst.instanceId))
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "audit_directory", "'%s/pg_audit/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), cooInst.instanceId))
            self.__modifyCommonItems(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile)
            self.__modifyGtmInfo(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile)
            for entry in self.__cooConfig.items():
                self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, entry[0], entry[1])
            self.__modifyConfigItem(INSTANCE_ROLE_COODINATOR, cooInst.datadir, configFile, "alarm_component", "'%s'" % self.__alarm_component)
        
        g_logger.log("Modify datanode config...")
        for dnInst in self.__dbNodeInfo.datanodes:
            configFile = os.path.join(dnInst.datadir, "postgresql.conf")
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, "listen_addresses", "'%s'" % ",".join(dnInst.listenIps))
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, "port", dnInst.port)
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, "log_directory", "'%s/pg_log/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), dnInst.instanceId))
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, "audit_directory", "'%s/pg_audit/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), dnInst.instanceId))
            self.__modifyCommonItems(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile)
            self.__modifyGtmInfo(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile)
            self.__modifyReplConninfo(dnInst, configFile)
            for entry in self.__dataConfig.items():
                self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, entry[0], entry[1])
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dnInst.datadir, configFile, "alarm_component", "'%s'" % self.__alarm_component)
            
    def __modifyCommonItems(self, type, datadir, configFile):
        """
        Set default value for each inst
        """
        self.__modifyConfigItem(type, datadir, configFile, "unix_socket_directory", "'%s'" % DefaultValue.getTmpDirFromEnv())
        self.__modifyConfigItem(type, datadir, configFile, "unix_socket_permissions", "0700")
        self.__modifyConfigItem(type, datadir, configFile, "log_file_mode", "0650")
        self.__modifyConfigItem(type, datadir, configFile, "max_coordinators", "256")
        self.__modifyConfigItem(type, datadir, configFile, "max_datanodes", "1280")
        
    def __modifyReplConninfo(self, dbInst, configFile):
        """
        Modify replconninfo for datanode
        """
        peerInsts = self.__clusterInfo.getPeerInstance(dbInst)
        if (len(peerInsts) != 1 and len(peerInsts) != 2):
            return
        masterInst = None
        standbyInst = None
        dummyStandbyInst = None
        for i in range(len(peerInsts)):
            if(peerInsts[i].instanceType == MASTER_INSTANCE):
                masterInst = peerInsts[i]
            elif(peerInsts[i].instanceType == STANDBY_INSTANCE):
                standbyInst = peerInsts[i]
            elif(peerInsts[i].instanceType == DUMMY_STANDBY_INSTANCE):
                dummyStandbyInst = peerInsts[i]
                
        if(dbInst.instanceType == MASTER_INSTANCE):
            masterInst = dbInst
        elif(dbInst.instanceType == STANDBY_INSTANCE):
            standbyInst = dbInst
        elif(dbInst.instanceType == DUMMY_STANDBY_INSTANCE):
            dummyStandbyInst = dbInst

        if(len(masterInst.haIps) == 0 or len(standbyInst.haIps) == 0):
            g_logger.logExit("HA IP is empty!DataDir:%s" % dbInst.datadir)
        if(dummyStandbyInst != None and len(dummyStandbyInst.haIps) == 0):
            g_logger.logExit("HA IP is empty!DataDir:%s" % dbInst.datadir)
            
        connInfo1 = ""
        connInfo2 = ""
        channelCount = len(masterInst.haIps)
        for i in range(channelCount):
            if(dbInst.instanceType == MASTER_INSTANCE):
                if (i > 0):connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, standbyInst.haIps[i], standbyInst.haPort)
                if(dummyStandbyInst != None):
                    if (i > 0):connInfo2 += ","
                    connInfo2 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, dummyStandbyInst.haIps[i], dummyStandbyInst.haPort)
            elif(dbInst.instanceType == STANDBY_INSTANCE):
                if (i > 0):connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, masterInst.haIps[i], masterInst.haPort)
                if(dummyStandbyInst != None):
                    if (i > 0):connInfo2 += ","
                    connInfo2 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, dummyStandbyInst.haIps[i], dummyStandbyInst.haPort)
            elif(dbInst.instanceType == DUMMY_STANDBY_INSTANCE):
                if (i > 0):connInfo1 += ","
                connInfo1 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, masterInst.haIps[i], masterInst.haPort)
                if (i > 0):connInfo2 += ","
                connInfo2 += "localhost=%s localport=%d remotehost=%s remoteport=%d" % (dbInst.haIps[i], dbInst.haPort, standbyInst.haIps[i], standbyInst.haPort)

        self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dbInst.datadir, configFile, "replconninfo1", "'%s'" % connInfo1)
        if(dummyStandbyInst != None):
            self.__modifyConfigItem(INSTANCE_ROLE_DATANODE, dbInst.datadir, configFile, "replconninfo2", "'%s'" % connInfo2)

    def __modifyGtmInfo(self, type, datadir, configFile):
        """
        Modify gtm info
        """
        masterGTM = None
        standbyGTM = None
        for gtmInst in self.__gtmList:
            if (gtmInst.instanceType == MASTER_INSTANCE):
                masterGTM = gtmInst
            else:
                standbyGTM = gtmInst
                
        ips = ",".join(masterGTM.listenIps)
        self.__modifyConfigItem(type, datadir, configFile, "gtm_host", "'%s'" % ips)
        self.__modifyConfigItem(type, datadir, configFile, "gtm_port", masterGTM.port)
        ips = ",".join(standbyGTM.listenIps)
        self.__modifyConfigItem(type, datadir, configFile, "gtm_host1", "'%s'" % ips)
        self.__modifyConfigItem(type, datadir, configFile, "gtm_port1", standbyGTM.port)

    def __modifyConfigItem(self, type, datadir, configFile, key, value):
        """
        Modify a parameter
        """
        # comment out any existing entries for this setting
        if(type == INSTANCE_ROLE_CMSERVER or type == INSTANCE_ROLE_CMAGENT):
            cmd = "perl -pi.bak -e's/(^\s*%s\s*=.*$)/#$1/g' %s" % (key, configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Comment parameter failed!Error:%s" % output)
                
            # append new config to file
            cmd = 'echo "      " >> %s' % (configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Append null line failed!Error:%s" % output)
                
            cmd = 'echo "%s = %s" >> %s' % (key, value, configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Append new vaule failed!Error:%s" % output)
        else:
            if(type == INSTANCE_ROLE_GTM):
                inst_type = "gtm"
            elif(type == INSTANCE_ROLE_DATANODE):
                inst_type = "datanode"
            elif(type == INSTANCE_ROLE_COODINATOR):
                inst_type = "coordinator"
            elif(type == INSTANCE_ROLE_GTMPROXY):
                inst_type = "gtm_proxy"
            else:
                g_logger.logExit("Invalid instance type:%s" % type)   
    
            cmd = "gs_guc set -Z %s -N %s -D %s -c \"%s=%s\"" % (inst_type, self.__dbNodeInfo.name, datadir, key, value)
            g_logger.debug("set parameter command:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("set parameter failed!Error:%s" % output)
    
    def __createStaticConfig(self):
        """
        Save cluster info to static config
        """
        if (self.__gsStaticConfig == ""):
            self.__gsStaticConfig = "%s/bin/cluster_static_config" % self.__clusterInfo.appPath
        self.__clusterInfo.saveToStaticConfig(self.__gsStaticConfig, self.__dbNodeInfo.id)
        
        cmd = "chown %s:%s %s;chmod 640 %s" % (self.__user, self.__group, self.__gsStaticConfig, self.__gsStaticConfig)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Create cluster static config failed!Error:%s" % output)

def usage():
    """
Usage:
    python -h | -help
    python ConfigInstance.py -U user [-T config_type] [-P gs_config_path] [-C "PARAMETER=VALUE" [...]] [-D "PARAMETER=VALUE" [...]] [-L log]
        target file: pg_config, gs_config, all
    """
    
    print usage.__doc__
    
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:C:D:T:P:l:h", ["help", "alarm="])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

    global g_clusterUser
    logFile = ""
    cooParams = []
    dataParams = []
    confType = ConfigInstance.CONFIG_ALL_FILE
    gsPath = ""
    alarm_component = ""
    
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-C"):
            cooParams.append(value)
        elif (key == "-D"):
            dataParams.append(value)
        elif (key == "-T"):
            confType = value
        elif (key == "-P"):
            gsPath = value
        elif (key == "-l"):
            logFile = os.path.abspath(value)
        elif (key == "--alarm"):
            alarm_component = value

    # check if user exist and is the right user
    if (g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")     
    PlatformCommand.checkUser(g_clusterUser)
        
    if (confType not in [ConfigInstance.CONFIG_ALL_FILE, ConfigInstance.CONFIG_GS_FILE, ConfigInstance.CONFIG_PG_FILE]):
        GaussLog.exitWithError("Unknown config type: %s" % confType)
        
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")

    if (alarm_component == ""):
        alarm_component = DefaultValue.ALARM_COMPONENT_PATH

    # Init logger
    global g_logger
    g_logger = GaussLog(logFile, "ConfigInstance")
    try:
        configer = ConfigInstance(cooParams, dataParams, confType, gsPath, alarm_component)
        configer.run()
        
        g_logger.log("Config all instances on node[%s] successfully!" % socket.gethostname())
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.logExit(str(e))

if __name__ == '__main__':
    main()
