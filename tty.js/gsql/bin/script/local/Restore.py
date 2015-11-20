#!/usr/bin/env python


'''
Created on 2014-3-11

@author: 
'''
import commands
import getopt
import os
import sys
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.DbClusterInfo import *
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue, PlatformCommand

GTM_CONF = "gtm.conf"
GTM_PROXY_CONF = "gtm_proxy.conf"
POSTGRESQL_CONF = "postgresql.conf"
POSTGRESQL_HBA_CONF = "pg_hba.conf"
CM_SERVER_CONF = "cm_server.conf"
CM_AGENT_CONF = "cm_agent.conf"

g_logger = None
g_clusterUser = ""

class LocalRestore():
    '''
    classdocs
    '''   
    def __init__(self, user="", restoreDir="", restorePara=False, restoreBin=False):
        '''
        Constructor
        '''
        self.restoreDir = restoreDir
        self.restorePara = restorePara
        self.restoreBin = restoreBin
        
        self.installPath = ""
        self.binExtractName = ""
        self.user = user
        self.group = ""
        self.nodeInfo = None
        
        self.__logFile = ""
        self.__hostNameFile = ""
        
        # #static parameter
        self.defaultLogDir = ""
        self.logName = "gs_local_restore.log"
        self.envirName = "GAUSS_VERSION"
        self.binTarName = "binary.tar"
        self.paraTarName = "parameter.tar"
        self.hostnameFileName = "HOSTNAME"
        
    ####################################################################################
    # This is the main restore flow.
    ####################################################################################
    
    def run(self):
        '''
        check install
        '''
        try:
            self.parseConfigFile()
            self.checkRestoreDir()
            self.doRestore()
        except Exception, e:
            raise Exception(str(e))
        
    def parseConfigFile(self):
        g_logger.log("Parse config file begin...")
        
        try:
            clusterInfo = dbClusterInfo()
            clusterInfo.initFromStaticConfig(self.user)
            
            g_logger.log("Getting local install path for restore...")
            self.installPath = clusterInfo.appPath
            self.binExtractName = self.installPath.split("/")[-1]
            g_logger.debug("Local install path:%s" % self.installPath)
            
            g_logger.log("Getting user and group for restore...")
            cmd = "stat -c '%%U:%%G' %s" % self.installPath
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                raise Exception("Get user info failed: %s" % output)            
            userInfo = output.split(":")
            if (len(userInfo) != 2):
                raise Exception("Get user info failed: %s" % output)            
            self.user = userInfo[0]
            self.group = userInfo[1]
            g_logger.debug("Local user info: %s:%s" % (self.user, self.group)) 
            
            g_logger.log("Getting local node info for restore...")
            hostName = socket.gethostname()
            self.nodeInfo = clusterInfo.getDbNodeByName(hostName)
            if (self.nodeInfo is None):
                raise Exception("Get local node info failed: There is no host named %s!" % hostName)                                   
            g_logger.debug("Local node info:\n%s" % str(self.nodeInfo))            
        except Exception, e:
            g_logger.debug("Parse config file failed!Error:\n%s" % str(e))
            raise Exception("Parse config file failed!Error:\n%s" % str(e))
        
        g_logger.log("Parse config file succeed.")
        
    def checkRestoreDir(self):
        g_logger.log("Check restore dir begin...")

        try:
            if(not os.path.exists(self.restoreDir) or len(os.listdir(self.restoreDir)) == 0):
                raise Exception("Restore directory is not existed or empty.")
        except Exception, e:
            g_logger.debug("Check restore dir failed!Error:\n%s" % str(e))
            raise Exception("Check restore dir failed!Error:\n%s" % str(e))
            
        g_logger.log("Check restore dir succeed.")
        
    def doRestore(self):
        g_logger.log("Restore files begin...")
        
        if(self.restoreBin == True):
            g_logger.log("Restore binary files begin...")
            try:
                g_logger.debug("Check if binary files exist...")
                tarName = os.path.join(self.restoreDir, self.binTarName) 
                if(not os.path.exists(tarName)):
                    raise Exception("Binary files does not exist!") 
                
                g_logger.debug("Create install path if not exists")
                if(not os.path.exists(self.installPath)):
                    os.makedirs(self.installPath, 0750)
                        
                g_logger.debug("restore binary files to install path")        
                cmd = "rm -rf %s/* && tar -xf %s && cp -rf %s/* %s && chown %s:%s -R %s && rm -rf %s" % (self.installPath, tarName, self.binExtractName, 
                                            self.installPath, self.user, self.group, self.installPath, self.binExtractName)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("restore directory(%s) to (%s) error:%s" % (tarName, self.installPath, output))
            except Exception, e:
                g_logger.debug("Restore binary files failed!Error:\n%s" % str(e))
                raise Exception("Restore binary files failed!Error:\n%s" % str(e))
            g_logger.log("Restore binary files succeed.")
        
        if(self.restorePara == True):
            g_logger.log("Restore parametr files begin...")
            
            try:
                g_logger.debug("Delete temp dir if exist...")
                temp_dir = os.path.join(self.restoreDir, "parameter_temp")
                if(os.path.exists(temp_dir)):
                    cmd = "rm -rf %s" % temp_dir
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        raise Exception("delete temp dir(%s) error:%s" % (temp_dir, output))  
                    (status, output) = commands.getstatusoutput("chmod 750 %s" % temp_dir)    
                
                g_logger.debug("extract parameter files to temp dir")
                tarName = os.path.join(self.restoreDir, self.paraTarName)
                if(not os.path.exists(tarName)):
                    raise Exception("Parameter files does not exist!") 
                cmd = "cd %s && tar -xvf %s" % (self.restoreDir, tarName)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("restore directory(%s) to (%s) error:%s" % (tarName, self.restoreDir, output))  
                
                g_logger.debug("Check hostname...")
                self.__checkHostName("%s/%s" % (temp_dir, self.hostnameFileName))
                g_logger.debug("Check parameter files...")
                paraFileList = []
                self.__checkParaFiles(temp_dir, paraFileList)
                
                g_logger.debug("restore parameter files...")
                paraFileNum = len(paraFileList)
                for i in range(paraFileNum):
                    tarFileName, paraFilePath = paraFileList[i].split('|')
                    cmd = "cp -f %s %s && chown %s:%s %s" % (os.path.join(temp_dir, tarFileName), paraFilePath, self.user, self.group, paraFilePath)
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        raise Exception("restore parameter file(%s) to dir(%s) error:%s" % (os.path.join(temp_dir, tarFileName), paraFilePath, output)) 
                
                g_logger.debug("Remove temp dir")
                cmd = "rm -rf " + temp_dir
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("Clean temp dir(%s) error:%s" % (temp_dir, output))
            except Exception, e:
                raise Exception("Restore parametr files failed!Error:\n%s" % str(e))         
            g_logger.log("Restore parametr files succeed.")
        
        g_logger.log("Restore files succeed.")
    
    def __checkHostName(self, hostnameFile):
        localHostName = socket.gethostname()
        self.__hostNameFile = open(hostnameFile)
        storedHostName = self.__hostNameFile.read()
        storedHostName.strip('\n')
        if(cmp(localHostName, storedHostName) != 0):
            raise Exception("Local hostname(%s) does not match with the hostname(%s) stored in tar files!" % (localHostName, storedHostName))
        
    def __checkParaFiles(self, temp_dir, paraFileList):
        storedParaFileNum = len(os.listdir(temp_dir)) - 1
        for inst in self.nodeInfo.cmservers:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)
        for inst in self.nodeInfo.cmagents:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)            
        for inst in self.nodeInfo.coordinators:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)                
        for inst in self.nodeInfo.datanodes:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)                
        for inst in self.nodeInfo.gtms:
            self.__checkSingleParaFile(inst, temp_dir, paraFileList)
        if(cmp(storedParaFileNum, len(paraFileList)) != 0):
            raise Exception("Parameter files does not match!")    

    def __checkSingleParaFile(self, inst, temp_dir, paraFileList):
        if(not os.path.exists(inst.datadir)):
            raise Exception("Data dir(%s) of instance(%s) is not existed." % (inst.datadir, str(inst)))

        paraFileMap = {}
        if(inst.instanceRole == INSTANCE_ROLE_CMSERVER):
            paraFileMap[CM_SERVER_CONF] = os.path.join(inst.datadir, CM_SERVER_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_CMAGENT):
            paraFileMap[CM_AGENT_CONF] = os.path.join(inst.datadir, CM_AGENT_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_GTM):
            paraFileMap[GTM_CONF] = os.path.join(inst.datadir, GTM_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            paraFileMap[POSTGRESQL_CONF] = os.path.join(inst.datadir, POSTGRESQL_CONF)
            paraFileMap[POSTGRESQL_HBA_CONF] = os.path.join(inst.datadir, POSTGRESQL_HBA_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_DATANODE):
            paraFileMap[POSTGRESQL_CONF] = os.path.join(inst.datadir, POSTGRESQL_CONF)
            paraFileMap[POSTGRESQL_HBA_CONF] = os.path.join(inst.datadir, POSTGRESQL_HBA_CONF)
        else:
            raise Exception("Invalid instance type.")
        
        for key in paraFileMap:
            backupFileName = "%d_%s" % (inst.instanceId, key)
            if(not os.path.exists(os.path.join(temp_dir, backupFileName))):
                raise Exception("parameter file(%s) does not found." % backupFileName)
            newRecord = "%s|%s" % (backupFileName, paraFileMap[key])
            paraFileList.append(newRecord)
        
            
####################################################################################
# Help context. U:R:oC:v: 
####################################################################################
def usage():
    print("Restore.py is a local utility to restore binary file and parameter file.")
    print(" ")
    print("Usage:")
    print("python Restore.py --help")
    print(" ")
    print("Common options:")
    print("  -U                              the user of cluster.")
    print("  -P, --position=RESTOREPATH      the restore directory.")
    print("  -p, --parameter                 restore parameter files.")
    print("  -b, --binary_file               restore binary files.")
    print("  -l, --logpath=LOGPATH           the log directory.")
    print("  -h, --help                      show this help, then exit.")
    print(" ")
    
def main():
    """
    main function
    """
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:P:l:pbh", ["position=", "parameter", "binary_file", "logpath=", "help"])
    except getopt.GetoptError, e:
        GaussLog.exitWithError("Parameter input error: " + e.msg)
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: " + str(args[0]))
        
    global g_clusterUser
    restoreDir = ""
    restorePara = False
    restoreBin = False
    logFile = ""

    for key, value in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif(key == "-U"):
            g_clusterUser = value.strip()
        elif(key == "-P" or key == "--position"):
            restoreDir = value.strip()
        elif(key == "-p" or key == "--parameter"):
            restorePara = True
        elif(key == "-b" or key == "--binary_file"):
            restoreBin = True
        elif(key == "-l" or key == "--logpath"):
            logFile = value
        else:
            GaussLog.exitWithError("Parameter input error: " + value + ".")

    # check if user exist and is the right user
    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)

    # check log file
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log path need absolute path.")
    
    if (restorePara == False and restoreBin == False):
        GaussLog.exitWithError("Parameter input error, need '-p' or '-b' parameter.")

    if (restoreDir == ""):
        GaussLog.exitWithError("Parameter input error, need '-P' parameter.")
    
    global g_logger
    g_logger = GaussLog(logFile, "LocalRestore")
    g_logger.log("Local restore begin...")
    try:
        LocalRestorer = LocalRestore(g_clusterUser, restoreDir, restorePara, restoreBin)
        LocalRestorer.run()
        
        g_logger.log("Local restore succeed.")
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        GaussLog.exitWithError("Local restore Failed! %s" % str(e))

if __name__ == '__main__':   
    main()
