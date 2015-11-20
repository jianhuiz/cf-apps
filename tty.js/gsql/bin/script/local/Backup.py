#!/usr/bin/env python


'''
Created on 2014-3-4

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
from script.util.SshTool import SshTool
from script.util.Common import *

GTM_CONF = "gtm.conf"
GTM_PROXY_CONF = "gtm_proxy.conf"
POSTGRESQL_CONF = "postgresql.conf"
POSTGRESQL_HBA_CONF = "pg_hba.conf"
CM_SERVER_CONF = "cm_server.conf"
CM_AGENT_CONF = "cm_agent.conf"


g_clusterUser = ""

class LocalBackup():
    '''
    classdocs
    '''   
    def __init__(self, user = "", backupDir = "", backupPara = False, backupBin = False, logFile = ""):
        '''
        Constructor
        '''
        self.backupDir = backupDir
        self.backupPara = backupPara
        self.backupBin = backupBin
        self.logFile = logFile
        
        self.installPath = ""
        self.user = user
        self.group = ""
        self.nodeInfo = None
        
        self.logger = None
        self.__hostnameFile = None
        
        ##static parameter
        self.defaultLogDir = ""
        self.logName = "gs_local_backup.log"
        self.envirName = "GAUSS_VERSION"
        self.binTarName = "binary.tar"
        self.paraTarName = "parameter.tar"
        self.hostnameFileName = "HOSTNAME"
        
    ####################################################################################
    # This is the main install flow.  
    ####################################################################################
    
    def run(self):
        '''
        check install 
        '''
        self.logger = GaussLog(self.logFile, "LocalBackup") 
        try:
            self.parseConfigFile()
            self.checkBackupDir()
            self.doBackup()
        except Exception,e:
            self.logger.closeLog()
            raise Exception(str(e))     
            
        self.logger.closeLog()
        
    def parseConfigFile(self):
        self.logger.log("Parse config file begin...")
        
        try:            
            clusterInfo = dbClusterInfo()
            clusterInfo.initFromStaticConfig(self.user)
            
            self.logger.log("Getting local install path for backup...")
            self.installPath = clusterInfo.appPath
            if(not os.path.exists(self.installPath)):
                raise Exception("Local install path(%s) doesn't exist." % self.installPath)
            self.logger.debug("Local install path:%s" % self.installPath)
            
            self.logger.log("Getting user and group for backup...")
            cmd = "stat -c '%%U:%%G' %s" % self.installPath
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                raise Exception("Get user info failed: %s" % output)            
            userInfo = output.split(":")
            if (len(userInfo) != 2):
                raise Exception("Get user info failed: %s" % output)            
            self.user = userInfo[0]
            self.group = userInfo[1] 
            self.logger.debug("Local user info: %s:%s" % (self.user,self.group)) 
            
            self.logger.log("Getting local node info for backup...")
            hostName = socket.gethostname()
            self.nodeInfo = clusterInfo.getDbNodeByName(hostName)
            if (self.nodeInfo is None):
                raise Exception("Get local node info failed: There is no host named %s!" % hostName)                                   
            self.logger.debug("Local node info:\n%s" % str(self.nodeInfo))            
        except Exception, e:
            self.logger.debug("Parse config file failed!Error:\n%s" % str(e))
            raise Exception("Parse config file failed!Error:\n%s" % str(e))
        
        self.logger.log("Parse config file succeed.")
        
    def checkBackupDir(self):
        self.logger.log("Check backup dir begin...")

        try:          
            if(not os.path.exists(self.backupDir)):
                os.makedirs(self.backupDir, 0750)
        except Exception,e:
            self.logger.debug("Check backup dir failed!Error:\n%s" % str(e))
            raise Exception("Check backup dir failed!Error:\n%s" % str(e))
          
        self.logger.log("Check backup dir succeed.")
        
    def doBackup(self):
        self.logger.log("Backup files begin...")
        
        if(self.backupBin == True):
            self.logger.log("Backup binary files begin...")
            
            try:
                self.logger.debug("Install path is %s." % self.installPath)
                if(len(os.listdir(self.installPath)) == 0):
                    raise Exception("Install path is empty.")
                self.__tarDir(self.installPath, self.binTarName)                
            except Exception,e:
                self.logger.debug("Backup binary files failed!Error:\n%s" % str(e))
                raise Exception("Backup binary files failed!Error:\n%s" % str(e))
            
            self.logger.log("Backup binary files succeed.")
        
        if(self.backupPara == True):
            self.logger.log("Backup parametr files begin...")
            
            try:
                self.logger.debug("Create temp dir for all parameter files...")
                temp_dir = os.path.join(self.backupDir, "parameter_temp")
                self.logger.debug("Temp dir path:%s." % temp_dir)
                if(os.path.exists(temp_dir)):
                    file_list = os.listdir(temp_dir)
                    if(len(file_list) != 0):
                        self.logger.debug("Temp dir is not empty.\n%s\nRemove all files silently." % file_list)
                        cmd = "rm -rf " + temp_dir + "/*"
                        (status, output) = commands.getstatusoutput(cmd)
                        if(status != 0):
                            raise Exception("Clean temp dir(%s) error:%s" % (temp_dir, output))             
                else:
                    os.makedirs(temp_dir, 0750)
                    
                self.logger.debug("Create hostname file...")
                hostnameFile = os.path.join(temp_dir, self.hostnameFileName)
                self.logger.debug("Register hostname file path:%s." % hostnameFile)
                self.__hostnameFile = open(hostnameFile, "w")   
                hostName = socket.gethostname()
                self.__hostnameFile.write("%s" % hostName)
                self.logger.debug("Flush hostname file...")
                self.__hostnameFile.flush()
                self.__hostnameFile.close()
                self.__hostnameFile = None
                
                self.logger.debug("Collect parameter files...")
                for inst in self.nodeInfo.cmservers:
                    self.__collectParaFilesToTempDir(inst, temp_dir)
                for inst in self.nodeInfo.cmagents:
                    self.__collectParaFilesToTempDir(inst, temp_dir)
                for inst in self.nodeInfo.coordinators:
                    self.__collectParaFilesToTempDir(inst, temp_dir)                
                for inst in self.nodeInfo.datanodes:
                    self.__collectParaFilesToTempDir(inst, temp_dir)                
                for inst in self.nodeInfo.gtms:
                    self.__collectParaFilesToTempDir(inst, temp_dir)
                for inst in self.nodeInfo.gtmProxys:
                    self.__collectParaFilesToTempDir(inst, temp_dir)
                    
                
                self.logger.debug("Make parameter tar file...")
                self.__tarDir(temp_dir, self.paraTarName)   
                
                self.logger.debug("Remove temp dir...")
                cmd = "rm -rf " + temp_dir
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("Clean temp dir(%s) error:%s" % (temp_dir, output)) 
                            
            except Exception,e:          
                self.logger.debug("Backup parametr files failed!Error:\n%s" % str(e))
                raise Exception("Backup parametr files failed!Error:\n%s" % str(e))
            
            self.logger.log("Backup parametr files succeed.")
        
        self.logger.log("Backup files succeed.")
        
    def __collectParaFilesToTempDir(self, inst, temp_dir):
        #todo: add backup pg_hba.conf file in this function
        if(not os.path.exists(inst.datadir) or len(os.listdir(inst.datadir)) == 0):
            raise Exception("Data dir(%s) of instance(%s) is not existed or empty." % (inst.datadir, str(inst)))

        paraFileList = {}
        if(inst.instanceRole == INSTANCE_ROLE_CMSERVER):
            paraFileList[CM_SERVER_CONF] = os.path.join(inst.datadir, CM_SERVER_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_CMAGENT):
            paraFileList[CM_AGENT_CONF] = os.path.join(inst.datadir, CM_AGENT_CONF)           
        elif(inst.instanceRole == INSTANCE_ROLE_GTM):
            paraFileList[GTM_CONF] = os.path.join(inst.datadir, GTM_CONF) 
        elif(inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            paraFileList[POSTGRESQL_CONF] = os.path.join(inst.datadir, POSTGRESQL_CONF)
            paraFileList[POSTGRESQL_HBA_CONF] = os.path.join(inst.datadir, POSTGRESQL_HBA_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_DATANODE):
            paraFileList[POSTGRESQL_CONF] = os.path.join(inst.datadir, POSTGRESQL_CONF)
            paraFileList[POSTGRESQL_HBA_CONF] = os.path.join(inst.datadir, POSTGRESQL_HBA_CONF)
        else:
            raise Exception("Invalid instance type.")

        for key in paraFileList:
            if(not os.path.exists(paraFileList[key])):
                self.logger.debug("The paraPath is: %s" % paraFileList[key])
                raise Exception("Parameter file of instance(%s) is not existed." % (str(inst)))

        for key in paraFileList:
            backupFileName = "%d_%s" % (inst.instanceId, key)
            cmd = "cp -f %s %s" % (paraFileList[key], os.path.join(temp_dir, backupFileName))
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("Copy parameter file(%s) to temp dir(%s) error:%s" % (paraFileList[key], temp_dir, output))
        
        
    def __tarDir(self, targetDir, tarFileName):
        tarName = os.path.join(self.backupDir,tarFileName)
        tarDir = targetDir.split("/")[-1]
        path = os.path.abspath(os.path.join(targetDir,".."))
        cmd = "cd %s && tar -c --exclude=cm_monitor.log --exclude=cm_server.log --exclude=cm_agent.log -f %s %s" % (path, tarName, tarDir)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("Tar directory(%s) to (%s) error:%s" % (targetDir, tarName, output))
            
####################################################################################
# Help context. U:R:oC:v: 
####################################################################################
def usage():
    print("Backup.py is a local utility to backup binary file and parameter file.")
    print(" ")
    print("Usage:")
    print("python Backup.py --help")
    print(" ")
    print("Common options:")
    print("  -U                              the user of cluster.")
    print("  -P, --position=RESTOREPATH      the backup directory.")
    print("  -p, --parameter                 backup parameter files.")
    print("  -b, --binary_file               backup binary files.")
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
        GaussLog.exitWithError("Parameter input error: " + str(args[0]) )
        
    global g_clusterUser
    backupDir = ""
    backupPara = False
    backupBin = False
    logFile = ""

    for key, value in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif(key == "-U"):
            g_clusterUser = value.strip()
        elif(key == "-P" or key == "--position"):
            backupDir = value.strip()
        elif(key == "-p" or key == "--parameter"):
            backupPara = True
        elif(key == "-b" or key == "--binary_file"):
            backupBin = True
        elif(key == "-l" or key == "--logpath"):
            logFile = value.strip()
        else:
            GaussLog.exitWithError("Parameter input error: " + value + ".")

    # check if user exist and is the right user
    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)

    # check log file
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "", "")
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log path need absolute path.")

    if (backupPara == False and backupBin == False):
        GaussLog.exitWithError("Parameter input error, need '-p' or '-b' parameter.")

    if (backupDir == ""):
        GaussLog.exitWithError("Parameter input error, need '-P' parameter.")
    
    try:
        LocalBackuper = LocalBackup(g_clusterUser, backupDir, backupPara, backupBin, logFile)
        LocalBackuper.run()
    except Exception,e:
        GaussLog.exitWithError("Local backup Failed! %s" % str(e))

if __name__ == '__main__':

    main()
    sys.exit(0)
