'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import string
import statvfs
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.OMCommand import OMCommand 
from script.util.DbClusterInfo import *
from script.util.Common import *
########################################################################
# Global variables define
########################################################################
GTM_CONF = "gtm.conf"
GTM_PROXY_CONF = "gtm_proxy.conf"
POSTGRESQL_CONF = "postgresql.conf"
CMSERVER_CONF = "cm_server.conf"
CMAGENT_CONF = "cm_agent.conf"
PG_HBA_CONF = "pg_hba.conf"

INIT_DB = "gs_initdb"
INIT_GTM = "gs_initgtm"
INIT_CM = "gs_initcm"

INSTANCE_SPACE_USED = 200
TIME_OUT = 2

g_logger = None
g_clusterUser = ""
g_mpprcFile = ""
g_initdbParam = ""

def Deduplication(list):
    for i in range(len(list) - 2, -1, -1):
        if list.count(list[i]) > 1:
            del list[i]

class ReplaceConfig():
    '''
    classdocs
    ''' 
    def __init__(self, installPath, nodeName, instanceIds):
        '''
        Constructor
        '''
        self.installPath = installPath
        self.nodename = nodeName
        self.instanceIds = instanceIds
                
        self.__user = ""
        self.__group = ""   
        self.__clusterInfo = None
        self.__dbNodeInfo = None
        self.__replaceInstances = []
        self.__instanceIds = []
        
        self.__pgsqlFiles = []
        self.__diskSizeInfo = {}
        
               
    ####################################################################################
    # This is the main install flow.  
    ####################################################################################
    def run(self):
        '''
        check install 
        '''
        try:
            self.readConfigInfo()
            self.getUserInfo()
            self.__createStaticConfig()
            self.initInstances()
            self.__setManualStart()
            self.__setCron()
            self.rebuildInstances()
        except Exception, e:
            g_logger.logExit("Replace config Failed: %s" % str(e)) 
    
    def readConfigInfo(self):
        """
        Read config from static config file
        """
        g_logger.log("readConfigInfo for replace config...")
        
        try:
            self.__clusterInfo = dbClusterInfo()
            self.__clusterInfo.initFromStaticConfig(g_clusterUser)
            
            self.__dbNodeInfo = self.__clusterInfo.getDbNodeByName(self.nodename)
            if (self.__dbNodeInfo is None):
                g_logger.logExit("Get local instance info failed!There is no host named %s!" % self.nodename)
            
            for inst in self.__dbNodeInfo.cmagents:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst)
                    self.__instanceIds.append(inst.instanceId) 
                    self.instanceIds.remove(inst.instanceId)
            for inst in self.__dbNodeInfo.cmservers:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst)
                    self.__instanceIds.append(inst.instanceId) 
                    self.instanceIds.remove(inst.instanceId)
            for inst in self.__dbNodeInfo.gtms:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst) 
                    self.__instanceIds.append(inst.instanceId) 
                    self.instanceIds.remove(inst.instanceId)     
            for inst in self.__dbNodeInfo.gtmProxys:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst)
                    self.__instanceIds.append(inst.instanceId) 
                    self.instanceIds.remove(inst.instanceId)
            for inst in self.__dbNodeInfo.coordinators:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst)
                    self.__instanceIds.append(inst.instanceId)
                    self.instanceIds.remove(inst.instanceId)
            for inst in self.__dbNodeInfo.datanodes:
                if(inst.instanceId in self.instanceIds):
                    self.__replaceInstances.append(inst)
                    self.__instanceIds.append(inst.instanceId) 
                    self.instanceIds.remove(inst.instanceId)
                    
            if (self.instanceIds != []):
                g_logger.logExit("The instanceIds(%s) not found in %s!" % (str(self.instanceIds), self.nodename))                              
        except Exception, e:
            g_logger.logExit(str(e))
        
        g_logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))
    
    def getUserInfo(self):
        """
        Get user and group
        """
        g_logger.log("Getting user and group for application...")
        cmd = "stat -c '%%U:%%G' %s" % self.installPath
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Get user info failed!Error :%s" % output)
        
        userInfo = output.split(":")
        if (len(userInfo) != 2):
            g_logger.logExit("Get user info failed!Error :%s" % output)
        
        self.__user = userInfo[0]
        self.__group = userInfo[1]  
                
    def checkXMLConfig(self): 
        """
        check the port and datadir
        """
        self.__checkPgsqlDir()
        self.__checkNodeConfig()
    
    def __checkPgsqlDir(self):
        """
        Check pgsql directory
        """
        tmpDir = DefaultValue.getTmpDirFromEnv()
        g_logger.log("Checking directory[%s]..." % tmpDir)
        if(os.path.exists(tmpDir)):
            self.__pgsqlFiles = os.listdir(tmpDir)
        else:
            os.makedirs(tmpDir, DefaultValue.DIRECTORY_MODE)
                
        cmd = "chown %s:%s %s" % (self.__user, self.__group, tmpDir)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (tmpDir, output))   
            
    def __checkNodeConfig(self):
        """
        Check instances conifg on local node
        """
        for inst in self.__replaceInstances:
            if (inst.instanceRole != INSTANCE_ROLE_CMAGENT):
                self.__checkPort(inst.port)
                self.__checkPort(inst.haPort)
            self.__checkDataDir(inst.datadir)
        
        dirList = []
        dirList.append(self.__dbNodeInfo.cmDataDir)
        instances = self.__dbNodeInfo.gtms + self.__dbNodeInfo.coordinators + self.__dbNodeInfo.datanodes
        for dbInst in instances:
            dirList.append(dbInst.datadir)
        
        for dirPath in dirList:
            if (not os.path.exists(dirPath)):
                continue
            cmd = "chown -R %s:%s %s" % (self.__user, self.__group, dirPath)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (dirPath, output))    
            
    def __checkDataDir(self, dirPath):
        """
        Check if directory exists and disk size lefted
        """
        g_logger.log("Checking directory[%s]..." % dirPath)
        ownerPath = dirPath
        if(os.path.exists(dirPath)):
            fileList = os.listdir(dirPath)
            if(len(fileList) != 0):
                g_logger.logExit("Data directory[%s] of instance should be empty." % dirPath)
        else:
            while True:
                (ownerPath, dirName) = os.path.split(ownerPath)
                if (os.path.exists(ownerPath) or dirName == ""):
                    ownerPath = os.path.join(ownerPath, dirName)
                    os.makedirs(dirPath, 0750)
                    break
                
        cmd = "chown -R %s:%s %s" % (self.__user, self.__group, ownerPath)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (dirPath, output))
        
        # The file system of directory
        dfCmd = "df -h '%s' | head -2 |tail -1 | awk -F\" \" '{print $1}'" % dirPath
        status, output = commands.getstatusoutput(dfCmd)
        if (status != 0):
            g_logger.logExit("Get the file system of directory failed!Error: %s" % output)
        
        fileSysName = str(output)
        diskSize = self.__diskSizeInfo.get(fileSysName)
        if (diskSize is None):
            vfs = os.statvfs(dirPath)
            diskSize = vfs[statvfs.F_BAVAIL] * vfs[statvfs.F_BSIZE] / (1024 * 1024)
            self.__diskSizeInfo[fileSysName] = diskSize
        
        # 200M for a instance
        if (diskSize < INSTANCE_SPACE_USED):
            g_logger.logExit("The available size of file system[%s] is not enough for the instances on it. Each instance needs 200M!" % fileSysName)
        
        self.__diskSizeInfo[fileSysName] -= INSTANCE_SPACE_USED 
        
    def __checkPort(self, port):
        """
        Check if port is 
        """
        if(port < 0 or port > 65535):
            g_logger.logExit("illegal number of port[%d]." % port)
        if(port >= 0 and port <= 1023):
            g_logger.logExit("system reserved port[%d]." % port)
            
        pgsql = ".s.PGSQL.%d" % port
        pgsql_lock = ".s.PGSQL.%d.lock" % port
        if (pgsql in self.__pgsqlFiles):
            g_logger.logExit("socket file already exists for port: %d" % port)
        
        if (pgsql_lock in self.__pgsqlFiles):
            g_logger.logExit("socket lock file already exists for port: %d" % port)
            
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(TIME_OUT)
        try:
            sk.bind(('127.0.0.1', port))
            sk.close()
        except socket.error, e:
            try:
                if int(e[0]) == 98 or int(e[0]) == 13:
                    g_logger.logExit("port[%d] has been used!" % port) 
            except ValueError, ex:
                g_logger.logExit("check port failed: %s" % str(ex))
  
    def initInstances(self):
        """
        """
        self.__getInitdbParam()
        for inst in self.__replaceInstances:
            self.__initSingleInstance(inst)
            self.__configSingleInstance(inst)


    def __getInitdbParam(self):
        """
        """
        global g_initdbParam
        initdbParamFile = "%s/bin/initdb_param" % self.__clusterInfo.appPath
        
        if(not os.path.exists(initdbParamFile)):
            g_logger.logExit("%s does not exists." % initdbParamFile)
        if(not os.path.isfile(initdbParamFile)):
            g_logger.logExit("%s is not a file." % initdbParamFile)
            
        try:
            fp = open(initdbParamFile, "r")
            initdbParamStr = (fp.read()).strip()
            g_initdbParam = initdbParamStr.replace("##", " ")
            fp.close()
        except Exception, e:
            if(fp):fp.close()
            g_logger.logExit("get initdb param failed: %s" % str(e))

    def __initSingleInstance(self, dbInst):
        """
        Init a single instance
        """
        if (dbInst.datadir == ""):
            g_logger.logExit("Data directory of instance is empty!")
        
        if(not os.path.exists(dbInst.datadir)):
            g_logger.logExit("Data directory[%s] does not exist!" % dbInst.datadir)
        
        cmd = "" 
        if (dbInst.instanceRole == INSTANCE_ROLE_GTM):
            cmd += "%s/bin/%s -D %s -Z gtm" % (self.__clusterInfo.appPath, INIT_GTM, dbInst.datadir)    
        elif (dbInst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            cmd += "%s/bin/%s -D %s -Z gtm_proxy" % (self.__clusterInfo.appPath, INIT_GTM, dbInst.datadir)
        elif (dbInst.instanceRole == INSTANCE_ROLE_CMSERVER):
            cmd += "%s/bin/%s -D %s -Z cm_server" % (self.__clusterInfo.appPath, INIT_CM, dbInst.datadir)
        elif (dbInst.instanceRole == INSTANCE_ROLE_CMAGENT):
            cmd += "%s/bin/%s -D %s -Z cm_agent" % (self.__clusterInfo.appPath, INIT_CM, dbInst.datadir)
        elif (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            cmd += "%s/bin/%s --locale=C -D %s --nodename=cn_%d -C %s/bin" % (
                    self.__clusterInfo.appPath, INIT_DB, dbInst.datadir, dbInst.instanceId, self.__clusterInfo.appPath)
            if(g_initdbParam != ""):
                cmd += " %s" % g_initdbParam
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
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(dbInst.instanceType == STANDBY_INSTANCE):
                standbyInst = dbInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(dbInst.instanceType == DUMMY_STANDBY_INSTANCE):
                dummyStandbyInst = dbInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, dummyStandbyInst.instanceId)

            cmd += "%s/bin/%s --locale=C -D %s --nodename=%s -C %s/bin" % (
                    self.__clusterInfo.appPath, INIT_DB, dbInst.datadir, nodename, self.__clusterInfo.appPath)
            if(g_initdbParam != ""):
                cmd += " %s" % g_initdbParam
        else:
            g_logger.logExit("Invalid Instance Role(%d)!" % dbInst.instanceRole)
       
        g_logger.debug("Init instance cmd: %s" % cmd)  
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Init instance failed!Error:\n%s" % output)
        
        g_logger.debug("Init instance output:\n %s" % output)
  
    def __configSingleInstance(self, inst):
        """
        config instance by peer instance or the same type instance and set our private config items.
        """
        if (inst.instanceRole == INSTANCE_ROLE_CMSERVER):
            self.__configInstanceFromPeerInstance(inst, CMSERVER_CONF)
        elif (inst.instanceRole == INSTANCE_ROLE_CMAGENT):
            self.__configInstanceFromSimilarInstance(inst, CMAGENT_CONF)           
        elif (inst.instanceRole == INSTANCE_ROLE_GTM):
            self.__configInstanceFromPeerInstance(inst, GTM_CONF)          
        elif (inst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            self.__configInstanceFromSimilarInstance(inst, GTM_PROXY_CONF)            
        elif (inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            self.__configInstanceFromSimilarInstance(inst, POSTGRESQL_CONF) 
        elif (inst.instanceRole == INSTANCE_ROLE_DATANODE):
            self.__configInstanceFromPeerInstance(inst, POSTGRESQL_CONF)


    def __configCSOFilesFromOtherCnInstance(self, dbPeerInstance, dbLocalInstance):
        """
        copy C function so files from another CN instance, and rename them
        build cmd
        exec cmd
        """
        soFilePath = "%s/lib/postgresql/pg_plugin" % self.__clusterInfo.appPath
        preStr = "cn_%s#" % dbPeerInstance.instanceId
        g_logger.log("copy C function so files for instance(%s)" % str(dbLocalInstance))
        cmd = "ssh %s 'for file in `ls %s| grep %s`;do scp %s/$file %s:%s/${file#%s};done'" % (dbPeerInstance.listenIps[0], 
                    soFilePath, preStr, soFilePath, dbLocalInstance.listenIps[0], soFilePath, preStr)
        g_logger.debug("copy C function so files cmd:%s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("copy C function so files failed:%s" % output)
        
    def __configCSOFilesFromPeerDnInstance(self, dbPeerInstance, dbLocalInstance):
        """
        copy C function so files from peer dn instance, and rename them
        build cmd
        exec cmd
        """
        soFilePath = "%s/lib/postgresql/pg_plugin" % self.__clusterInfo.appPath
        if(dbLocalInstance.instanceType == MASTER_INSTANCE):
            nodename = "dn_%d_%d" % (dbLocalInstance.instanceId, dbPeerInstance.instanceId)
        elif(dbLocalInstance.instanceType == STANDBY_INSTANCE):
            nodename = "dn_%d_%d" % (dbPeerInstance.instanceId, dbLocalInstance.instanceId)
        elif(dbLocalInstance.instanceType == DUMMY_STANDBY_INSTANCE):
            g_logger.debug("no need copy C function so files for dummy standby instance[%s], just return." % dbLocalInstance.datadir)
            return
        else:
            raise Exception("dn instance[%s] type should be master or standby." % str(dbLocalInstance))
                
        preStr = "^%s#" % nodename
        g_logger.log("copy C function so files for instance(%s)" % str(dbLocalInstance))
        cmd = "ssh %s 'for file in `ls %s| grep %s`;do scp %s/$file %s:%s/$file;done'" % (dbPeerInstance.listenIps[0], 
                    soFilePath, preStr, soFilePath, dbLocalInstance.listenIps[0], soFilePath)
        g_logger.debug("copy C function so files cmd:%s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("copy C function so files failed:%s" % output)


    def __configInstanceFromPeerInstance(self, inst, configFile):
        """
        for cmserver, gtm and datanode, we will copy the peer instance's config file and set our private config items.
        """
        g_logger.log("copy peer instance config file for instace(%s)" % str(inst))
        # TODO current suport 1 primary 1 standby only
        peerInst = self.__clusterInfo.getPeerInstance(inst)[0]
        
        try:
            #copy c function so files from peer dn instance
            if (inst.instanceRole == INSTANCE_ROLE_DATANODE):
                self.__configCSOFilesFromPeerDnInstance(peerInst, inst)
                
            self.__copyFileFromOtherInstance(peerInst.listenIps[0], configFile, peerInst.datadir, inst.datadir)
            
            if (inst.instanceRole == INSTANCE_ROLE_DATANODE):
                self.__copyFileFromOtherInstance(peerInst.listenIps[0], PG_HBA_CONF, peerInst.datadir, inst.datadir)
        except Exception, e:
            g_logger.debug(str(e))
            g_logger.logExit("copy peer instance config file for instace [%s] failed:%s" % (inst.datadir, str(e)))
            
        self.__setPrivateConfigItems(inst, peerInst, configFile)
    
    def __configInstanceFromSimilarInstance(self, inst, configFile):
        """
        for gtm_proxy and coordinator, we will copy the similar instance's config file and set our private config items.
        """
        g_logger.log("copy similar instance config file for instance(%s)" % inst.datadir)
        similarInst = None
        find = False
        
        if (inst.instanceRole == INSTANCE_ROLE_CMAGENT):
            for otherNode in self.__clusterInfo.dbNodes:
                if (otherNode.cmagents != []):
                    for i in range(0, len(otherNode.cmagents)):
                        similarInst = otherNode.cmagents[i]
                        if (similarInst.instanceId not in self.__instanceIds):
                            find = True
                            break
                if (find == True):
                    break 
                
        if (inst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            for otherNode in self.__clusterInfo.dbNodes:
                if (otherNode.gtmProxys != []):
                    for i in range(0, len(otherNode.gtmProxys)):
                        similarInst = otherNode.gtmProxys[i]
                        if (similarInst.instanceId not in self.__instanceIds):
                            find = True
                            break
                if (find == True):
                    break
                                       
        if (inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            for otherNode in self.__clusterInfo.dbNodes:
                if (otherNode.coordinators != []):
                    for i in range(0, len(otherNode.coordinators)):
                        similarInst = otherNode.coordinators[i]
                        if (similarInst.instanceId not in self.__instanceIds):
                            find = True
                            break
                if (find == True):
                    break
        
        if (similarInst == None):
            g_logger.log("Can't find another instance as model for instance(%s)" % str(inst))
        else:
            try:
                #copy c function so files from other cn instance
                if (inst.instanceRole == INSTANCE_ROLE_COODINATOR):
                    self.__configCSOFilesFromOtherCnInstance(similarInst, inst)
                    
                self.__copyFileFromOtherInstance(similarInst.listenIps[0], configFile, similarInst.datadir, inst.datadir)

                if(inst.instanceRole == INSTANCE_ROLE_COODINATOR):
                    self.__copyFileFromOtherInstance(similarInst.listenIps[0], PG_HBA_CONF, similarInst.datadir, inst.datadir)
            except Exception, e:
                g_logger.debug(str(e))
                g_logger.logExit("copy similar instance config file for instance [%s] failed: %s" % (inst.datadir, str(e)))
        
        self.__setPrivateConfigItems(inst, similarInst, configFile)
    
    def __copyFileFromOtherInstance(self, remoteIp, fileName, remoteDir, localDir):
        """
        copy file(filname,not dir) from remoteIp:remoteDir to localDir
        """
        cmd = "scp %s:%s %s" % (remoteIp, os.path.join(remoteDir, fileName), localDir)
        g_logger.debug("scp command is %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("Copy File(%s from %s:%s to %s) failed!Error:\n%s" % (fileName, remoteIp, remoteDir, localDir, output))
            
        cmd = "chown %s:%s %s" % (self.__user, self.__group, os.path.join(localDir, fileName))
        g_logger.debug("chown command is %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("Change ownership(%s to %s:%s) failed!Error:\n%s" % (fileName, self.__user, self.__group, output))
    
    def __setPrivateConfigItems(self, localInst, remoteInst, configFile):
        """
        set local config information according to XML file.
        """
        configFile = os.path.join(localInst.datadir, configFile)
        g_logger.debug("Set private config file: %s" % configFile)
        
        if (localInst.instanceRole == INSTANCE_ROLE_CMSERVER): 
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "log_dir", "%s/cm/cm_server" % DefaultValue.getUserLogDirWithUser(self.__user))         
            pass
            
        if (localInst.instanceRole == INSTANCE_ROLE_CMAGENT):
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "log_dir", "%s/cm/cm_agent" % DefaultValue.getUserLogDirWithUser(self.__user))          
            pass
        
        if (localInst.instanceRole == INSTANCE_ROLE_GTM):
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "listen_addresses", "'localhost,%s'" % ",".join(localInst.listenIps))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "port", str(localInst.port))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "log_directory", "'%s/pg_log/gtm'" % DefaultValue.getUserLogDirWithUser(self.__user))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "local_host", "'%s'" % ",".join(localInst.haIps))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "local_port", localInst.haPort)
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "active_host", "'%s'" % remoteInst.haIps[0])
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "active_port", str(remoteInst.haPort))
            
        if (localInst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            pass
            
        if (localInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "listen_addresses", "'localhost,%s'" % ",".join(localInst.listenIps))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "port", str(localInst.port))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "pgxc_node_name", "'cn_%d'" % localInst.instanceId)
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "pooler_port", str(localInst.haPort))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "log_directory", "'%s/pg_log/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), localInst.instanceId))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "audit_directory", "'%s/pg_audit/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), localInst.instanceId))
            
        if (localInst.instanceRole == INSTANCE_ROLE_DATANODE):
            peerInsts = self.__clusterInfo.getPeerInstance(localInst)
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
                    
            if(localInst.instanceType == MASTER_INSTANCE):
                masterInst = localInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(localInst.instanceType == STANDBY_INSTANCE):
                standbyInst = localInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(localInst.instanceType == DUMMY_STANDBY_INSTANCE):
                dummyStandbyInst = localInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, dummyStandbyInst.instanceId)

            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "listen_addresses", "'%s'" % ",".join(localInst.listenIps))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "port", str(localInst.port))
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "pgxc_node_name", "'%s'" % nodename)
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "log_directory", "'%s/pg_log/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), localInst.instanceId))  
            self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "audit_directory", "'%s/pg_audit/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.__user), localInst.instanceId))

            if(localInst.instanceType == MASTER_INSTANCE):
                self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, standbyInst.haIps[0], standbyInst.haPort))
                if(dummyStandbyInst != None):
                    self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                            (localInst.haIps[0], localInst.haPort, dummyStandbyInst.haIps[0], dummyStandbyInst.haPort))
            elif(localInst.instanceType == STANDBY_INSTANCE):
                self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, masterInst.haIps[0], masterInst.haPort))
                if(dummyStandbyInst != None):
                    self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                            (localInst.haIps[0], localInst.haPort, dummyStandbyInst.haIps[0], dummyStandbyInst.haPort))
            elif(localInst.instanceType == DUMMY_STANDBY_INSTANCE):
                self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, masterInst.haIps[0], masterInst.haPort))
                self.__modifyConfigItem(localInst.instanceRole, localInst.datadir, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, standbyInst.haIps[0], standbyInst.haPort))

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
            cmd = 'echo "       " >> %s' % (configFile)
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
            cmd = "gs_guc set -Z %s -N %s -D %s -c \"%s=%s\"" % (inst_type, self.nodename, datadir, key, value)
            g_logger.debug("set parameter command:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("set parameter failed!Error:%s" % output)
            
    
    def rebuildInstances(self):
        """
        rebuild coordinator
        """
        if (len(self.__dbNodeInfo.coordinators) == 0):
            g_logger.log("There is no coordinator on local node!Skip rebuild instance.")
            return
        cooInst = self.__dbNodeInfo.coordinators[0]
        needRebuild = False
        for inst in self.__replaceInstances:
            if (cooInst.instanceId == inst.instanceId):
                needRebuild = True
        if (not needRebuild):
            g_logger.log("The coordinator don't need rebuild!Skip rebuild instance.")
            return
        
        nodeName = ""
        currCnNode = ""
        
        for dbNode in self.__clusterInfo.dbNodes:
            if (len(dbNode.coordinators) != 0 and dbNode.name != self.__dbNodeInfo.name):
                nodeName = dbNode.name
                break
        
        if (nodeName == ""):
            g_logger.logExit("There is no other node with coordinator!")

        currCnNode = nodeName
        tmpDir = DefaultValue.getTmpDirFromEnv()
        if(not os.path.exists(tmpDir)):
            g_logger.logExit("The directory(%s) does not exist, please check it!" % tmpDir)
        sqlFile = "%s/%s" % (tmpDir, DefaultValue.SCHEMA_COORDINATOR)
        
        try:
            g_logger.log("Begin to rebuild instance...")
            cooInfo = "cn_%d:%s:%d" % (cooInst.instanceId, ",".join(cooInst.listenIps), cooInst.port)
            if(g_mpprcFile != ""):
                cmd = "ssh %s -o BatchMode=yes 'source %s;python %s --lock-cluster --dump-cn --drop-node=cn_%d --create-cn=%s -U %s'" % (nodeName, g_mpprcFile, OMCommand.Local_Query, cooInst.instanceId, cooInfo, self.__user)
            else:
                cmd = "ssh %s -o BatchMode=yes 'python %s --lock-cluster --dump-cn --drop-node=cn_%d --create-cn=%s -U %s'" % (nodeName, OMCommand.Local_Query, cooInst.instanceId, cooInfo, self.__user)
            g_logger.debug("Dump schema cmd: %s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Dump coordinator schema failed!Output: %s" % output)
            
            scpCmd = "scp %s:%s %s" % (nodeName, sqlFile, sqlFile)
            g_logger.debug("Copy schema cmd: %s" % scpCmd)
            (status, output) = commands.getstatusoutput(scpCmd)
            if (status != 0):
                g_logger.logExit("Copy schema from other node failed!Output: %s" % output)
                
            chownCmd = "chown %s %s" % (self.__user, sqlFile)
            g_logger.debug("Chown cmd: %s" % chownCmd)
            (status, output) = commands.getstatusoutput(chownCmd)
            if (status != 0):
                g_logger.logExit("Change owner of sql file failed!Output: %s" % output)
            

            ### only need to build coordinators when we do replace.
            rebulidCmd = "python %s --build-node=coordinators -U %s" % (OMCommand.Local_Query, self.__user)

            g_logger.debug("rebulid cmd: %s" % rebulidCmd)
            (status, output) = commands.getstatusoutput(rebulidCmd)
            if (status != 0):
                g_logger.logExit("rebuild instance failed.!Output: %s" % output)

            cmd = "(if [ -f %s ]; then rm %s; fi)" % (sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            cmd = "ssh %s -o BatchMode=yes '(if [ -f %s ]; then rm %s; fi)'" % (currCnNode, sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            g_logger.debug("Remove coordinator sql file finished.")
        except Exception as e:
            cmd = "(if [ -f %s ]; then rm %s; fi)" % (sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            cmd = "ssh %s -o BatchMode=yes '(if [ -f %s ]; then rm %s; fi)'" % (currCnNode, sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            self.logger.log(str(e))
            raise Exception("Rebuild coordinator failed!")
            
    def __setManualStart(self):
        """
        Set manual start
        """
        g_logger.log("Set manual start...")
        cmd = "touch %s/bin/cluster_manual_start" % self.__clusterInfo.appPath
        
        g_logger.debug("Set manual cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("Set manual start failed!Error:%s" % str(output))
    
    def __setCron(self):
        """
        Set linux cron
        """
        g_logger.log("Set cron...")
        log_path = DefaultValue.getOMLogPath(DefaultValue.OM_MONITOR_DIR_FILE, self.__user, self.__clusterInfo.appPath)
        gaussCronFile = "%s/gauss_cron_%d" %  (DefaultValue.getTmpDirFromEnv(), os.getpid())
        setCronCmd = ""
        
        cmd = "crontab -l"
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            setCronCmd += "crontab -l > %s&& " % gaussCronFile
            setCronCmd += "sed -i '/\\/bin\\/om_monitor/d' %s; " % gaussCronFile
        elif(status != 256):#status==256 means this user has no cron
            g_logger.logExit("check user cron failed:%s" % output)

        setCronCmd += "echo '*/1 * * * * source /etc/profile;"
        if(g_mpprcFile != ""):
            setCronCmd += "source %s;" % g_mpprcFile
        else:
            setCronCmd += "source ~/.bashrc;"
            
        setCronCmd += "nohup %s/bin/om_monitor -L %s >/dev/null 2>&1 &' >> %s&& " % (self.__clusterInfo.appPath, log_path, gaussCronFile)
        setCronCmd += "crontab %s&& " % gaussCronFile
        setCronCmd += "rm -f %s" % gaussCronFile
        
        g_logger.debug("Set cron cmd: %s" % setCronCmd)
        (status, output) = commands.getstatusoutput(setCronCmd)
        if(status != 0):
            g_logger.logExit("Set cron failed!Error:%s" % str(output))
            
    def __createStaticConfig(self):
        """
        Save cluster info to static config
        """
        staticConf = "%s/bin/cluster_static_config" % self.__clusterInfo.appPath
        if (os.path.exists(staticConf)):
            return
        self.__clusterInfo.saveToStaticConfig(staticConf, self.__dbNodeInfo.id)
        
        cmd = "chown %s:%s %s;chmod 640 %s" % (self.__user, self.__group, staticConf, staticConf)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Create cluster static config failed!Error:%s" % output)
       
def usage(self):
    print("ReplaceConfig.py is a utility to stop the local instances.")
    print(" ")
    print("Usage:")
    print("  python ReplaceConfig.py --help")
    print(" ")
    print("Common options:")
    print("  -U        user of cluster")
    print("  -R        Application Dir")
    print("  -n        the nodename")
    print("  -i        the instance ids that we should replace")
    print("  --help    show this help, then exit")
    print(" ")

         
def main():
    """
    main function
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:R:i:n:l:", ["help"])
    except getopt.GetoptError, e:
        GaussLog.exitWithError("Parameter input error: " + e.msg)
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: " + str(args[0]))

    global g_clusterUser
    global g_mpprcFile
    installPath = ""
    nodename = ""
    instanceIds = []
    logFile = ""

    for key, value in opts:
        if(key == "-U"):
            g_clusterUser = value.strip()
        elif(key == "-R"):
            installPath = value.strip()
        elif(key == "-n"):
            nodename = value.strip()
        elif(key == "-i"):
            value = value.strip()
            if (value.isdigit()):
                instanceIds.append(string.atoi(value))
            else:
                GaussLog.exitWithError("Parameter invalid. -i %s is not digit." % value)
        elif (key == "-l"):
            logFile = value
        elif(key == "--help"):
            usage()
            sys.exit(0)
        else:
            GaussLog.exitWithError("Parameter input error: " + value + ".")

    #check mpprc file path
    g_mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
    if(g_mpprcFile == None):
        g_mpprcFile = ""
    if(g_mpprcFile != ""):
        if (not os.path.exists(g_mpprcFile)):
            GaussLog.exitWithError("mpprc file does not exist: %s" % g_mpprcFile)
        if (not os.path.isabs(g_mpprcFile)):
            GaussLog.exitWithError("mpprc file need absolute path:%s" % g_mpprcFile)

    # check if user exist and is the right user
    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)
        
    if(installPath == ""):
        GaussLog.exitWithError("Parameter input error, need '-R' parameter.")
    if(not os.path.exists(installPath)):
        GaussLog.exitWithError("Parameter Invalid. -R %s is not existed." % installPath)
        
    if(logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, installPath)

    hostname = socket.gethostname()
    if (hostname != nodename):
        GaussLog.exitWithError("The hostname %s is not match to the nodename %s." % (hostname, nodename)) 
        
    Deduplication(instanceIds)
    if (instanceIds == []):
        GaussLog.exitWithError("No specific instances to replace. Replace config finished.", 0)  

    global g_logger
    g_logger = GaussLog(logFile, "ReplaceConfig")
    try:
        replacer = ReplaceConfig(installPath, nodename, instanceIds)
        replacer.run()
        
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.logExit("Replace config Failed! Error:%s" % str(e))

if __name__ == '__main__':

    main()
