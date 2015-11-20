'''
Created on 2014-6-30

@author: 
'''
import commands
import getopt
import sys
import time
import os

sys.path.append(sys.path[0] + "/../../")
try:
    from script.util.Common import DefaultValue, ClusterCommand
    from script.util.DbClusterInfo import *
    from script.util.GaussLog import GaussLog
    from script.util.OMCommand import LocalCommand, CommandThread
except ImportError, e:
    sys.exit("ERROR: Cannot import modules: %s" % str(e))


POSTGRESQL_CONF = "postgresql.conf"
CMAGENT_CONF = "cm_agent.conf"

def Deduplication(list):
    list.sort()
    for i in range(len(list) - 2, -1, -1):
        if list.count(list[i]) > 1:
            del list[i]

class DilatationConfig(LocalCommand):
    
    CONFIG_CHANGE_FLAG = "cluster_dilatation_status"
    CLUSTER_STATIC_CONFIG_FILE = "cluster_static_config"
    PGHBA_CONFIG_FILE = "pg_hba.conf"
    
    def __init__(self, logFile, user, configFile):
        LocalCommand.__init__(self, logFile, user, configFile)
        self.readConfigInfoByXML()
        self.getUserInfo()
        statusFile = self.getStatusFilePath()
            
    def cleanFile(self, file):
        cmd = "rm -rf %s" % (file)
        self.logger.debug("clean file: %s" % file)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("clean file[%s] failed.Error:%s" % (file, output))
        
    def getStatusFilePath(self):
        return "%s/bin/%s" % (self.clusterInfo.appPath, DilatationConfig.CONFIG_CHANGE_FLAG)
        
    def touchStatusFile(self):
        self.logger.log("Touch cluster dilatation status file...")
        statusFile = self.getStatusFilePath()
        touchCmd = "touch %s" % statusFile
        self.logger.debug("touchCmd: %s" % touchCmd)
        (status, output) = commands.getstatusoutput(touchCmd)
        if(status != 0):
            raise Exception("Touch cluster dilatation status file failed!Error:%s" % str(output))
        self.logger.log("Touch cluster dilatation status file finished.")
        
    def cleanStatusFile(self):
        self.logger.log("Begin clean status file...")
        statusFile = self.getStatusFilePath()
        self.cleanFile(statusFile)
        self.logger.log("Clean status file finished.")
        
    def killagent(self):
        self.logger.log("Killing agent...")
        killCmd = "killall -u %s -9 cm_agent" % (self.user)
        self.logger.debug("killCmd: %s" % killCmd)
        (status, output) = commands.getstatusoutput(killCmd)
        if(status != 0 and output.find("cm_agent: no process found") < 0):
            raise Exception("Kill agent failed!Error:%s" % str(output))
        self.logger.log("Kill agent finished.")

    #### Config Functions
    def config(self):
        try:
            self.touchStatusFile()            
            self.killagent()            
            self.configPgHba()
            self.configStaticConfig()
            self.logger.closeLog()
        except Exception as e:
            self.logger.logExit(str(e))

    def configPgHba(self):     
        newIps = []
        connList = []   
        newIpStr = ""     
        for node in self.clusterInfo.newNodes:           
            newIps += node.backIps
            newIps += node.sshIps
            for inst in node.cmservers:
                newIps += inst.haIps
                newIps += inst.listenIps
            for inst in node.coordinators:
                newIps += inst.haIps
                newIps += inst.listenIps
                for ip in inst.listenIps:
                    connList.append([ip, inst.port])
            for inst in node.datanodes:
                newIps += inst.haIps
                newIps += inst.listenIps
            for inst in node.gtms:
                newIps += inst.haIps
                newIps += inst.listenIps
            for inst in node.gtmProxys:
                newIps += inst.haIps
                newIps += inst.listenIps                
        Deduplication(newIps)
        self.logger.log("new ips need to add: %s" % newIps)
        for dbInstance in self.dbNodeInfo.datanodes:
            hbaFile = "%s/%s" % (dbInstance.datadir, DilatationConfig.PGHBA_CONFIG_FILE)
            for ip in newIps:
                cmd = "gs_guc set -Z datanode -N %s -D %s -h \"host    all             all             %s/32              trust\"" % (self.dbNodeInfo.name, dbInstance.datadir, ip)
                (status, output) = commands.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception("Modify pg_hba.conf[%s] failed.Error:%s" % (hbaFile, output))

        for dbInstance in self.dbNodeInfo.coordinators:
            hbaFile = "%s/%s" % (dbInstance.datadir, DilatationConfig.PGHBA_CONFIG_FILE)
            for ip in newIps:
                cmd = "gs_guc set -Z coordinator -N %s -D %s -h \"host    all             all             %s/32            trust\"" % (self.dbNodeInfo.name, dbInstance.datadir, ip)
                (status, output) = commands.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception("Modify pg_hba.conf[%s] failed.Error:%s" % (hbaFile, output))
        self.logger.log("Modify pg_hba.conf finished.")
        
        
    def configStaticConfig(self):         
        staticConfigFile = "%s/bin/%s" % (self.clusterInfo.appPath, DilatationConfig.CLUSTER_STATIC_CONFIG_FILE)
        self.logger.log("Begin modify cluster static config file...")
        if(os.path.exists(staticConfigFile)):
            try:
                os.remove(staticConfigFile)      
            except Exception as e:
                self.logger.debug("Remove old static config file failed. Error:%s" % str(e)) 
                
        self.clusterInfo.saveToStaticConfig(staticConfigFile, self.dbNodeInfo.id)
        chownCmd = "chown %s:%s %s;chmod 640 %s" % (self.user, self.group, staticConfigFile, staticConfigFile)
        self.logger.debug("chownCmd: %s" % chownCmd)
        (status, output) = commands.getstatusoutput(chownCmd)
        if (status != 0):
            raise Exception("Chown cluster static config failed!Error:%s" % output)
        self.logger.log("Modify cluster static config file finished.")  
        
    #### Clean Functions
    def clean(self):
        try:              
            self.cleanStatusFile()
            self.logger.closeLog()
        except Exception as e:
            self.logger.logExit(str(e))
            
    #### Restore Functions       
    def restore(self):
        try:
            self.touchStatusFile()
            self.killagent()
            self.restoreStaticConfig()             
            self.logger.closeLog()
        except Exception as e:
            self.logger.logExit(str(e)) 
        
    def restoreStaticConfig(self):
        self.logger.log("Begin modify cluster static config file...")
        staticConfigFile = "%s/bin/%s" % (self.clusterInfo.appPath, DilatationConfig.CLUSTER_STATIC_CONFIG_FILE)
        if(os.path.exists(staticConfigFile)):
            try:
                os.remove(staticConfigFile)      
            except Exception as e:
                self.logger.debug("Remove old static config file failed. Error:%s" % str(e)) 
                
        oldNodes = []
        for dbNode in self.clusterInfo.dbNodes:
            if (dbNode not in self.clusterInfo.newNodes):
                oldNodes.append(dbNode)
        
        self.clusterInfo.saveToStaticConfig(staticConfigFile, self.dbNodeInfo.id, oldNodes)
        chownCmd = "chown %s:%s %s;chmod 640 %s" % (self.user, self.group, staticConfigFile, staticConfigFile)
        self.logger.debug("chownCmd: %s" % chownCmd)
        (status, output) = commands.getstatusoutput(chownCmd)
        if (status != 0):
            raise Exception("Chown cluster static config failed!Error:%s" % output)
        self.logger.log("Modify cluster static config file finished.")
        
    #### Check Functions
    def check(self):
        try:
            self.checkStaticConfig()
            self.logger.closeLog()
        except Exception as e:
            self.logger.logExit("Static config not matched. Error: %s" % str(e))
        
    def checkStaticConfig(self):       
        try:
            staticConfigInfo = dbClusterInfo()
            staticConfigInfo.initFromStaticConfig(self.user)
        except Exception as e:
            raise Exception("Read cluster static config file failed. Error: %s\n" % str(e))
        
        if (staticConfigInfo.name != self.clusterInfo.name):            
            raise Exception("Cluster name is not matched. Old name: %s, New name: %s." % (staticConfigInfo.name, self.clusterInfo.name))
        
        if (staticConfigInfo.appPath != self.clusterInfo.appPath):
            raise Exception("Cluster appPath is not matched. Old appPath: %s, New appPath: %s." % (staticConfigInfo.appPath, self.clusterInfo.appPath))
        
        if (staticConfigInfo.cmsFloatIp != self.clusterInfo.cmsFloatIp):
            raise Exception("Cluster cmsFloatIp is not matched. Old cmsFloatIp: %s, New cmsFloatIp: %s." % (staticConfigInfo.cmsFloatIp, self.clusterInfo.cmsFloatIp))                                                                                                                
        
        oldNodes = []
        for dbNode in self.clusterInfo.dbNodes:
            if (dbNode not in self.clusterInfo.newNodes):
                oldNodes.append(dbNode)
        
        theSameOld, buffer = compareObject(staticConfigInfo.dbNodes, oldNodes, "dbNodes")
        if (theSameOld):
            self.logger.log("Static config matched with old config file.")
        else:
            theSameNew = compareObject(staticConfigInfo, self.clusterInfo, "clusterInfo")[0]
            if (theSameNew):
                self.logger.log("Static config matched with new config file.")
            else:
                raise Exception("Cluster static config not matched. Error:\n%s" % buffer.strip("\n"))
            
    def localStart(self):
        nodeId = dbClusterInfo.getNodeIdByName(self.dbNodeInfo.name, self.clusterConfig)
        cmd = ClusterCommand.getStartCmd(self.user, nodeId)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.log(output)
            self.logger.logExit("Start node failed!")
        self.logger.log("Start node success.")
        self.logger.closeLog()
    
    def localStop(self):
        nodeId = dbClusterInfo.getNodeIdByName(self.dbNodeInfo.name, self.clusterConfig)
        cmd = ClusterCommand.getStopCmd(self.user, nodeId, "i")
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.log(output)
            self.logger.logExit("Stop node failed!")
        self.logger.log("Stop node success.")
        self.logger.closeLog()

    #### copy C-function so files to all new nodes
    def copyCSOFiles(self):
        """
        copy C function so files to new nodes
        """
        self.logger.log("begin copy C function so files to new nodes")
        allValidFile = []
        soFileList = {}
        sepStr = "#"
        sufStr = ".so"
        soFilePath = "%s/lib/postgresql/pg_plugin" % self.clusterInfo.appPath
        #get all valid file list
        allFileList = os.listdir(soFilePath)
        if(len(allFileList) == 0):
            self.logger.log("%s is empty, no need to copy c function so files." % soFilePath)
            return
        for file in allFileList:
            if(not os.path.isfile("%s/%s" % (soFilePath, file))):
                continue
            if(file.find(sepStr) == -1):
                continue
            if(file[-3:] != sufStr):
                continue
            allValidFile.append(file)
        self.logger.debug("all valid file list:%s" % allValidFile)
        #get so file list
        for file in allValidFile:
            soFileName = file[file.find(sepStr) + 1:]
            if(soFileName not in soFileList):
                soFileList[soFileName] = file
        self.logger.debug("so file list: %s" % soFileList)
        #copy so files to all new nodes
        for node in self.clusterInfo.newNodes:
            cmd = ""
            for key in soFileList:
                cmd +="scp %s/%s %s:%s/%s&&" % (soFilePath, soFileList[key], node.name, soFilePath, key)
            cmd += "date"#add date to aovid the cmd end with &&
            self.logger.debug("copy so file cmd:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                self.logger.debug("copy so file cmd:%s" % cmd)
                self.logger.logExit("copy so file failed!Error:%s" % output)
                
        self.logger.log("copy C function so files to new nodes succeed.")

    def configNewNode(self):
        """
        only have cm_agent cn and dn instance on new nodes
        for cn and dn instances on new nodes, copy similar config file from 
        old node, and then do some local modification
        1.copy file from similar instance
        2.do some local modification
        """
        for dbInstance in self.dbNodeInfo.cmagents:
            self.configOneInstance(dbInstance, CMAGENT_CONF)
        for dbInstance in self.dbNodeInfo.coordinators:
            self.configOneInstance(dbInstance, POSTGRESQL_CONF)
        for dbInstance in self.dbNodeInfo.datanodes:
            self.configOneInstance(dbInstance, POSTGRESQL_CONF)

    def configOneInstance(self, dbInstance, configFile):
        """
        """
        similarInstance = self.findSimilarInstance(dbInstance)
        self.copyFileFromSimilarInstace(dbInstance, similarInstance, configFile)
        self.doPrivateConfigSet(dbInstance, configFile)
    

    def findSimilarInstance(self, dbInstance):
        """
        """
        oldNodes = []
        similarInstance = None
        #find all old nodes
        #newNodeNames = [dbNode.name for dbNode in self.clusterInfo.newNodes]
        #print(newNodeNames)
        for dbNode in self.clusterInfo.dbNodes:
            if(dbNode not in self.clusterInfo.newNodes):
                oldNodes.append(dbNode)
        if(len(oldNodes) == 0):
            self.logger.logExit("can not find any old node.")

        #find a cm agent instance from old nodes
        if (dbInstance.instanceRole == INSTANCE_ROLE_CMAGENT):
            for dbNode in oldNodes:
                if(len(dbNode.cmagents) != 0):
                    similarInstance = dbNode.cmagents[0]
                    break
        #find a cn instance from old nodes
        elif (dbInstance.instanceRole == INSTANCE_ROLE_COODINATOR):
            for dbNode in oldNodes:
                if(len(dbNode.coordinators) != 0):
                    similarInstance = dbNode.coordinators[0]
                    break
        #find a dn instance with the same type from old nodes
        elif(dbInstance.instanceRole == INSTANCE_ROLE_DATANODE):
            for dbNode in oldNodes:
                if(len(dbNode.datanodes) != 0):
                    for dnInstance in dbNode.datanodes:
                        if(dnInstance.instanceType == dbInstance.instanceType):
                            similarInstance = dnInstance
                            break
                            
        #invalid instance role
        else:
            self.logger.logExit("current instance role is invalid [%s %s]." % (dbInstance.datadir,  dbInstance,instanceRole))

        if(similarInstance == []):
            self.logger.logExit("can not find a similar instance for [%s %s]." % (dbInstance.hostname, dbInstance.datadir))

        return similarInstance
        
        
    def copyFileFromSimilarInstace(self, localInstance, remoteInstance, configFile):
        """
        """
        #copy similar instance config file to local node
        cmd = "scp %s:%s %s" % (remoteInstance.hostname, os.path.join(remoteInstance.datadir, configFile),
                                localInstance.datadir)
        self.logger.debug("scp command is %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Copy File(%s from %s:%s to %s) failed!Error:\n%s" % (configFile, 
                    remoteInstance.hostname, remoteInstance.datadir, localInstance.datadir, output))

        #change the owner of config file
        cmd = "chown %s:%s %s" % (self.user, self.group, os.path.join(localInstance.datadir, configFile))
        self.logger.debug("chown command is %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Change ownership(%s to %s:%s) failed!Error:\n%s" % (configFile, self.user, self.group, output))


    def doPrivateConfigSet(self, localInst, configFile):
        """
        """
        configFile = os.path.join(localInst.datadir, configFile)
        self.logger.debug("Set private config file: %s" % configFile)
        
        if (localInst.instanceRole == INSTANCE_ROLE_CMAGENT):
            self.modifyConfigItem(localInst, configFile, "log_dir", "%s/cm/cm_agent" % DefaultValue.getUserLogDirWithUser(self.user))
        elif (localInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            self.modifyConfigItem(localInst, configFile, "listen_addresses", "'localhost,%s'" % ",".join(localInst.listenIps))
            self.modifyConfigItem(localInst, configFile, "port", str(localInst.port))
            self.modifyConfigItem(localInst, configFile, "pgxc_node_name", "'cn_%d'" % localInst.instanceId)
            self.modifyConfigItem(localInst, configFile, "pooler_port", str(localInst.haPort))
            self.modifyConfigItem(localInst, configFile, "log_directory", "'%s/pg_log/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.user), localInst.instanceId))
            self.modifyConfigItem(localInst, configFile, "audit_directory", "'%s/pg_audit/cn_%d'" % (DefaultValue.getUserLogDirWithUser(self.user), localInst.instanceId))
        elif(localInst.instanceRole == INSTANCE_ROLE_DATANODE):
            peerInsts = self.clusterInfo.getPeerInstance(localInst)
            if (len(peerInsts) != 2 and len(peerInsts) != 1):
                self.logger.logExit("Get peer instance failed!")
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

            self.modifyConfigItem(localInst, configFile, "listen_addresses", "'%s'" % ",".join(localInst.listenIps))
            self.modifyConfigItem(localInst, configFile, "port", str(localInst.port))
            self.modifyConfigItem(localInst, configFile, "pgxc_node_name", "'%s'" % nodename)
            self.modifyConfigItem(localInst, configFile, "log_directory", "'%s/pg_log/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.user), localInst.instanceId))  
            self.modifyConfigItem(localInst, configFile, "audit_directory", "'%s/pg_audit/dn_%d'" % (DefaultValue.getUserLogDirWithUser(self.user), localInst.instanceId))

            if(localInst.instanceType == MASTER_INSTANCE):
                self.modifyConfigItem(localInst, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, standbyInst.haIps[0], standbyInst.haPort))
                if(dummyStandbyInst != None):
                    self.modifyConfigItem(localInst, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                            (localInst.haIps[0], localInst.haPort, dummyStandbyInst.haIps[0], dummyStandbyInst.haPort))
            elif(localInst.instanceType == STANDBY_INSTANCE):
                self.modifyConfigItem(localInst, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, masterInst.haIps[0], masterInst.haPort))
                if(dummyStandbyInst != None):
                    self.modifyConfigItem(localInst, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                            (localInst.haIps[0], localInst.haPort, dummyStandbyInst.haIps[0], dummyStandbyInst.haPort))
            elif(localInst.instanceType == DUMMY_STANDBY_INSTANCE):
                self.modifyConfigItem(localInst, configFile, "replconninfo1", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, masterInst.haIps[0], masterInst.haPort))
                self.modifyConfigItem(localInst, configFile, "replconninfo2", "'localhost=%s localport=%d remotehost=%s remoteport=%d'" % 
                                        (localInst.haIps[0], localInst.haPort, standbyInst.haIps[0], standbyInst.haPort))
        else:
            self.logger.logExit("current instance role is invalid [%s %s]." % (localInst.datadir,  localInst,instanceRole))

    def modifyConfigItem(self, dbInstance, configFile, key, value):
        """
        """
        # comment out any existing entries for this setting
        if(dbInstance.instanceRole == INSTANCE_ROLE_CMSERVER or dbInstance.instanceRole == INSTANCE_ROLE_CMAGENT):
            cmd = "perl -pi.bak -e's/(^\s*%s\s*=.*$)/#$1/g' %s" % (key, configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Comment parameter failed!Error:%s" % output)
                
            # append new config to file
            cmd = 'echo "       " >> %s' % (configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Append null line failed!Error:%s" % output)
                
            cmd = 'echo "%s = %s" >> %s' % (key, value, configFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Append new vaule failed!Error:%s" % output)
        else:
            if(dbInstance.instanceRole == INSTANCE_ROLE_DATANODE):
                inst_type = "datanode"
            elif(dbInstance.instanceRole == INSTANCE_ROLE_COODINATOR):
                inst_type = "coordinator"
            else:
                self.logger.logExit("Invalid instance type:%s" % dbInstance.instanceRole)   
            cmd = "gs_guc set -Z %s -N %s -D %s -c \"%s=%s\"" % (inst_type, dbInstance.hostname, dbInstance.datadir, key, value)
            self.logger.debug("set parameter command:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("set parameter failed!Error:%s" % output)
    
def usage():
    pass    
            
def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "X:l:h", ["target=", "help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError(str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
        
    confFile = DefaultValue.CLUSTER_CONFIG_PATH
    logFile = ""
    target = ""
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-X"):
            confFile = value
        elif (key == "-l"):
            logFile = value
        elif (key == "--target"):
            target = value
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)

    if (not os.path.isabs(confFile)):
        GaussLog.exitWithError("Parameter input error, configure file need absolute path.")
        
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", "", confFile)
        
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log file need absolute path.")
        
    config = DilatationConfig(logFile, "", confFile)
    if (target == "clean"):
        config.clean()
    elif (target == "restore"):
        config.restore()
    elif (target == "config"):
        config.config()
    elif (target == "check"):
        config.check()
    elif (target == "localstart"):
        config.localStart()
    elif (target == "localstop"):
        config.localStop()
    elif (target == "copycso"):
        config.copyCSOFiles()
    elif (target == "config_cn_dn"):
        config.configNewNode()
    else:
        GaussLog.exitWithError("Parameter input error, unknown target.")
        
if __name__ == '__main__':
    main()
