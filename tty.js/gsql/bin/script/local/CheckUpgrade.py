'''
Created on 2014-2-17

@author: 
'''
import getopt
import shutil
import statvfs
import sys
import os
import socket
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import *

ACTION_BIGVERSION_UPGRADE = "big_upgrade"
ACTION_SMALLVERSION_UPGRADE = "small_upgrade"
CN_INSTANCE_NAME = "coordinator"
DN_INSTANCE_NAME = "datanode"
GTM_INSTANCE_NAME = "gtm"
CM_SERVER_INSTANCE_NAME = "cm_server"
CM_AGENT_INSTANCE_NAME = "cm_agent"

#############################################################################
# Global variables
#############################################################################
g_logger = None

class CmdOptions():
    
    def __init__(self):
        self.action = ""
        self.appPath = ""
        self.bakDir = ""
        self.user = ""
        self.configfile = ""
        self.logFile = ""

class CheckUpgrade():
    """
    Check application setting for upgrade
    """
    def __init__(self, appPath, bakDir, action):
        self.appPath = appPath
        self.bakDir = bakDir
        self.action = action
        self.newClusterInfo = None
        self.oldClusterInfo = None
    
    def run(self):
        """
        Check upgrade environment
        """
        #check common items
        self.__checkOsVersion()
        self.__checkSHA256()
        #check items depends on action type
        if(self.action == ACTION_SMALLVERSION_UPGRADE):
            self.__checkAppPath()
            self.__checkAppVersion()
            self.__checkBackupDir()
        elif(self.action == ACTION_BIGVERSION_UPGRADE):
            self.__checkXML()
            #should be called after self.__checkXML()  
            self.__saveClusterGroupInfo(g_opts.user, self.oldClusterInfo)
        else:
            g_logger.logExit("Invalid action: %s" % self.action)

    def __checkXML(self):
        """
        Check if the config in xml is similar to old cluster 
        """
        g_logger.log("begin check xml ...")
        self.__initClusterInfo()
        self.__checkClusterConfigInfo()
        self.__checkNodeConfigInfo()
        self.__checkPortConflict()
        self.__checkPathConflict()
        g_logger.log("check xml succeed.")

    def __saveClusterGroupInfo(self, user, clusterInfo):
        """
        save group info of old cluster, should be called after checkXML.
        only need do this when there is a cn on this node 
        """
        g_logger.log("begin save old cluster group info...")
        tmpDir = DefaultValue.getTmpDirAppendMppdb(user)
        groupInfoFile = "%s/groupinfo_out" % tmpDir
        if(os.path.isfile(groupInfoFile)):
            os.remove(groupInfoFile)

        sqlFile = "%s/upgrade_groupinfo.sql" % tmpDir
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)

        try:
            #check if there is a cn on this node
            hostname = socket.gethostname()
            currentNode = clusterInfo.getDbNodeByName(hostname)
            if(currentNode == None):
                raise Exception("there is no node named %s in current cluster" % hostname)
            if(len(currentNode.coordinators) != 1):
                g_logger.debug("there is no coordinator on current node, nothing to do.")
                return
                
            #get cn port
            cnPort = currentNode.coordinators[0].port
            
            #build sql statement
            sql = "COPY pgxc_group TO '%s';" % groupInfoFile

            #build sql file
            cmd = "echo \"%s\" > %s 2>/dev/null;chmod 640 %s" % (sql, sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("create sql file failed: %s" % output)
            
            #exec sql file
            cmd = "su - %s -c 'gsql -d postgres -p %s -f %s -X'" % (user, cnPort, sqlFile)
            g_logger.debug("copy cluster group info to tmp file cmd: %s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0 or output.find("ERROR") >= 0):
                raise Exception("copy group info to tmp file failed: %s" % output)
            
            #check if group info file been created
            if(not os.path.isfile(groupInfoFile)):
                raise Exception("create group info file failed: %s does not exist." % groupInfoFile)
            
            #clean tmp sql file
            if(os.path.isfile(sqlFile)):
                os.remove(sqlFile)
            
            g_logger.log("save old cluster group info finished.")
        except Exception, e:
            if(os.path.isfile(groupInfoFile)):
                os.remove(groupInfoFile)
            if(os.path.isfile(sqlFile)):
                os.remove(sqlFile)
            g_logger.logExit("save old cluster group info failed:%s" % str(e))

    def __initClusterInfo(self):
        """
        init the cluster info of new cluster and old cluster
        """
        self.newClusterInfo = dbClusterInfo()
        self.newClusterInfo.initFromXml(g_opts.configfile)
        g_logger.debug("New cluster Info:\n%s" % str(self.newClusterInfo))

        installDir = DefaultValue.getInstallDir(g_opts.user)
        if(installDir == ""):
            self.logger.logExit("get install of user %s failed." % g_opts.user)
        sys.path.append("%s/bin/script/util" % installDir)
        module = __import__('DbClusterInfo')
        self.oldClusterInfo = module.dbClusterInfo()
        self.oldClusterInfo.initFromStaticConfig(g_opts.user)
        g_logger.debug("Old cluster Info:\n%s" % str(self.oldClusterInfo))

    def __checkPortConflict(self):
        """
        check if any port conflict
        """
        g_logger.log("begin check port conflict...")
        newClusterPorts = {}
        oldClusterPorts = {}
        #get port info of new cluster
        for dbNode in self.newClusterInfo.dbNodes:
            dbInstList = []
            portList = []
            dbInstList.extend(dbNode.cmservers)
            dbInstList.extend(dbNode.coordinators)
            dbInstList.extend(dbNode.datanodes)
            dbInstList.extend(dbNode.gtms)
            for dbInst in dbInstList:
                portList.append(dbInst.port)
                portList.append(dbInst.haPort)
            newClusterPorts[dbNode.name] = portList
        #get port info of old cluster
        for dbNode in self.oldClusterInfo.dbNodes:
            dbInstList = []
            portList = []
            dbInstList.extend(dbNode.cmservers)
            dbInstList.extend(dbNode.coordinators)
            dbInstList.extend(dbNode.datanodes)
            dbInstList.extend(dbNode.gtms)
            for dbInst in dbInstList:
                portList.append(dbInst.port)
                portList.append(dbInst.haPort)
            oldClusterPorts[dbNode.name] = portList

        #check port conflict node by node
        for nodename in newClusterPorts:
            for port in newClusterPorts[nodename]:
                if(port in oldClusterPorts[nodename]):
                    g_logger.logExit("found port conflict: port %s is used both in new and old clusters on node %s" % (port, nodename))
        g_logger.log("check port conflict succeed.")

    def __checkPathConflict(self):
        """
        check if any path conflict
        """
        g_logger.log("begin check path conflict...")
        newClusterPaths = {}
        oldClusterPaths = {}
        #get path info of new cluster
        for dbNode in self.newClusterInfo.dbNodes:
            dbInstList = []
            pathList = []
            pathList.append(dbNode.cmDataDir)
            dbInstList.extend(dbNode.cmservers)
            dbInstList.extend(dbNode.coordinators)
            dbInstList.extend(dbNode.datanodes)
            dbInstList.extend(dbNode.gtms)
            for dbInst in dbInstList:
                pathList.append(dbInst.datadir)
            newClusterPaths[dbNode.name] = pathList
        #get path info of old cluster
        for dbNode in self.oldClusterInfo.dbNodes:
            dbInstList = []
            pathList = []
            pathList.append(dbNode.cmDataDir)
            dbInstList.extend(dbNode.cmservers)
            dbInstList.extend(dbNode.coordinators)
            dbInstList.extend(dbNode.datanodes)
            dbInstList.extend(dbNode.gtms)
            for dbInst in dbInstList:
                pathList.append(dbInst.datadir)
            oldClusterPaths[dbNode.name] = pathList
        #check path conflict node by node
        for nodename in newClusterPaths:
            for path in newClusterPaths[nodename]:
                if(path in oldClusterPaths[nodename]):
                    g_logger.logExit("found path conflict: path %s is used both in new and old clusters on node %s" % (path, nodename))

        #check install path
        if(self.newClusterInfo.appPath == self.oldClusterInfo.appPath):
            g_logger.logExit("found path conflict: path %s is used both in new and old clusters" % (self.newClusterInfo.appPath))
        g_logger.log("check path conflict succeed.")

    def __checkClusterConfigInfo(self):
        """
        check the config info on cluster level
        """
        #check cluster basic info
        if(self.newClusterInfo.name != self.oldClusterInfo.name):
            g_logger.logExit("cluster name are different. new cluster name: %s, old cluster name:%s" % (self.newClusterInfo.name, self.oldClusterInfo.name))

    def __checkNodeConfigInfo(self):
        """
        check the config info on node level
        """
        #check node number
        if(len(self.newClusterInfo.dbNodes) != len(self.oldClusterInfo.dbNodes)):
            g_logger.logExit("there are %s nodes in new cluster and %s nodes in old cluster, they should be same." 
                % (len(self.newClusterInfo.dbNodes), len(self.oldClusterInfo.dbNodes)))
        #check node info
        for dbNodeNew in self.newClusterInfo.dbNodes:
            g_logger.log("begin check node %s..." % dbNodeNew.name)
            dbNodeOld = self.oldClusterInfo.getDbNodeByName(dbNodeNew.name)
            if(dbNodeOld == None):
                g_logger.logExit("node %s in new cluster is not found in old cluster." % dbNodeNew.name)
            if(dbNodeNew.id != dbNodeOld.id):
                g_logger.logExit("node id is different. new cluster is %s, old cluster is %s" % (dbNodeNew.id, dbNodeOld.id))

            #check instances
            self.__checkInstanceConfigInfo(dbNodeNew.cmservers, dbNodeOld.cmservers, CM_SERVER_INSTANCE_NAME) 
            self.__checkInstanceConfigInfo(dbNodeNew.coordinators, dbNodeOld.coordinators, CN_INSTANCE_NAME) 
            self.__checkInstanceConfigInfo(dbNodeNew.datanodes, dbNodeOld.datanodes, DN_INSTANCE_NAME) 
            self.__checkInstanceConfigInfo(dbNodeNew.gtms, dbNodeOld.gtms, GTM_INSTANCE_NAME)
            self.__checkInstanceConfigInfo(dbNodeNew.cmagents, dbNodeOld.cmagents, CM_AGENT_INSTANCE_NAME)
            
    def __checkIps(self, newIpList, oldIpList):
        """
        check the ip info
        """
        newIpList.sort()
        oldIpList.sort()
        if(len(newIpList) != len(oldIpList)):
            g_logger.logExit("ip number is different. new cluster is %s, old cluster is %s" % (len(newIpList), len(oldIpList)))
        for ip_index in range(len(newIpList)):
            if(newIpList[ip_index] != oldIpList[ip_index]):
                g_logger.logExit("the %s ip is different. new cluster is %s, old cluster is %s" % (ip_index + 1, newIpList[ip_index], oldIpList[ip_index]))

    def __checkInstanceConfigInfo(self, newInstances, OldInstances, instance_tag):
        """
        check the config info on instance level
        """
        g_logger.log("begin check %s ..." % instance_tag)
        if(instance_tag == DN_INSTANCE_NAME):
            newInstances = [instance for instance in newInstances if instance.instanceType != DUMMY_STANDBY_INSTANCE]
            if(hasattr(OldInstances[0], 'instanceType')):
                OldInstances = [instance for instance in OldInstances if instance.instanceType != DUMMY_STANDBY_INSTANCE]
        if(len(newInstances) != len(OldInstances)):
            g_logger.logExit("%s num is different. new cluster is %s, old cluster is %s" % (instance_tag, len(newInstances), len(OldInstances)))
        g_logger.log("check %s finished." % instance_tag)
            
    def __getInstanceById(self, instanceList, instanceId):
        """
        get instance from given instance list, using given instance id.
        """
        res = None
        for instance in instanceList:
            if(instance.instanceId == instanceId):
                res = instance
                break
        return res
    
    def __checkOsVersion(self):
        """
        Check os version
        """
        if (not PlatformCommand.checkOsVersion()):
            g_logger.logExit("Can't support current os version!")
    
    def __checkAppPath(self):
        if (not os.path.isdir(self.appPath)):
            g_logger.logExit("Application path does not exist!Path: %s" % self.appPath)
            
        static_config = "%s/bin/cluster_static_config" % self.appPath
        if (not os.path.isfile(static_config)):
            g_logger.logExit("cluster static config does not exist!Path: %s" % static_config)
        
    def __checkAppVersion(self):
        """
        Check version
        """
        curVer = DefaultValue.getAppVersion(self.appPath)
        if (curVer == ""):
            g_logger.logExit("Get current version failed!")
        
        # TODO: debug info
        
    def __checkSHA256(self):
        '''
        Check the sha256 of new version
        '''
        try:
            binPath = DefaultValue.getBinFilePath()
            sha256Path = DefaultValue.getSHA256FilePath()
            
            fileSHA256 = PlatformCommand.getFileSHA256(binPath)
            sha256Value = PlatformCommand.readFileLine(sha256Path)
            if(fileSHA256 != sha256Value):
                g_logger.logExit("The sha256 value is different!\nBin file%s\nSHA256 file:%s." % (fileSHA256, sha256Value))  
        except Exception, e:
            g_logger.logExit("Check sha256 failed.Error: %s" % str(e))
            
    def __checkBackupDir(self):
        """
        Check if backup dir exists and is empty
        """
        if (os.path.isdir(self.bakDir)):
            shutil.rmtree(self.bakDir)
        os.makedirs(self.bakDir, DefaultValue.DIRECTORY_MODE)
        
        vfs = os.statvfs(self.bakDir)
        availableSize = vfs[statvfs.F_BAVAIL] * vfs[statvfs.F_BSIZE] / (1024 * 1024)
        g_logger.debug("Backup directory available size:%d M" % availableSize)
        if(availableSize < DefaultValue.APP_DISK_SIZE):
            g_logger.logExit("Backup directory available size smaller than 100M, current size is:%d M" % availableSize)

def usage():
    """
Usage:
  python CheckUpgrade.py -t action -R installpath [-B backup_dir] [-U user] [-X xmlfile] [-l log]
Common options:
  -t                                the type of action
  -l                                the path of log file
  --help                            show this help, then exit
Options for big version upgrade check
  -U                                the user of old cluster
  -X                                the xml file path of new cluster
Options for small version upgrade check
  -R                                the install path of old cluster
  -B                                the backup path of old cluster
    """
    print usage.__doc__

def parseCommandLine():
    """
    Parse command line and save to global variable
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:R:B:U:X:l:", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-R"):
            g_opts.appPath = value
        elif (key == "-B"):
            g_opts.bakDir = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-X"):
            g_opts.configfile = value
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

    #only check need parameter, just ignore no need parameter
    if(g_opts.action == ACTION_BIGVERSION_UPGRADE):
        if (g_opts.user == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        if (g_opts.configfile == ""):
            GaussLog.exitWithError("Parameter input error, need '-X' parameter.")
    elif(g_opts.action == ACTION_SMALLVERSION_UPGRADE):
        if (g_opts.bakDir == ""):
            GaussLog.exitWithError("Parameter input error, need '-B' parameter.")
        if (g_opts.appPath == ""):
            GaussLog.exitWithError("Parameter input error, need '-R' parameter.")
    else:
        GaussLog.exitWithError("Invalid action: %s" % g_opts.action)

    if(g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", g_opts.appPath, "")

def initLogger():
    """
    Init logger
    """
    global g_logger
    g_logger = GaussLog(g_opts.logFile, "CheckUpgrade")
    

if __name__ == '__main__':
    """
    main function
    """
    g_opts = CmdOptions()
    parseCommandLine()
    initLogger()
    
    try:
        g_logger.log("Checking upgrade environment...")
        checker = CheckUpgrade(g_opts.appPath, g_opts.bakDir, g_opts.action)
        checker.run()
        g_logger.log("Check upgrade environment successfully!")
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.log(str(e))
        g_logger.logExit("Check upgrade environment failed!")


        
