'''
Created on 2014-3-1

@author: 
'''
import os
import socket
import sys
import threading
import time
import signal
from datetime import datetime, timedelta

from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.DbClusterStatus import DbClusterStatus
from script.util.SshTool import SshTool
from script.util.Common import *
from multiprocessing import Process       

class OMCommand():
    """
    Descript command of om
    """
    Current_Path = os.path.dirname(os.path.abspath(__file__))
    
    Local_Backup = os.path.normpath(Current_Path + "/../local/Backup.py")
    Local_Backup_Data = os.path.normpath(Current_Path + "/../local/BackupData.py")
    Local_Backup_Status = os.path.normpath(Current_Path + "/../local/BackupStatus.py")
    Local_Query = os.path.normpath(Current_Path + "/../local/LocalQuery.py")
    Local_Check_Config = os.path.normpath(Current_Path + "/../local/CheckConfig.py")
    Local_Check_Install = os.path.normpath(Current_Path + "/../local/CheckInstall.py")
    Local_Check_Uninstall = os.path.normpath(Current_Path + "/../local/CheckUninstall.py")
    Local_Clean_Instance = os.path.normpath(Current_Path + "/../local/CleanInstance.py")
    Local_Check_Running = os.path.normpath(Current_Path + "/../local/CheckRunStatus.py")
    Local_Check_Upgrade = os.path.normpath(Current_Path + "/../local/CheckUpgrade.py")
    Local_Clean_OsUser = os.path.normpath(Current_Path + "/../local/CleanOsUser.py")
    Local_Config_Hba = os.path.normpath(Current_Path + "/../local/ConfigHba.py")
    Local_Config_Instance = os.path.normpath(Current_Path + "/../local/ConfigInstance.py")
    Local_Create_OsUser = os.path.normpath(Current_Path + "/../local/CreateOsUser.py")
    Local_Health_Check = os.path.normpath(Current_Path + "/../local/HealthCheck.py")
    Local_Init_Instance = os.path.normpath(Current_Path + "/../local/InitInstance.py")
    Local_Install = os.path.normpath(Current_Path + "/../local/Install.py")
    Local_Lock_Cluster = os.path.normpath(Current_Path + "/../local/LockCluster.py")
    Local_Replace_Config = os.path.normpath(Current_Path + "/../local/ReplaceConfig.py")
    Local_Restore = os.path.normpath(Current_Path + "/../local/Restore.py")
    Local_Uninstall = os.path.normpath(Current_Path + "/../local/Uninstall.py")
    Local_Upgrade_Config = os.path.normpath(Current_Path + "/../local/UpgradeConfig.py")
    Local_Dilatation_Config = os.path.normpath(Current_Path + "/../local/DilatationConfig.py")
    LOCAL_STOP_SERVER = os.path.normpath(Current_Path + "/../local/StopLocalServer.py")
    Local_Cgroup = os.path.normpath(Current_Path + "/../local/Cgroup.py")
    Local_PreInstall = os.path.normpath(Current_Path + "/../local/PreInstallUtility.py")
    Local_Check_PreInstall = os.path.normpath(Current_Path + "/../local/CheckPreInstall.py")
    Local_UnPreInstall = os.path.normpath(Current_Path + "/../local/UnPreInstallUtility.py")
    Local_Clean_Firewall = os.path.normpath(Current_Path + "/../local/CleanFireWall.py")
    Local_Roach = os.path.normpath(Current_Path + "/../local/LocalRoach.py")
    Gauss_UnInstall = os.path.normpath(Current_Path + "/../GaussUninstall.py")
    Gauss_Backup = os.path.normpath(Current_Path + "/../GaussBackup.py")
    
    @staticmethod
    def getSetCronCmd(user, appPath):
        """
        Set crontab
        """
        log_path = DefaultValue.getOMLogPath(DefaultValue.OM_MONITOR_DIR_FILE, "", appPath)
        cronFile = "%s/gauss_cron_%d" % (DefaultValue.getTmpDirFromEnv(), os.getpid())
        cmd = "crontab -l > %s;" % cronFile
        cmd += "sed -i '/\\/bin\\/om_monitor/d' %s; " % cronFile
        cmd += "echo \"*/1 * * * * source /etc/profile;source ~/.bashrc;nohup %s/bin/om_monitor -L %s >>/dev/null 2>&1 &\" >> %s;" % (appPath, log_path, cronFile)
        cmd += "crontab -u %s %s;service cron restart;" % (user, cronFile)
        cmd += "rm -f %s" % cronFile
        
        return cmd
    
    @staticmethod
    def getRemoveCronCmd(user):
        """
        Remove crontab
        """
        cmd = "crontab -u %s -r;service cron restart" % user
        
        return cmd

    @staticmethod
    def doCheckStaus(user, nodeId, cluster_normal_status = None):
        """
        Check cluster status
        """
        if (cluster_normal_status is None):
            cluster_normal_status = [DbClusterStatus.CLUSTER_STATUS_NORMAL]
        statusFile = "/home/%s/gauss_check_status_%d.dat" % (user, os.getpid())
        cmd = ClusterCommand.getQueryStatusCmd(user, 0, statusFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            if (os.path.exists(statusFile)):os.remove(statusFile)
            return (status, output)
        
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(statusFile)
        if (os.path.exists(statusFile)):os.remove(statusFile)
        
        status = 0
        output = ""
        statusRep = None
        if (nodeId > 0):
            nodeStatus = clusterStatus.getDbNodeStatusById(nodeId)
            if (nodeStatus is None):
                raise Exception("Can't find node in cluster!")
            
            status = 0 if nodeStatus.isNodeHealthy() else 1
            statusRep = nodeStatus.getNodeStatusReport()
        else:
            status = 0 if clusterStatus.isAllHealthy(cluster_normal_status) else 1
            statusRep = clusterStatus.getClusterStatusReport()
            output += "cluster_state      : %s\n" % clusterStatus.clusterStatus
            output += "node_count         : %d\n" % statusRep.nodeCount

        output += "Coordinator State\n"
        output += "    normal         : %d\n" % statusRep.cooNormal
        output += "    abnormal       : %d\n" % statusRep.cooAbnormal
        output += "GTM State\n"
        output += "    primary        : %d\n" % statusRep.gtmPrimary
        output += "    standby        : %d\n" % statusRep.gtmStandby
        output += "    abnormal       : %d\n" % statusRep.gtmAbnormal
        output += "    down           : %d\n" % statusRep.gtmDown
        output += "Datanode State\n"
        output += "    primary        : %d\n" % statusRep.dnPrimary
        output += "    standby        : %d\n" % statusRep.dnStandby
        output += "    secondary      : %d\n" % statusRep.dnDummy
        output += "    building       : %d\n" % statusRep.dnBuild
        output += "    abnormal       : %d\n" % statusRep.dnAbnormal
        output += "    down           : %d\n" % statusRep.dnDown
        
        return (status, output)
    @staticmethod
    def getClusterStatus(user):
        """
        get cluster status
        """
        statusFile = "/home/%s/gauss_check_status_%d.dat" % (user, os.getpid())
        cmd = ClusterCommand.getQueryStatusCmd(user, 0, statusFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            if (os.path.exists(statusFile)):os.remove(statusFile)
            return None
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(statusFile)
        if (os.path.exists(statusFile)):os.remove(statusFile)
        return clusterStatus

class LocalCommand():
    """
    Base class for local command
    """
    def __init__(self, logFile, user, clusterConf = None):
        self.logger = GaussLog(logFile, self.__class__.__name__)
        self.clusterInfo = None
        self.dbNodeInfo = None
        self.clusterConfig = clusterConf
        self.user = user
        self.group = ""
        
    def readConfigInfo(self):
        """
        Read config from static config file
        """
        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromStaticConfig(self.user)
            hostName = socket.gethostname()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.debug("Instance info on local node:\n%s" % str(self.dbNodeInfo))
        
    def readConfigInfoByXML(self):
        """
        Read config from static config file
        """
        try:
            if (self.clusterConfig is None):
                self.logger.logExit("Get local instance info failed!There is no XML assigned!")
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromXml(self.clusterConfig)
            hostName = socket.gethostname()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.debug("Instance info on local node:\n%s" % str(self.dbNodeInfo))
        
    def getUserInfo(self):
        """
        Get user and group
        """
        (self.user, self.group) = PlatformCommand.getPathOwner(self.clusterInfo.appPath)
        if (self.user == "" or self.group == ""):
            self.logger.logExit("Get user info failed!")
class Timeout(Exception): pass
            
class ParallelCommand():
    """
    Base class of parallel command
    """
    ACTION_INSTALL = "install"
    ACTION_CONFIG = "config"
    ACTION_START = "start"
    ACTION_REDISTRIBUTE = "redistribute"
    ACTION_HEALTHCHECK = "healthcheck"
    
    HEALTH_CHECK_BEFORE = "before"
    HEALTH_CHECK_AFTER = "after"
    """
    Base class for parallel command
    """
    def __init__(self):
        self.logger = None
        self.clusterInfo = None
        self.sshTool = None
        self.action = ""
        self.xmlFile = ""
        self.logType = DefaultValue.DEFAULT_LOG_FILE
        self.logFile = ""
        self.localLog = ""
        self.user = ""
        self.group = ""
        self.rollbackCommands = []
            
    def initLogger(self):
        """
        Init logger
        """
        self.logger = GaussLog(self.logFile)
        
        dirName = os.path.dirname(self.logFile)
        self.localLog = os.path.join(dirName, DefaultValue.LOCAL_LOG_FILE)
        
    def initClusterInfo(self):
        """
        Init cluster info
        """
        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromXml(self.xmlFile)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.debug("Instance info of cluster:\n%s" % str(self.clusterInfo))
        
    def initClusterInfoFromStaticFile(self, user):
        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromStaticConfig(user)
        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.debug("Instance info of cluster:\n%s" % str(self.clusterInfo))
    def initSshTool(self, nodeNames):
        """
        Init ssh tool
        """
        self.sshTool = SshTool(nodeNames, self.logger.logFile)
        
    def getUserInfo(self):
        """
        Get user and group
        """
        (self.user, self.group) = PlatformCommand.getPathOwner(self.clusterInfo.appPath)
        if (self.user == "" or self.group == ""):
            self.logger.logExit("Get user info failed!")

    def checkBaseFile(self, checkXml = True):
        """
        Check xml file and log file
        """
        if (checkXml):
            if (not os.path.exists(self.xmlFile)):
                GaussLog.exitWithError("Config file does not exist: %s" % self.xmlFile)
                
            if (not os.path.isabs(self.xmlFile)):
                GaussLog.exitWithError("Parameter input error, configure file need absolute path.")
        else:
            self.xmlFile = ""
            
        if (self.logFile == ""):
            self.logFile = DefaultValue.getOMLogPath(self.logType, self.user, "", self.xmlFile)
            
        if (not os.path.isabs(self.logFile)):
            GaussLog.exitWithError("Parameter input error, log path need absolute path.")
        
    def initSignalHandler(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)        
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)
        
    def print_signal_stack(self, frame):
        if (self.logger is None):
            return
        try:
            import inspect
            stacks = inspect.getouterframes(frame)
            for curr in range(len(stacks)):
                stack = stacks[curr]
                self.logger.debug("Stack level: %d, File: %s, Function: %s LineNo: %d" % (curr, stack[1], stack[3], stack[2]))
                self.logger.debug("Code: %s" % (stack[4][0].strip().strip("\n")))
        except Exception as e:
            self.logger.debug("Print signal stack failed. Error: %s" % str(e))
    def raise_handler(self, signal, frame):
        if (self.logger is not None):
            self.logger.debug("Receive signal[%d]." % (signal))
            self.print_signal_stack(frame)
        raise Exception("Receive signal[%d]." % (signal))
    def setupTimeoutHandler(self):
        signal.signal(signal.SIGALRM, self.timeout_handler) 
    def setTimer(self, timeout):
        self.logger.debug("Set timer. Timeout %d" % timeout)
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(timeout)
    def resetTimer(self):
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        self.logger.debug("Reset timer. Left time %d." % signal.alarm(0))
    def timeout_handler(self, signal, frame):
        if (self.logger is not None):
            self.logger.debug("Receive time out signal[%d]." % (signal))
            self.print_signal_stack(frame)
        raise Timeout("Time out...")
    
    def switchNodeToStandby(self, nodeName):
        """
        Switch all instances on a node to standby
        """
        self.logger.log("Begin to switch node to be standby...")
        
        dbNode = self.clusterInfo.getDbNodeByName(nodeName)
        if (dbNode is None):
            self.logger.logExit("Get info of node[%s] failed!" % nodeName)
        
        instList = dbNode.datanodes + dbNode.gtms
        for inst in instList:
            peerInstList = self.clusterInfo.getPeerInstance(inst)
            if (len(peerInstList) != 2 and len(peerInstList) != 1):
                raise Exception("Get peer instance failed!DataDir:%s" % inst.datadir)
            
            peerInst = peerInstList[0]
            peerNode = self.clusterInfo.getDbNodeByName(peerInst.hostname)
            if (peerNode is None):
                raise Exception("Get peer node failed!DataDir:%s" % inst.datadir)
            cmd = ClusterCommand.getSwitchOverCmd(self.user, peerNode.id, peerInst.datadir)
            
            self.logger.debug("Switch over command: %s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.debug(output)
                raise Exception("Switch instance to standby failed!DataDir:%s" % inst.datadir)
        
        self.logger.log("Switch node to be standby finished.")
        
    def checkHaSync(self, nodeName, instList=[]):
        """
        Check if all instance on the node are sync.
        """
        statusFile = "/home/%s/gauss_check_status_%d.dat" % (self.user, os.getpid())
        cmd = ClusterCommand.getQueryStatusCmd(self.user, 0, statusFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            if (os.path.exists(statusFile)):os.remove(statusFile)
            self.logger.debug("Get cluster status failed!Output:%s." % output)
            return False
        
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(statusFile)
        if (os.path.exists(statusFile)):os.remove(statusFile)
        localNodeStatus = clusterStatus.getDbNodeStatusByName(nodeName)
        if (localNodeStatus is None):
            self.logger.debug("Get node status failed!There is no status info about node[%s]!" % nodeName)
            return False

        isAllSync = True
        localNode = self.clusterInfo.getDbNodeByName(nodeName)
        if (localNode is None):
            raise Exception("Get node info failed!There is no info about node[%s]!" % nodeName)
        if (len(instList) == 0):
            instList = localNode.datanodes + localNode.gtms
        for localInst in instList:
            localInstStatus = localNodeStatus.getInstanceByDir(localInst.datadir)
            if (localInstStatus is None):
                self.logger.debug("Get instance status failed!There is no status info about instance[%s]!" % localInst.datadir)
                isAllSync = False
                continue
            
            if (not localInstStatus.isInstanceHealthy()):
                self.logger.debug("Local instance[%s] on node[%s] is not healthy!" % (localInst.datadir, nodeName))
                isAllSync = False
                continue
            
            if (localInstStatus.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                if (localInstStatus.syncStatus == DbClusterStatus.DATA_STATUS_SYNC):
                    self.logger.debug("Local instance[%s] on node[%s] is primary and sync." % (localInstStatus.datadir, localInstStatus.nodeId))
                else:
                    self.logger.debug("local instance[%s] on node[%s] is primary but not sync." % (localInstStatus.datadir, localInstStatus.nodeId))
                    isAllSync = False
            else:
                self.logger.debug("Local instance[%s] on node[%s] is standby." % (localInstStatus.datadir, localInstStatus.nodeId))
                peerInstList = self.clusterInfo.getPeerInstance(localInst)
                if (len(peerInstList) == 0):
                    raise Exception("Get peer instance of local instance[%s] on node[%s] failed!" % (localInst.datadir, localInstStatus.nodeId))
                peerInst = peerInstList[0]

                peerInstaStatus = clusterStatus.getInstanceStatusById(peerInst.instanceId)
                if (peerInstaStatus is None):
                    self.logger.debug("Get peer instance status failed!There is no status info about instance[%s]!" % peerInst.datadir)
                    isAllSync = False
                    continue
                
                if (not peerInstaStatus.isInstanceHealthy()):
                    self.logger.debug("Peer instance[%s] on node[%s] is not healthy!" % (peerInstaStatus.datadir,peerInstaStatus.nodeId))
                    isAllSync = False
                    continue
                
                if (peerInstaStatus.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                    if (peerInstaStatus.syncStatus == DbClusterStatus.DATA_STATUS_SYNC):
                        self.logger.debug("Peer instance[%s] on node[%s] is primary and sync." % (peerInstaStatus.datadir,peerInstaStatus.nodeId))
                    else:
                        self.logger.debug("Peer instance[%s] on node[%s] is primary but not sync." % (peerInstaStatus.datadir,peerInstaStatus.nodeId))
                        isAllSync = False
                else:
                    self.logger.debug("Peer instance[%s] on node[%s] is not primary." % (peerInstaStatus.datadir,peerInstaStatus.nodeId))
                    isAllSync = False

        return isAllSync
    
    def waitForSync(self, nodeName, timeout=DefaultValue.TIMEOUT_CLUSTER_SYNC):
        """
        Wait the node become normal
        """
        self.logger.log("Wait node[%s] to become sync..." % nodeName)
        endTime = None if timeout <= 0 else datetime.now() + timedelta(seconds=timeout)
        
        dotCount = 0
        isSync = False
        while True:
            time.sleep(5)
            sys.stdout.write(".")
            dotCount += 1
            if (dotCount >= 12):
                dotCount = 0
                sys.stdout.write("\n")
            
            isSync = self.checkHaSync(nodeName)
            if (isSync):
                if (dotCount != 0): sys.stdout.write("\n")
                self.logger.log("Node[%s] is sync." % nodeName)
                break
            
            if (endTime is not None and datetime.now() >= endTime):
                if (dotCount != 0): sys.stdout.write("\n")
                self.logger.log("Timeout!Node[%s] is still not sync!" % nodeName)
                break

        if (not isSync):
            raise Exception("Wait node[%s] to become sync failed!" % nodeName)
        
        self.logger.log("Wait node[%s] to become sync finished!" % nodeName)
    
    def waitForFailover(self, timeout=DefaultValue.TIMEOUT_CLUSTER_FAILOVER):
        """
        Wait the cluster fail over
        """
        self.logger.log("Wait for promoting peer instances...")
        endTime = None if timeout <= 0 else datetime.now() + timedelta(seconds=timeout)
        
        dotCount = 0
        checkStatus = 0
        while True:
            time.sleep(5)
            sys.stdout.write(".")
            dotCount += 1
            if (dotCount >= 12):
                dotCount = 0
                sys.stdout.write("\n")
                
            if (endTime is not None and datetime.now() >= endTime):
                if (dotCount != 0): sys.stdout.write("\n")
                checkStatus = 1
                self.logger.log("Timeout!Cluster is still not available!")
                break
            
            statusFile = "/home/%s/gauss_check_status_%d.dat" % (self.user, os.getpid())
            cmd = ClusterCommand.getQueryStatusCmd(self.user, 0, statusFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                if (os.path.exists(statusFile)):os.remove(statusFile)
                self.logger.debug("Get cluster status failed!Output:%s." % output)
                continue
                
            clusterStatus = DbClusterStatus()
            clusterStatus.initFromFile(statusFile)
            if (os.path.exists(statusFile)):os.remove(statusFile)
            if (clusterStatus.clusterStatus == "Degraded"):
                if (dotCount != 0): sys.stdout.write("\n")
                self.logger.log("Cluster is degrade now!")
                break
            else:
                self.logger.debug("Cluster is %s(%s) now." % (clusterStatus.clusterStatus, clusterStatus.clusterStatusDetail))
     
        if (checkStatus != 0):
            raise Exception("Promote peer instances failed!")
                
        self.logger.log("Promote peer instances finished!")

    def stopNode(self, user="", nodeName="", stopForce=False, timeout=DefaultValue.TIMEOUT_CLUSTER_STOP):
        """
        Stop all Instances on the node
        """
        self.logger.log("Begin to stop node...")
        if(user == ""):
            user = self.user
        nodeId = 0
        if(nodeName != ""):
            clusterInfo = dbClusterInfo()
            clusterInfo.initFromStaticConfig(user)
            dbNode = clusterInfo.getDbNodeByName(nodeName)
            if(dbNode == None):
                raise Exception("No node named %s" % nodeName)
            nodeId = dbNode.id
        
        stopMode = "i" if stopForce else "f"
        cmd = ClusterCommand.getStopCmd(user, nodeId, stopMode, timeout)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.debug("Stop failed for the first time!Output: %s" % output)
            
            self.logger.log("Try forcibly stop!")
            cmd = ClusterCommand.getStopCmd(user, nodeId, "i")
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.debug("Stop forcibly failed!Output: %s" % output)
                raise Exception("Stop node failed!")
        
        self.logger.log("Stop node finished.")
        
    def startNode(self, user="", nodeName="", doCheck=True, timeout=DefaultValue.TIMEOUT_CLUSTER_START):
        """
        Start all instance on the node
        """
        if(user == ""):
            user = self.user
        nodeId = 0
        startType = "cluster"
        if (nodeName != ""):
            startType = "node"
            clusterInfo = dbClusterInfo()
            clusterInfo.initFromStaticConfig(user)
            dbNode = clusterInfo.getDbNodeByName(nodeName)
            if(dbNode == None):
                raise Exception("No node named %s" % nodeName)
            nodeId = dbNode.id
        
        endTime = None
        if (timeout > 0):
            endTime = datetime.now() + timedelta(seconds=timeout)
        
        self.logger.log("Begin to start %s..." % startType)
        cmd = ClusterCommand.getStartCmd(user, nodeId, timeout)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.debug(output)
            raise Exception("Start %s failed!" % startType)
        
        if (doCheck):
            self.waitForNormal(user, nodeId, endTime)
        
        self.logger.log("Start %s finished." % startType)
        
    def waitForNormal(self, user="", nodeId=0, endTime=None):
        """
        Wait the node become normal
        """
        checkStatus = 0
        checkResult = ""
        startType = "cluster" if nodeId == 0 else "node"
        self.logger.log("%s is starting..." % startType)
        dotCount = 0
        while True:
            time.sleep(5)
            sys.stdout.write(".")
            dotCount += 1
            if (dotCount >= 12):
                dotCount = 0
                sys.stdout.write("\n")
            
            checkStatus = 0
            checkResult = ""
            (checkStatus, checkResult) = OMCommand.doCheckStaus(user, nodeId)
            if (checkStatus == 0):
                if (dotCount != 0): sys.stdout.write("\n")
                self.logger.log("Start %s successfully." % startType)
                break
            if (endTime is not None and datetime.now() >= endTime):
                if (dotCount != 0): sys.stdout.write("\n")
                self.logger.log("Timeout!Start %s failed!" % startType)
                break

        if (checkStatus != 0):
            self.logger.debug(checkResult)
            raise Exception("Start %s failed!" % startType)

    def checkPreInstall(self, user, nodes = None):
        """
        check if have done preinstall on given nodes
        """
        try:
            cmd = "python %s -U %s" % (OMCommand.Local_Check_PreInstall, user)
            if(nodes is None):
                self.sshTool.executeCommand(cmd, "Check preinstall") 
            else:
                self.sshTool.executeCommand(cmd, "Check preinstall", 0, nodes) 
        except Exception, e:
            self.logger.logExit(str(e))
        
    def checkNodeInstall(self, nodes = None, checkParams = [], strictUserCheck = True):
        validParam = ["shared_buffers", "max_connections"]    
        cooGucParam = ""
        for param in checkParams:
            entry = param.split("=")
            if (len(entry) != 2):
                self.logger.logExit("Error guc parameter: %s" % param)
            if (entry[0].strip() in validParam):
                cooGucParam += " -C \\\"%s\\\"" % param
        self.logger.log("Check installation environment on all nodes...")
        cmd = "python %s -U %s:%s -R %s %s -l %s" % (OMCommand.Local_Check_Install, self.user, self.group, self.clusterInfo.appPath, cooGucParam, self.localLog)
        if (not strictUserCheck):
            cmd += " -O"
        self.logger.debug("Check install cmd: %s" % cmd)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Check installation environment")
        else:
            self.sshTool.executeCommand(cmd, "Check installation environment", 0, nodes)

    def createOsUser(self, nodes = None, passwd = "", strictUserCheck = True):
        self.logger.log("Create os user on all nodes...")
        cmd = "python %s -U %s:%s -l %s" % (OMCommand.Local_Create_OsUser, self.user, self.group, self.localLog)        
        if (not strictUserCheck):
            cmd += " -O"
        else:
            self.rollbackCommands.insert(0, ["python %s -U %s -l %s" % (OMCommand.Local_Clean_OsUser, self.user, self.localLog), nodes])
        self.rollbackCommands.insert(0, ["(if [ -f /tmp/temp.%s ];then rm -f /tmp/temp.%s;fi)" % (self.user, self.user), nodes])
        fp = open("/tmp/temp.%s" % self.user, "w")
        fp.write(passwd)
        fp.flush()
        fp.close()
        self.sshTool.scpFiles("/tmp/temp.%s" % self.user, "/tmp", nodes)
        self.logger.debug("Create os user cmd: %s" % cmd.replace(passwd, "******"))
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Create os user")
        else:
            self.sshTool.executeCommand(cmd, "Create os user", 0, nodes)
        self.logger.log("Exchange ssh keys...")
        self.sshTool.exchangeHostnameSshKeys(self.user, passwd)
        backIps = self.clusterInfo.getClusterBackIps()
        for ips in backIps:
            self.sshTool.exchangeIpSshKeys(self.user, passwd, ips)
    def installApp(self, nodes = None):
        self.logger.log("Begin to install application on all nodes...")
        
        cmd = "python %s -U %s:%s -X %s -R %s -c %s -l %s" % (OMCommand.Local_Install, self.user, self.group, self.xmlFile, self.clusterInfo.appPath, self.clusterInfo.name, self.localLog)
        self.rollbackCommands.insert(0, ["python %s -U %s -R %s -l %s -c" % (OMCommand.Local_Uninstall, self.user, self.clusterInfo.appPath, self.localLog), nodes])
        self.logger.debug("Install application cmd: %s" % cmd)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Install application on all nodes")
        else:
            self.sshTool.executeCommand(cmd, "Install application on all nodes", 0, nodes)

    def installCgroup(self, nodes = None):
        """
        Install libcgroup on each node
        """
        self.logger.log("Begin to install libcgroup on all nodes...")
        failedList = []
        cmd = "python %s -t install -U %s -R %s -l %s" % (OMCommand.Local_Cgroup, self.user, self.clusterInfo.appPath, self.localLog)
        self.rollbackCommands.insert(0, "python %s -t uninstall -U %s -R %s -l %s" % (OMCommand.Local_Cgroup, self.user, self.clusterInfo.appPath, self.localLog))
    
        self.logger.debug("Install libcgroup cmd: %s" % cmd)
    
        (status, output) = self.sshTool.getSshStatusOutput(cmd, nodes)
        outputMap = self.sshTool.parseSshOutput(nodes)
        for node in status.keys():
            if (status[node] != 0):
                failedList.append(node)
            elif (status[node] == 0 and outputMap[node].find("Install libcgroup failed") >= 0):
                failedList.append(node)
        if (failedList != []):
            self.logger.log("Libcgroup on %s install failed!" % failedList)
    
    def cleanNodeConfig(self, nodes = None, datadirs = []):
        """
        Clean instance
        """
        self.logger.log("Clean instance on all nodes...")
        cmdParam = ""
        for datadir in datadirs:
            cmdParam += " -D %s " % datadir
        cmd = "python %s -U %s %s -l %s" % (OMCommand.Local_Clean_Instance, self.user, cmdParam, self.localLog)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Clean instance")
        else:
            self.sshTool.executeCommand(cmd, "Clean instance", 0, nodes)
            
    def checkNodeConfig(self, nodes = None, cooGucParam = [], dataGucParam= []):
        """
        Check node config on all nodes
        """
        self.logger.log("Check node config on all nodes...")
        cmdParam = ""
        for param in cooGucParam:
            cmdParam += " -C \\\"%s\\\"" % param
        for param in dataGucParam:
            cmdParam += " -D \\\"%s\\\"" % param
        cmd = "python %s -U %s -l %s %s" % (OMCommand.Local_Check_Config, self.user, self.localLog, cmdParam)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Check node config")
        else:
            self.sshTool.executeCommand(cmd, "Check node config", 0, nodes)
            
    def initNodeInstance(self, nodes = None, dbInitParam = [], gtmInitParam = []):
        """
        Init instances on all nodes
        """
        self.logger.log("Init instances on all nodes...")
        cmdParam = ""
        for param in dbInitParam:
            cmdParam += " -P \\\"%s\\\"" % param
        for param in gtmInitParam:
            cmdParam += " -G \\\"%s\\\"" % param
        cmd = "python %s -U %s %s -l %s" % (OMCommand.Local_Init_Instance, self.user, cmdParam, self.localLog)
        self.rollbackCommands.insert(0, [self.cleanNodeConfig, (nodes,)])
        if(nodes is None):
            self.sshTool.executeCommand(cmd, "Init instances")
        else:
            self.sshTool.executeCommand(cmd, "Init instances", 0, nodes)
    def configNodeInstance(self, nodes = None, cooGucParam = [], dataGucParam = [], alarm_component = ""):
        """
        Update instances config on all nodes
        """
        self.logger.log("Update instances config on all nodes...")
        cmdParam = ""
        for param in cooGucParam:
            cmdParam += " -C \\\"%s\\\"" % param
        for param in dataGucParam:
            cmdParam += " -D \\\"%s\\\"" % param
        if(alarm_component != ""):
            cmdParam += " --alarm=%s" % alarm_component
        cmd = "python %s -U %s %s -l %s" % (OMCommand.Local_Config_Instance, self.user, cmdParam, self.localLog)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Update instances config")
        else:
            self.sshTool.executeCommand(cmd, "Update instances config", 0, nodes)
    def configHba(self, nodes = None):
        """
        """
        self.logger.log("config pg_hba on all nodes...")
        cmd = "python %s -U %s -l %s" % (OMCommand.Local_Config_Hba, self.user, self.localLog)
        if (nodes is None):
            self.sshTool.executeCommand(cmd, "Update instances config")
        else:
            self.sshTool.executeCommand(cmd, "Update instances config", 0, nodes)
    def excuteRollbackCommands(self):
        """
        support two kinds of rollback command:
        1. rollback commands + excute nodes
        2. rollback function + function args
        """
        self.logger.log("Rollback...")
        for rollback in self.rollbackCommands:
            if (callable(rollback[0])): 
                func, args = rollback[0], rollback[1]
                try:
                    if (hasattr(func, "func_name")):
                        func_name = func.func_name
                    elif (hasattr(func, "im_func") and hasattr(func.im_func, "func_name")):
                        func_name = func.im_func.func_name
                    else:
                        func_name = str(func)
                except:
                    func_name = str(func)           
                try:
                    self.logger.log("Rollback function: %s" % func_name)
                    result = func(*args)
                    self.logger.debug("Rollback function: %s. result: %s" % (func_name, str(result) if result is not None else "finished!"))
                except Exception as e:
                    self.logger.log("[Warning]:Execute function[%s] failed!Error: %s." % (func_name, str(e)))
            else:   
                cmd, nodes = rollback[0], rollback[1]         
                self.logger.log("Rollback cmd: %s" % cmd)
                if (nodes is None):
                    (status, output) = self.sshTool.getSshStatusOutput(cmd)
                else:
                    (status, output) = self.sshTool.getSshStatusOutput(cmd, nodes)
                for ret in status.values():
                    if (ret != 0):
                        self.logger.log("[Warning]:Execute command[%s] failed!Result: %s." % (cmd, status))
                        break
                self.logger.debug(output)
        self.logger.log("Rollback finished!") 
        
    def lockClusterInternal(self, dbNodes = [], lockTime = 120, setDaemon = True):
        extra = ""
        if (setDaemon):
            extra = "--set-daemon"
        cmd = "python %s --lock-cluster %s --lock-time=%d -U %s" % (OMCommand.Local_Query, extra, lockTime, self.user)
        self.logger.debug("lock cluster command: %s" % cmd)
        lock_success = False
        curNode = ""
        self.logger.debug("all node names: %s" % ",".join([dbNode.name for dbNode in dbNodes]))
        for dbNode in dbNodes:
            if (len(dbNode.coordinators) > 0):
                curNode = dbNode.name
                try:
                    self.setTimer(lockTime)
                    self.sshTool.executeCommand(cmd, "lock cluster", 0, [curNode])
                    self.logger.debug("lock cluster succeed returned.")
                    lock_success = True
                    break
                except Exception as lockError: 
                    try:
                        self.unlockClusterInternal([curNode], lockTime, False)
                    except Timeout as to:
                        self.logger.debug("catch timeout error[%s] when unlocking cluster. ignore it." % str(to)) 
                    except Exception as unlockError:
                        ### when we try to lock cluster on one node failed. we try to unlock on that node.
                        ### but if the unlock operation failed too. it seems have no bad side-effect.
                        ### so it's better to be silent and try to lock cluster on next node.
                        self.logger.debug("try to unlock cluster on node[%s] failed. Error:%s" % (curNode, str(unlockError)))  
                    finally:
                        self.logger.debug("lock cluster on node[%s] failed. Error:%s" % (curNode, str(lockError)))
        if (not lock_success):
            self.logger.debug("lock cluster failed on all nodes.")
            return None
        else:
            self.logger.debug("lock cluster success on node[%s]." % curNode)
            return curNode
            
    def unlockClusterInternal(self, dbNodeNames = [], lockTime = 120, ignoreError = True):
        try:
            self.resetTimer()
            cmd = "python %s --release-cluster --lock-time=%d -U %s" % (OMCommand.Local_Query, lockTime, self.user)
            self.logger.debug("unlock cluster command: %s" % cmd)
            self.sshTool.executeCommand(cmd, "unlock cluster", 0, dbNodeNames)
        except Exception as e:
            if (not ignoreError):
                raise e
            else:
                self.logger.debug("unlock cluster failed. Error: %s" % str(e))
                
class CommandThread(threading.Thread):
    """
    The class is used to execute command in thread
    """
    def __init__(self, cmd):
        threading.Thread.__init__(self)
        self.command = cmd
        self.cmdStauts = 0
        self.cmdOutput = ""
        
    def run(self):
        """
        Run command
        """
        (self.cmdStauts, self.cmdOutput) = commands.getstatusoutput(self.command)
        
class CommandProcess(Process):
    """
    The class is used to execute command in process
    """
    def __init__(self, cmd, logger):
        Process.__init__(self)
        self.command = cmd
        self.logger = logger
        self.ppid = os.getpid()
        self.logger.debug("Command Process parent: %d" % self.ppid)
      
    ###parent do not need to wait child.  
    def join(self, timeout = None):
        pass

    def run(self):
        try:
            ##forbid standard output
            sys.stdout = None
            sys.stderr = None
            try:
                import signal
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            except Exception as e:
                self.logger.debug("Sub command process block signal failed. Error: %s" % str(e))
            self.logger.debug("Sub command process started. Command: %s" % self.command)
            try:
                status, output = commands.getstatusoutput(self.command)
                self.logger.debug("Command Process parent: %d, command status: %d, output:\n%s" % (self.ppid, status, output))
            except Exception as e:
                self.logger.debug("Execute command failed. Error: %s" % str(e))
        except:
            ###ignore logger error
            pass