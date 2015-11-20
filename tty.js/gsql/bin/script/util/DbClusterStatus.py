'''
Created on 2014-3-1

@author: 
'''
import os
import sys
import time

from script.util.Common import DefaultValue,PlatformCommand

from script.util.DbClusterInfo import *

g_clusterInfo = None 
g_instanceInfo = None
g_clusterInfoInitialized = False

class StatusReport():
    """
    classdocs
    """
    def __init__(self):
        """
        Constructor
        """
        self.nodeCount = 0
        self.cooNormal = 0
        self.cooAbnormal = 0
        self.gtmPrimary = 0
        self.gtmStandby = 0
        self.gtmAbnormal = 0
        self.gtmDown = 0
        self.dnPrimary = 0
        self.dnStandby = 0
        self.dnDummy = 0
        self.dnBuild = 0
        self.dnAbnormal = 0
        self.dnDown = 0

class DbInstanceStatus():
    """
    classdocs
    """
    def __init__(self, nodeId, instId=0):
        """
        Constructor
        """
        self.nodeId = nodeId
        self.instanceId = instId
        self.datadir = ""
        self.type = ""
        self.status = ""
        self.haStatus = ""
        self.connStatus = ""
        self.syncStatus = ""
        self.reason = ""
        
    def isInstanceHealthy(self):
        """
        Check if instance is healthy
        """
        if (self.type == DbClusterStatus.INSTANCE_TYPE_COORDINATOR):
            if (self.status != DbClusterStatus.INSTANCE_STATUS_NORMAL):
                return False

        if (self.type == DbClusterStatus.INSTANCE_TYPE_GTM):
            if (self.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                return True
            elif (self.status == DbClusterStatus.INSTANCE_STATUS_STANDBY):
                if (self.connStatus != DbClusterStatus.CONN_STATUS_NORMAL):
                    return False
            else:
                return False
        
        if (self.type == DbClusterStatus.INSTANCE_TYPE_DATANODE):
            if (self.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                return True
            elif (self.status == DbClusterStatus.INSTANCE_STATUS_DUMMY):
                return True
            elif (self.status == DbClusterStatus.INSTANCE_STATUS_STANDBY):
                if (self.haStatus != DbClusterStatus.HA_STATUS_NORMAL):
                    return False
            else:
                return False

        return True

class DbNodeStatus():
    """
    classdocs
    """
    def __init__(self, nodeId):
        """
        Constructor
        """
        self.id = nodeId
        self.name = ""
        self.version = ""
        self.coordinators = []
        self.gtms = []
        self.datanodes = []
    
    def getInstanceByDir(self, datadir):
        """
        Get instance by its datadir
        """
        instances = self.coordinators + self.gtms + self.datanodes
        
        for inst in instances:
            if (inst.datadir == datadir):
                return inst
        
        return None
    
    def isNodeHealthy(self):
        """
        Check if node is healthy
        """
        instances = self.coordinators + self.gtms + self.datanodes
        
        for inst in instances:
            if (not inst.isInstanceHealthy()):
                return False
        
        return True
    
    def getNodeStatusReport(self):
        """
        Get the status report of node
        """
        report = StatusReport()
        for inst in self.coordinators:
            if (inst.status == DbClusterStatus.INSTANCE_STATUS_NORMAL):
                report.cooNormal += 1
            else:
                report.cooAbnormal += 1
        
        for inst in self.gtms:
            if (inst.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                report.gtmPrimary += 1
            elif (inst.status == DbClusterStatus.INSTANCE_STATUS_STANDBY):
                if (inst.connStatus == DbClusterStatus.CONN_STATUS_NORMAL):
                    report.gtmStandby += 1
                else:
                    report.gtmAbnormal += 1
            elif (inst.status == DbClusterStatus.INSTANCE_STATUS_DOWN):
                report.gtmDown += 1
            else:
                report.gtmAbnormal += 1
            
        for inst in self.datanodes:
            if (inst.status == DbClusterStatus.INSTANCE_STATUS_PRIMARY):
                report.dnPrimary += 1
            elif (inst.status == DbClusterStatus.INSTANCE_STATUS_STANDBY):
                if (inst.haStatus == DbClusterStatus.HA_STATUS_NORMAL):
                    report.dnStandby += 1
                elif (inst.haStatus == DbClusterStatus.HA_STATUS_BUILD):
                    report.dnBuild += 1
                else:
                    report.dnAbnormal += 1
            elif (inst.status == DbClusterStatus.INSTANCE_STATUS_DOWN):
                report.dnDown += 1
            elif (inst.status == DbClusterStatus.INSTANCE_STATUS_DUMMY):
                report.dnDummy += 1
            else:
                report.dnAbnormal += 1
        
        return report
    
    def outputNodeStatus(self, stdout, user, showDetail = False):
        """
        output the status of node
        """
        global g_clusterInfo
        global g_instanceInfo
        global g_clusterInfoInitialized
        if(g_clusterInfoInitialized == False):
            PlatformCommand.checkUser(user)
            g_clusterInfo = dbClusterInfo()
            g_clusterInfo.initFromStaticConfig(user)
            g_clusterInfoInitialized = True
        dbNode = g_clusterInfo.getDbNodeByName(self.name)
        instName = ""
        
        print >>stdout, "%-20s: %d" % ("node", self.id)
        print >>stdout, "%-20s: %s" % ("node_name", self.name)
        if (self.isNodeHealthy()):
            print >>stdout, "%-20s: %s\n" % ("node_state", DbClusterStatus.OM_NODE_STATUS_NORMAL)
        else:
            print >>stdout, "%-20s: %s\n" % ("node_state", DbClusterStatus.OM_NODE_STATUS_ABNORMAL)
            
        if (not showDetail):
            return
        
        # coordinator status
        for inst in self.coordinators:
            #get the instance info
            g_instanceInfo = None
            for instInfo in dbNode.coordinators:
                if instInfo.instanceId == inst.instanceId:
                    g_instanceInfo = instInfo
                    break
            if(g_instanceInfo == None):
                raise Exception("get coordinator instance info failed!")
            #construct the instance name
            instName = "cn_%s" % g_instanceInfo.instanceId
                    
            print >>stdout, "Coordinator"
            print >>stdout, "%-20s: %d" % ("    node", inst.nodeId)
            print >>stdout, "%-20s: %s" % ("    instance_name", instName)
            print >>stdout, "%-20s: %s" % ("    listen_IP", g_instanceInfo.listenIps)
            print >>stdout, "%-20s: %d" % ("    port", g_instanceInfo.port)
            print >>stdout, "%-20s: %s" % ("    data_path", inst.datadir)
            print >>stdout, "%-20s: %s" % ("    instance_state", inst.status)
            print >>stdout, ""
            
            
        for inst in self.gtms:
            #get the instance info
            g_instanceInfo = None
            for instInfo in dbNode.gtms:
                if instInfo.instanceId == inst.instanceId:
                    g_instanceInfo = instInfo
                    break
            if(g_instanceInfo == None):
                raise Exception("get gtm instance info failed!")              
            #construct the instance name
            instName = "gtm_%s" % g_instanceInfo.instanceId
            print >>stdout, "GTM"
            print >>stdout, "%-20s: %d" % ("    node", inst.nodeId)
            print >>stdout, "%-20s: %s" % ("    instance_name", instName)
            print >>stdout, "%-20s: %s" % ("    listen_IP", g_instanceInfo.listenIps)
            print >>stdout, "%-20s: %d" % ("    port", g_instanceInfo.port)
            print >>stdout, "%-20s: %s" % ("    data_path", inst.datadir)
            print >>stdout, "%-20s: %s" % ("    instance_state", inst.status)
            print >>stdout, "%-20s: %s" % ("    conn_state", inst.connStatus)
            print >>stdout, "%-20s: %s" % ("    reason", inst.reason)
            print >>stdout, ""
            
        i = 1
        for inst in self.datanodes:
            #get the instance info
            g_instanceInfo = None
            for instInfo in dbNode.datanodes:
                if instInfo.instanceId == inst.instanceId:
                    g_instanceInfo = instInfo
                    break
            if(g_instanceInfo == None):
                raise Exception("get datanode instance info failed!")
            #construct the instance name
            peerInsts = g_clusterInfo.getPeerInstance(g_instanceInfo)
            if (len(peerInsts) != 2 and len(peerInsts) != 1):
                raise Exception("Get peer instance for status query failed!")
            masterInst = None
            standbyInst = None
                
            if (g_instanceInfo.instanceType == MASTER_INSTANCE):
                masterInst = g_instanceInfo
                for instIndex in range(len(peerInsts)):
                    if(peerInsts[instIndex].instanceType == STANDBY_INSTANCE):
                        standbyInst = peerInsts[instIndex]
            else:
                standbyInst = g_instanceInfo
                for instIndex in range(len(peerInsts)):
                    if(peerInsts[instIndex].instanceType == MASTER_INSTANCE):
                        masterInst = peerInsts[instIndex]

            instName = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            
            print >>stdout, "Datanode%d" % i
            print >>stdout, "%-20s: %d" % ("    node", inst.nodeId)
            print >>stdout, "%-20s: %s" % ("    instance_name", instName)
            print >>stdout, "%-20s: %s" % ("    listen_IP", g_instanceInfo.listenIps)
            print >>stdout, "%-20s: %s" % ("    HA_IP", g_instanceInfo.haIps)
            print >>stdout, "%-20s: %d" % ("    port", g_instanceInfo.port)
            print >>stdout, "%-20s: %s" % ("    data_path", inst.datadir)
            print >>stdout, "%-20s: %s" % ("    instance_state", inst.status)
            print >>stdout, "%-20s: %s" % ("    HA_state", inst.haStatus)
            print >>stdout, "%-20s: %s" % ("    reason", inst.reason)
            print >>stdout, ""
            
            i += 1

class DbClusterStatus():
    """
    classdocs
    """
    OM_STATUS_FILE = "gs_om_status.dat"
    OM_STATUS_KEEPTIME = 1800
    
    OM_STATUS_NORMAL = "Normal"
    OM_STATUS_ABNORMAL = "Abnormal"
    OM_STATUS_STARTING = "Starting"
    OM_STATUS_UPGRADE = "Upgrade"
    OM_STATUS_DILATATION = "Dilatation"
    OM_STATUS_REPLACE = "Replace"
    OM_STATUS_REDISTIRBUTE = "Redistributing"
    
    OM_NODE_STATUS_NORMAL = "Normal"
    OM_NODE_STATUS_ABNORMAL = "Abnormal"
    
    CLUSTER_STATUS_NORMAL = "Normal"
    CLUSTER_STATUS_STARTING = "Starting"
    CLUSTER_STATUS_ABNORMAL = "Abnormal"
    CLUSTER_STATUS_PENDING = "Pending"
    CLUSTER_STATUS_MAP = {
                          "Normal":"Normal",
                          "Redistributing":"Redistributing",
                          "Repair":"Abnormal",
                          "Starting":"Starting",
                          "Degraded":"Degraded",
                          "Unknown":"Abnormal"
                          }
    
    INSTANCE_TYPE_GTM = "GTM"
    INSTANCE_TYPE_DATANODE = "Datanode"
    INSTANCE_TYPE_COORDINATOR = "Coordinator"
    
    INSTANCE_STATUS_NORMAL = "Normal"
    INSTANCE_STATUS_PRIMARY = "Primary"
    INSTANCE_STATUS_STANDBY = "Standby"
    INSTANCE_STATUS_ABNORMAL = "Abnormal"
    INSTANCE_STATUS_DOWN = "Down"
    INSTANCE_STATUS_DUMMY = "Secondary"
    INSTANCE_STATUS_MAP = {
                           "Normal":"Abnormal",  # When instance run stand-alone,it's 'Normal' 
                           "Unnormal":"Abnormal",
                           "Primary":"Primary",
                           "Standby":"Standby",
                           "Secondary":"Secondary",
                           "Pending":"Abnormal",
                           "Down":"Down",
                           "Unknown":"Abnormal"
                           }
    
    HA_STATUS_NORMAL = "Normal"
    HA_STATUS_BUILD = "Building"
    HA_STATUS_ABNORMAL = "Abnormal"
    HA_STATUS_MAP = {
                     "Normal":"Normal",
                     "Building":"Building",
                     "Need repair":"Abnormal",
                     "Starting":"Starting",
                     "Demoting":"Demoting",
                     "Promoting":"Promoting",
                     "Waiting":"Abnormal",
                     "Unknown":"Abnormal"
                     }
    
    CONN_STATUS_NORMAL = "Normal"
    CONN_STATUS_ABNORMAL = "Abnormal"
    CONN_STATUS_MAP = {
                       "Connection ok":"Normal",
                       "Connection bad":"Abnormal",
                       "Connection started":"Abnormal",
                       "Connection made":"Abnormal",
                       "Connection awaiting response":"Abnormal",
                       "Connection authentication ok":"Abnormal",
                       "Connection prepare SSL":"Abnormal",
                       "Connection needed":"Abnormal",
                       "Unknown":"Abnormal"
                       }
    
    DATA_STATUS_SYNC = "Sync"
    DATA_STATUS_ASYNC = "Async"
    DATA_STATUS_Unknown = "Unknown"
    DATA_STATUS_MAP = {
                       "Async":"Async",
                       "Sync":"Sync",
                       "Most available":"Standby Down",
                       "Potential":"Potential",
                       "Unknown":"Unknown"
                      }
    
    def __init__(self):
        """
        Constructor
        """
        self.dbNodes = []
        self.clusterStatus = ""
        self.clusterStatusDetail = ""
        self.__curNode = None
        self.__curInstance = None

    @staticmethod
    def saveOmStatus(status, sshTool, user):
        """
        Save om status to a file
        """
        if (sshTool is None):
            raise Exception("The ssh tool is None, can't save status to all nodes!")
        
        try:
            statFile = os.path.join(DefaultValue.getTmpDirFromEnv(), DbClusterStatus.OM_STATUS_FILE)
            cmd = "echo \"%s\" > %s" % (status, statFile)
            sshTool.executeCommand(cmd, "Record om status info")
        except Exception, e:
            raise Exception("Record om status info failed!Output:%s" % str(e))

    @staticmethod
    def getOmStatus(user):
        """
        Get om status from file
        """ 
        statFile = os.path.join(DefaultValue.getTmpDirFromEnv(), DbClusterStatus.OM_STATUS_FILE)
        if (not os.path.isfile(statFile)):
            return DbClusterStatus.OM_STATUS_NORMAL
        
        status = DbClusterStatus.OM_STATUS_NORMAL
        try:
            modifiedTime = os.stat(statFile).st_mtime
            deltaTime = time.time() - modifiedTime
            if deltaTime <= DbClusterStatus.OM_STATUS_KEEPTIME:
                status = PlatformCommand.readFileLine(statFile)
        except Exception:
            pass
        
        return status
    
    def getDbNodeStatusById(self, nodeId):
        """
        Get node status by node id
        """
        for dbNode in self.dbNodes:
            if (dbNode.id == nodeId):
                return dbNode

        return None
    
    def getDbNodeStatusByName(self, nodeName):
        """
        Get node status by node
        """
        for dbNode in self.dbNodes:
            if (dbNode.name == nodeName):
                return dbNode

        return None
    
    def getInstanceStatusById(self, instId):
        """
        Get instance by its id
        """
        for dbNode in self.dbNodes:
            instances = dbNode.coordinators + dbNode.gtms + dbNode.datanodes
            for dbInst in instances:
                if (dbInst.instanceId == instId):
                    return dbInst
        
        return None
        
    def isAllHealthy(self, cluster_normal_status = None):
        """
        Check if cluster is healthy
        """
        if (cluster_normal_status is None):
            cluster_normal_status = [DbClusterStatus.CLUSTER_STATUS_NORMAL]
            
        if (self.clusterStatus not in cluster_normal_status):
            return False
        
        for dbNode in self.dbNodes:
            if (not dbNode.isNodeHealthy()):
                return False
        
        return True
    
    def getClusterStatusReport(self):
        """
        Get the health report of cluster
        """
        clusterRep = StatusReport()
        for dbNode in self.dbNodes:
            nodeRep = dbNode.getNodeStatusReport()
            clusterRep.nodeCount += 1
            clusterRep.cooNormal += nodeRep.cooNormal
            clusterRep.cooAbnormal += nodeRep.cooAbnormal
            clusterRep.gtmPrimary += nodeRep.gtmPrimary
            clusterRep.gtmStandby += nodeRep.gtmStandby
            clusterRep.gtmAbnormal += nodeRep.gtmAbnormal
            clusterRep.gtmDown += nodeRep.gtmDown
            clusterRep.dnPrimary += nodeRep.dnPrimary
            clusterRep.dnStandby += nodeRep.dnStandby
            clusterRep.dnDummy += nodeRep.dnDummy
            clusterRep.dnBuild += nodeRep.dnBuild
            clusterRep.dnAbnormal += nodeRep.dnAbnormal
            clusterRep.dnDown += nodeRep.dnDown
        
        return clusterRep
    
    def outputClusterStauts(self, stdout, user, showDetail = False):
        """
        output the status of cluster
        """
        clusterStat = DbClusterStatus.getOmStatus(user)
        if (clusterStat == DbClusterStatus.OM_STATUS_NORMAL):
            clusterStat = self.clusterStatus
        print >>stdout, "%-20s: %s" % ("cluster_state", clusterStat)
        print >>stdout, ""
        
        for dbNode in self.dbNodes:
            dbNode.outputNodeStatus(stdout, user, showDetail)
    
    def initFromFile(self, filePath):
        """
        Init from status file
        """
        if (not os.path.isfile(filePath)):
            raise Exception("Status file does not exist!Path: %s" % filePath)
        
        fp = None
        try:
            fp = open(filePath, "r")
            for line in fp.readlines():
                line = line.strip()
                if (line == ""):
                    continue
                
                strList = line.split(":")
                if (len(strList) != 2):
                    continue

                self.__fillField(strList[0].strip(), strList[1].strip())
            fp.close()
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Read status file failed! error: %s" % str(e))
            
    def __fillField(self, field, value):
        
        if (field == "cluster_state"):
            status = DbClusterStatus.CLUSTER_STATUS_MAP.get(value)
            self.clusterStatus = DbClusterStatus.CLUSTER_STATUS_ABNORMAL if status is None else status
            self.clusterStatusDetail = value
        elif (field == "node"):
            if (not value.isdigit()):
                raise Exception("Node id should be digit.")
            newId = int(value)
            if (self.__curNode is None or self.__curNode.id != newId):
                self.__curNode = DbNodeStatus(newId)
                self.dbNodes.append(self.__curNode)
        elif (field == "node_name"):
            self.__curNode.name = value
        elif (field == "instance_id"):
            if (not value.isdigit()):
                raise Exception("Instance id should be digit.")
            self.__curInstance = DbInstanceStatus(self.__curNode.id, int(value))
        elif (field == "data_path"):
            self.__curInstance.datadir = value
        elif (field == "type"):
            self.__curInstance.type = value
            if (value == DbClusterStatus.INSTANCE_TYPE_GTM):
                self.__curNode.gtms.append(self.__curInstance)
            elif (value == DbClusterStatus.INSTANCE_TYPE_DATANODE):
                self.__curNode.datanodes.append(self.__curInstance)
            elif (value == DbClusterStatus.INSTANCE_TYPE_COORDINATOR):
                self.__curNode.coordinators.append(self.__curInstance)
        elif (field == "instance_state"):
            status = DbClusterStatus.INSTANCE_STATUS_MAP.get(value)
            self.__curInstance.status = DbClusterStatus.INSTANCE_STATUS_ABNORMAL if status is None else status
        elif (field == "state"):
            if (value == DbClusterStatus.INSTANCE_STATUS_NORMAL):
                self.__curInstance.status = DbClusterStatus.INSTANCE_STATUS_NORMAL
            else:
                self.__curInstance.status = DbClusterStatus.INSTANCE_STATUS_ABNORMAL
        elif (field == "HA_state"):
            haStatus = DbClusterStatus.HA_STATUS_MAP.get(value)
            self.__curInstance.haStatus = DbClusterStatus.HA_STATUS_ABNORMAL if haStatus is None else haStatus
        elif (field == "con_state"):
            connStatus = DbClusterStatus.CONN_STATUS_MAP.get(value)
            self.__curInstance.connStatus = DbClusterStatus.CONN_STATUS_ABNORMAL if connStatus is None else connStatus
        elif (field == "static_connections"):
            connStatus = DbClusterStatus.CONN_STATUS_MAP.get(value)
            self.__curInstance.connStatus = DbClusterStatus.CONN_STATUS_ABNORMAL if connStatus is None else connStatus
        elif (field == "sync_state"):
            dataStatus = DbClusterStatus.DATA_STATUS_MAP.get(value)
            self.__curInstance.syncStatus = DbClusterStatus.DATA_STATUS_Unknown if dataStatus is None else dataStatus
        elif (field == "reason"):
            self.__curInstance.reason = value
