#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 2014-1-27

@author: 
'''

import binascii
import os
import commands
import struct
import time
import types
import sys
import xml.dom.minidom

INSTANCE_ROLE_UNDEFINED = -1
INSTANCE_ROLE_CMSERVER = 0
INSTANCE_ROLE_GTM = 1
INSTANCE_ROLE_GTMPROXY = 2
INSTANCE_ROLE_COODINATOR = 3
INSTANCE_ROLE_DATANODE = 4
INSTANCE_ROLE_CMAGENT = 5

BASE_ID_CMSERVER = 1
BASE_ID_GTM = 1001
BASE_ID_GTMPROXY = 2001
BASE_ID_DUMMYDATANODE = 3001
BASE_ID_COORDINATOR = 5001
BASE_ID_DATANODE = 6001
BASE_ID_CMAGENT = 10001

MASTER_BASEPORT_CMS = 5000
MASTER_BASEPORT_GTM = 6000
MASTER_BASEPORT_GTMPROXY = 7000
MASTER_BASEPORT_COO = 8000
MASTER_BASEPORT_DATA = 40000

STANDBY_BASEPORT_CMS = 5500
STANDBY_BASEPORT_GTM = 6500
STANDBY_BASEPORT_GTMPROXY = 7500
STANDBY_BASEPORT_COO = 8500
STANDBY_BASEPORT_DATA = 45000
DUMMY_STANDBY_BASEPORT_DATA = 50000

SCTP_BEGIN_PORT = 1024
SCTP_END_PORT = 65535

INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2

MIRROR_COUNT_CMS = 2
MIRROR_COUNT_GTM = 2
MIRROR_COUNT_DATA = 3

MIRROR_ID_COO = -1
MIRROR_ID_GTMPROXY = -2
MIRROR_ID_AGENT = -3

ENV_CLUSTERCONFIG = "CLUSTERCONFIGFILE"

BIN_CONFIG_VERSION = 1
PAGE_SIZE = 8192
MAX_IP_NUM = 3

def InstanceIgnore_haPort(Object):
    if (Object.instanceRole == INSTANCE_ROLE_COODINATOR or Object.instanceRole == INSTANCE_ROLE_CMAGENT):
        return True
    else:
        return False
def InstanceIgnore_isMaster(Object):
    if (Object.instanceRole != INSTANCE_ROLE_GTM and Object.instanceRole != INSTANCE_ROLE_DATANODE):
        return True
    else:
        return False
def ignoreCheck(Object, member):
    INSTANCEINFO_IGNORE_TABLE = {}
    DBNODEINFO_IGNORE_TABLE = {
                               "masterBasePorts": None, 
                               "standbyBasePorts": None, 
                               "dummyStandbyBasePort": None,
                               "cmsNum": None, 
                               "cooNum": None, 
                               "dataNum": None, 
                               "gtmNum": None
                               }
    DBCLUSTERINFO_IGNORE_TABLE = {
                                  "xmlFile": None, 
                                  "newNodes": None
                                  }
    if (isinstance(Object, instanceInfo)):
        if (member not in INSTANCEINFO_IGNORE_TABLE.keys()):
            return False
        elif (INSTANCEINFO_IGNORE_TABLE[member] is None or not callable(INSTANCEINFO_IGNORE_TABLE[member])):
            return True
        else:
            return INSTANCEINFO_IGNORE_TABLE[member](Object)            
    elif (isinstance(Object, dbNodeInfo)):
        if (member not in DBNODEINFO_IGNORE_TABLE.keys()):
            return False
        elif (DBNODEINFO_IGNORE_TABLE[member] is None or not callable(DBNODEINFO_IGNORE_TABLE[member])):
            return True
        else:
            return INSTANCEINFO_IGNORE_TABLE[member](Object)      
    elif (isinstance(Object, dbClusterInfo)):
        if (member not in DBCLUSTERINFO_IGNORE_TABLE.keys()):
            return False
        elif (DBCLUSTERINFO_IGNORE_TABLE[member] is None or not callable(DBCLUSTERINFO_IGNORE_TABLE[member])):
            return True
        else:
            return DBCLUSTERINFO_IGNORE_TABLE[member](Object)      
    else:
        return False
def compareObject(Object_A, Object_B, instName, buffer = ""):    
    ### not the same type
    if (type(Object_A) != type(Object_B)):
        buffer += "\nThe Instance type of %s are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
        return False, buffer
    
    ### string, int, long, float, bool type
    if (isinstance(Object_A, types.StringType)):
        if (Object_A != Object_B):
            buffer += "\nInstacne type: string. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, Object_A, Object_B)
            return False, buffer
    elif(isinstance(Object_A, types.IntType) or isinstance(Object_A, types.LongType)
        or isinstance(Object_A, types.FloatType) or isinstance(Object_A, types.BooleanType)):
        if (Object_A != Object_B):
            buffer += "\nInstacne type: number. Instance[%s] are not the same.\nA: %d\nB: %d\n" % (instName, Object_A, Object_B)
            return False, buffer               
    ### list type
    elif (isinstance(Object_A, types.ListType)):
        if (len(Object_A) != len(Object_B)):
            buffer += "\nInstacne type: list. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
            return False, buffer
        Object_A.sort()
        Object_B.sort()        
        for idx in range(len(Object_A)):
            result, buffer = compareObject(Object_A[idx], Object_B[idx], "%s[%d]" % (instName, idx), buffer)
            if (not result):
                buffer = buffer + "\nInstacne type: list. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
                return False, buffer
    ### function type 
    elif (isinstance(Object_A, types.UnboundMethodType) or isinstance(Object_A, types.FunctionType)):
        return True, buffer    
    elif (isinstance(Object_A, types.InstanceType)):
        Object_A_list = dir(Object_A)
        Object_B_list = dir(Object_B)
        if (len(Object_A_list) != len(Object_B_list)):
            buffer += "\nInstacne type: instance. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
            return False, buffer
        for i in Object_A_list:
            if (i.startswith("_") or ignoreCheck(Object_A, i)):
                continue
            Inst_A = getattr(Object_A, i)
            try:
                Inst_B = getattr(Object_B, i)
            except:
                buffer += "\nInstacne type: instance. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
                return False, buffer
            result, buffer = compareObject(Inst_A, Inst_B, i, buffer)
            if(not result):
                buffer = buffer + "\nInstacne type: instance. Instance[%s] are not the same.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
                return False, buffer
    else:
        buffer = buffer + "\nUnrecognized instance type. Instance name: %s.\nA: %s\nB: %s\n" % (instName, str(Object_A), str(Object_B))
        return False, buffer
    return True, buffer


####################################################################
##readcluser functions
####################################################################

xmlRootNode = None

def initParserXMLFile(xmlFilePath):
    """
    """
    dom = xml.dom.minidom.parse(xmlFilePath)
    rootNode = dom.documentElement
    return rootNode

def readOneClusterConfigItem(rootNode, paraName, inputElementName, nodeName = ""):
    """
    """
    #if read node level config item, should input node name
    if(inputElementName.upper() == 'node'.upper() and nodeName == ""):
        raise Exception("need node name for node config level!")
        
    ElementName = inputElementName.upper()
    configPath = os.environ.get('CLUSTERCONFIGFILE')
    returnValue = ""
    returnStatus = 2

    if(ElementName == 'cluster'.upper()):
        element = rootNode.getElementsByTagName('CLUSTER')[0]
        nodeArray = element.getElementsByTagName('PARAM')
        (returnStatus, returnValue) = findParamInCluster(paraName, nodeArray)
    elif (ElementName == 'node'.upper()):
        ElementName = 'DEVICELIST'
        DeviceArray = rootNode.getElementsByTagName('DEVICELIST')[0]
        DeviceNode = DeviceArray.getElementsByTagName('DEVICE')
        (returnStatus, returnValue) = findParamByName(nodeName, paraName, DeviceNode)
    else:
        raise Exception("Param:%s does not exist!" % ElementName)

    return (returnStatus, returnValue)

def findParamInCluster(paraName, nodeArray):
    """
    """
    returnValue = ""
    returnStatus = 2
    for node in nodeArray:
        name=node.getAttribute('name')
        if(name == paraName):
            returnStatus = 0
            returnValue = str(node.getAttribute('value'))
    return (returnStatus, returnValue)
    

def findParamByName(nodeName, paraName, DeviceNode):
    """
    """
    returnValue = ""
    returnStatus = 2
    for dev in DeviceNode:
        paramList=dev.getElementsByTagName('PARAM')
        for param in paramList:
            thisname=param.getAttribute('name')
            if(thisname == 'name'):
                value=param.getAttribute('value')
                if(nodeName == value):
                    for param in paramList: 
                        name=param.getAttribute('name')
                        if(name == paraName):
                            returnStatus = 0
                            returnValue = str(param.getAttribute('value'))
    return (returnStatus, returnValue)


####################################################################
    

    
class instanceInfo():
    """
    Instance info
    """
    def __init__(self, instId=0, mirrorId=0):
        """
        Constructor
        """
        self.instanceId = instId
        self.mirrorId = mirrorId
        self.hostname = ""
        self.listenIps = []
        self.haIps = []
        self.port = 0
        self.haPort = 0 # It's pool port for coordinator, and ha port for other instance
        self.datadir = ""
        self.instanceType = INSTANCE_TYPE_UNDEFINED
        self.instanceRole = INSTANCE_ROLE_UNDEFINED
        self.level = 1
    def __cmp__(self, target):
        if (type(self) != type(target)):
            return 1
        if (not isinstance(target, instanceInfo)):
            return 1
        if (not hasattr(target, "instanceId")):
            return 1
        else:
            return self.instanceId - target.instanceId
    
    def __str__(self):
        """
        Construct a printable string representation of a instanceInfo
        """
        
        return "InstanceId=%d,MirrorId=%d,Host=%s,Port=%d,DataDir=%s,InstanceType=%s,Role=%d,ListenIps=%s,HaIps=%s" % (
            self.instanceId, self.mirrorId, self.hostname, self.port, self.datadir, self.instanceType, self.instanceRole, self.listenIps, self.haIps)

class dbNodeInfo():
    """
    Instance info on a node
    """        
    def __init__(self, nodeId = 0, name=""):
        """
        Constructor
        """
        self.id = nodeId
        self.name = name
        self.backIps = []
        self.sshIps = []
        self.cmsNum = 0
        self.cooNum = 0
        self.dataNum = 0
        self.gtmNum = 0
        self.gtmProxyNum = 0
        self.cmservers = []
        self.coordinators = []
        self.datanodes = []
        self.gtms = []
        self.gtmProxys = []
        self.cmagents = []
        self.cmDataDir = ""
        self.sctpBeginPort = 0
        self.sctpEndPort = 0
        self.dummyStandbyBasePort = 0
        self.masterBasePorts = [MASTER_BASEPORT_CMS, MASTER_BASEPORT_GTM, MASTER_BASEPORT_GTMPROXY, MASTER_BASEPORT_COO, MASTER_BASEPORT_DATA]
        self.standbyBasePorts = [STANDBY_BASEPORT_CMS, STANDBY_BASEPORT_GTM, STANDBY_BASEPORT_GTMPROXY, STANDBY_BASEPORT_COO, STANDBY_BASEPORT_DATA]

    def __cmp__(self, target):
        if (type(self) != type(target)):
            return 1
        if (not isinstance(target, dbNodeInfo)):
            return 1
        if (not hasattr(target, "id")):
            return 1
        else:
            return self.id - target.id
    
    def __str__(self):
        """
        Construct a printable string representation of a dbNodeInfo
        """
        retStr = "HostName=%s,backIps=%s" % (self.name, self.backIps)
        
        for cmsInst in self.cmservers:
            retStr += "\n%s" % str(cmsInst)
        
        for cmaInst in self.cmagents:
            retStr += "\n%s" % str(cmaInst)
        
        for gtmInst in self.gtms:
            retStr += "\n%s" % str(gtmInst)
        
        for pxyInst in self.gtmProxys:
            retStr += "\n%s" % str(pxyInst)
        
        for cooInst in self.coordinators:
            retStr += "\n%s" % str(cooInst)
        
        for dataInst in self.datanodes:
            retStr += "\n%s" % str(dataInst)
            
        return retStr
    
    def appendInstance(self, instId, mirrorId, instRole, instanceType, listenIps=None, haIps=None, datadir="", level=1):
        """
        create a new instance on node
        """
        if not self.__checkDataDir(datadir, instRole):
            raise Exception("Append instance failed on host[%s]!Data directory is conflicting!" % self.name)
        
        dbInst = instanceInfo(instId, mirrorId)
        dbInst.hostname = self.name
        dbInst.datadir = datadir
        dbInst.instanceType = instanceType
        dbInst.instanceRole = instRole
        if (listenIps is not None):
            if (len(listenIps) == 0):
                dbInst.listenIps = self.backIps[:]
            else:
                dbInst.listenIps = listenIps[:]
        
        if (haIps is not None):
            if (len(haIps) == 0):
                dbInst.haIps = self.backIps[:]
            else:
                dbInst.haIps = haIps[:]
        
        if (instRole == INSTANCE_ROLE_CMSERVER):
            dbInst.level = level
            dbInst.port = self.__assignNewInstancePort(self.cmservers, instRole, instanceType)
            dbInst.haPort = dbInst.port + 1
            dbInst.datadir = os.path.join(self.cmDataDir, "cm_server")
            self.cmservers.append(dbInst)
        elif (instRole == INSTANCE_ROLE_GTM):
            dbInst.port = self.__assignNewInstancePort(self.gtms, instRole, instanceType)
            dbInst.haPort = dbInst.port + 1
            self.gtms.append(dbInst)
        elif (instRole == INSTANCE_ROLE_GTMPROXY):
            dbInst.port = self.__assignNewInstancePort(self.gtmProxys, instRole, instanceType)
            dbInst.haPort = dbInst.port + 1
            self.gtmProxys.append(dbInst)
        elif (instRole == INSTANCE_ROLE_COODINATOR):
            dbInst.port = self.__assignNewInstancePort(self.coordinators, instRole, instanceType)
            dbInst.haPort = dbInst.port + 1
            self.coordinators.append(dbInst)
        elif (instRole == INSTANCE_ROLE_DATANODE):
            dbInst.port = self.__assignNewInstancePort(self.datanodes, instRole, instanceType)
            dbInst.haPort = dbInst.port + 1
            self.datanodes.append(dbInst)
        elif (instRole == INSTANCE_ROLE_CMAGENT):
            dbInst.datadir = os.path.join(self.cmDataDir, "cm_agent")
            self.cmagents.append(dbInst)

    def __checkDataDir(self, datadir, instRole):
        """
        Check if datadir is conflicting, return true if ok
        """
        if (datadir == ""):
            return (instRole == INSTANCE_ROLE_GTMPROXY or instRole == INSTANCE_ROLE_CMSERVER or instRole == INSTANCE_ROLE_CMAGENT)
        
        for cmsInst in self.cmservers:
            if (cmsInst.datadir == datadir):
                return False
        
        for cooInst in self.coordinators:
            if (cooInst.datadir == datadir):
                return False
        
        for dataInst in self.datanodes:
            if (dataInst.datadir == datadir):
                return False
        
        for gtmInst in self.gtms:
            if (gtmInst.datadir == datadir):
                return False
        
        return True
    
    def __assignNewInstancePort(self, instList, instRole, instanceType):
        """
        Assign a new port for instance
        """
        port = 0
        if instanceType == MASTER_INSTANCE:
            port = self.masterBasePorts[instRole]
        elif instanceType == STANDBY_INSTANCE:
            port = self.standbyBasePorts[instRole]
        #dn dummy standby instance
        elif instanceType == DUMMY_STANDBY_INSTANCE:
            port = self.dummyStandbyBasePort
        #cn and cm_agent instance
        elif instanceType == INSTANCE_TYPE_UNDEFINED:
            port = self.masterBasePorts[instRole]
            return port
        for inst in instList:
            if (inst.instanceType == instanceType):
                port += 2
    
        return port
        
class dbClusterInfo():
    """
    Cluster info
    """
    def __init__(self):
        """
        Constructor
        """
        self.name = ""
        self.appPath = ""
        self.logPath = ""
        self.xmlFile = ""
        self.dbNodes = []
        self.newNodes = []
        self.cmsFloatIp = ""
        self.__newInstanceId = [BASE_ID_CMSERVER, BASE_ID_GTM, BASE_ID_GTMPROXY, BASE_ID_COORDINATOR, BASE_ID_DATANODE, BASE_ID_CMAGENT]
        self.__newDummyStandbyId = BASE_ID_DUMMYDATANODE
        self.__newMirrorId = 0
    
    def __str__(self):
        """
        Construct a printable string representation of a dbClusterInfo
        """
        retStr = "ClusterName=%s,AppPath=%s,LogPath=%s" % (self.name, self.appPath, self.logPath)
        
        for dbNode in self.dbNodes:
            retStr += "\n%s" % str(dbNode)
        
        return retStr
    
    @staticmethod
    def setDefaultXmlFile(xmlFile):
        if not os.path.exists(xmlFile):
            raise Exception("xml config file does not exist!")
        
        os.putenv(ENV_CLUSTERCONFIG, xmlFile)
    
    @staticmethod
    def readClusterHosts(xmlFile=""):
        """
        Read cluster nodes from xmlFile
        """
        if (xmlFile != ""):
            dbClusterInfo.setDefaultXmlFile(xmlFile)
        
        
        (retStatus, retValue) = readOneClusterConfigItem(initParserXMLFile(xmlFile), "nodeNames", "cluster")
        if (retStatus != 0):
            raise Exception("Read node names failed! error: %s" % retValue)
        nodeNames = retValue.split(",")
        if (len(nodeNames) == 0):
            raise Exception("Read cluster config failed!There is no nodes in cluster config file!")
        
        return nodeNames
    
    @staticmethod
    def readClusterAppPath(xmlFile):
        """
        Read application path from xmlFile
        """
        dbClusterInfo.setDefaultXmlFile(xmlFile)
        
        (retStatus, retValue) = readOneClusterConfigItem(initParserXMLFile(xmlFile), "gaussdbAppPath", "cluster")
        if (retStatus != 0):
            raise Exception("Read application path failed! error: %s" % retValue)

        return os.path.normpath(retValue)

    @staticmethod
    def readClusterTmpMppdbPath(user, xmlFile):
        """
        Read tmp mppdb path from xmlFile
        """
        dbClusterInfo.setDefaultXmlFile(xmlFile)

        (retStatus, retValue) = readOneClusterConfigItem(initParserXMLFile(xmlFile), "tmpMppdbPath", "cluster")
        if (retStatus != 0):
            return "/tmp/%s_mppdb" % user
        return os.path.normpath(retValue)
    
    @staticmethod
    def readClusterLogPath(xmlFile):
        """
        Read log path from xmlFile
        """
        dbClusterInfo.setDefaultXmlFile(xmlFile)

        (retStatus, retValue) = readOneClusterConfigItem(initParserXMLFile(xmlFile), "gaussdbLogPath", "cluster")
        if(retStatus == 0):
            return os.path.normpath(retValue)
        elif(retStatus == 2):
            return "/var/log/gaussdb"
        else:
            raise Exception("Read log path failed! error: %s" % retValue)

    @staticmethod
    def getNodeIdByName(nodeName, xmlFile=""):
        """
        Get the node id by name
        """
        if (nodeName == ""):
            return 0
        
        nodeNames = dbClusterInfo.readClusterHosts(xmlFile)
        nodeId = 1
        for node in nodeNames:
            if (nodeName == node):
                return nodeId
            
            nodeId += 1
        
        return 0

    def initFromStaticConfig(self, user):
        """
        Init cluster from static config file
        """
        self.__checkOsUser(user)
        staticConfigFile = self.__getStaticConfigFilePath(user)
        self.__readStaticConfigFile(staticConfigFile, user)
        self.__checkClusterConfig()
        
    def __checkOsUser(self, user):
        """
        Check os user if exist
        """
        cmd = "id -gn '%s'" % user
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("User[%s] does not exist!Output: %s" % (user, output))

    def __getStaticConfigFilePath(self, user):
        """
        get static config file path
        """
        gaussHome = self.__getEnvironmentParameterValue("GAUSSHOME", user)
        if(gaussHome == ""):
            raise Exception("The install path of designated user (%s) does not exist!" % user)
        
        staticConfigFile = "%s/bin/cluster_static_config" % gaussHome
        if (not os.path.exists(staticConfigFile)):
            raise Exception("The static config file (%s) of designated user (%s) does not exist!" % (staticConfigFile, user)) 
        return staticConfigFile

    def __getEnvironmentParameterValue(self, environmentParameterName, user):
        """
        !!!!do not call this function in preinstall.py script.
        because we determine if we are using env separate version by the value of MPPDB_ENV_SEPARATE_PATH
        """
        mpprcFile = os.getenv('MPPDB_ENV_SEPARATE_PATH')
        if(mpprcFile != None and mpprcFile != ""):
            userProfile = mpprcFile
        else:
            userProfile = "~/.bashrc"
        if(os.getgid() == 0):
            cmd = "su - %s -c 'source %s;echo $%s' 2>/dev/null" % (user, userProfile, environmentParameterName)
        else:
            cmd = "source %s;echo $%s 2>/dev/null" % (userProfile, environmentParameterName)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("Get environment parameter value failed. Error:%s" % output)
        return output.split("\n")[0]    

    def __readStaticConfigFile(self, staticConfigFile, user):
        """
        read cluster info from static config file
        """
        fp = None
        try:
            self.name = self.__getEnvironmentParameterValue("GS_CLUSTER_NAME", user)
            self.appPath = self.__getEnvironmentParameterValue("GAUSSHOME", user)
            logPathWithUser = self.__getEnvironmentParameterValue("GAUSSLOG", user)
            if(self.name == ""):
                raise Exception("The cluster name of designated user (%s) does not exist!" % user)
            if(self.appPath == ""):
                raise Exception("The install path of designated user (%s) does not exist!" % user)
            if(logPathWithUser == ""):
                raise Exception("The log path of designated user (%s) does not exist!" % user)
                
            splitMark = "/%s" % user
            #set log path without user
            self.logPath = logPathWithUser.split(splitMark)[0]

            fp = open(staticConfigFile, "rb")
            info = fp.read(28)
            (crc, lenth, version, time, nodeNum, localNodeId) = struct.unpack("=iIIqiI", info)
            self.dbNodes = []
            for i in range(nodeNum):
                offset = (fp.tell() / PAGE_SIZE + 1) * PAGE_SIZE
                fp.seek(offset)
                dbNode = self.__unPackNodeInfo(fp)
                self.dbNodes.append(dbNode)
            fp.close()
        except Exception, e:
            if(fp):
                fp.close()
            raise Exception("Read cluster info from static config file failed! Error: %s" % str(e))
        
    def __unPackNodeInfo(self, fp):
        """
        unpack a node config info
        """
        info = fp.read(72)
        (crc, nodeId, nodeName) = struct.unpack("=iI64s", info)
        nodeName = nodeName.strip('\x00')
        dbNode = dbNodeInfo(nodeId, nodeName)
        self.__unPackIps(fp, dbNode.backIps)
        self.__unPackIps(fp, dbNode.sshIps)
        self.__unPackCmsInfo(fp, dbNode)
        self.__unpackAgentInfo(fp, dbNode)
        self.__unpackGtmInfo(fp, dbNode)
        self.__unpackGtmProxyInfo(fp, dbNode)
        self.__unpackCooInfo(fp, dbNode)
        self.__unpackDataNode(fp, dbNode)
        info = fp.read(8)
        (dbNode.sctpBeginPort, dbNode.sctpEndPort) = struct.unpack("II", info)
        return dbNode
        
    def __unPackIps(self, fp, ips):
        """
        unpack the info of ips
        """
        info = fp.read(4)
        (n,) = struct.unpack("=i", info)
        for i in range(int(n)):
            info = fp.read(128)
            (currentIp,) = struct.unpack("=128s", info)
            currentIp = currentIp.strip('\x00')
            ips.append(str(currentIp.strip()))
        info = fp.read(128 * (MAX_IP_NUM - n))
        
    def __unPackCmsInfo(self, fp, dbNode):
        """
        unpack the info of cm server
        """
        cmsInst = instanceInfo()
        cmsInst.instanceRole = INSTANCE_ROLE_CMSERVER
        cmsInst.hostname = dbNode.name
        info = fp.read(1164)
        (cmsInst.instanceId, cmsInst.mirrorId , dbNode.cmDataDir, cmsInst.level, self.cmsFloatIp) = struct.unpack("=II1024sI128s", info)
        dbNode.cmDataDir = dbNode.cmDataDir.strip('\x00')
        self.cmsFloatIp = self.cmsFloatIp.strip('\x00')
        cmsInst.datadir = "%s/cm_server" % dbNode.cmDataDir
        self.__unPackIps(fp, cmsInst.listenIps)
        info = fp.read(4)
        (cmsInst.port,) = struct.unpack("=I", info)
        self.__unPackIps(fp, cmsInst.haIps)
        info = fp.read(8)
        (cmsInst.haPort, cmsInst.instanceType) = struct.unpack("=II", info)
        if(cmsInst.instanceType == MASTER_INSTANCE):
            dbNode.cmsNum = 1
        elif(cmsInst.instanceType == STANDBY_INSTANCE):
            dbNode.cmsNum = 0
        else:
            raise Exception("Invalid cm server instance type: %d" % cmsInst.instanceType)
        info = fp.read(4 + 128 * MAX_IP_NUM + 4)
        
        if(cmsInst.instanceId):
            dbNode.cmservers.append(cmsInst)
        else:
            dbNode.cmservers = []
            
    def __unpackAgentInfo(self, fp, dbNode):
        """
        unpack the info of agent, it should be called after __unPackCmsInfo, because dbNode.cmDataDir
        got value in __unPackCmsInfo
        """
        cmaInst = instanceInfo()
        cmaInst.instanceRole = INSTANCE_ROLE_CMAGENT
        cmaInst.hostname = dbNode.name
        cmaInst.instanceType = INSTANCE_TYPE_UNDEFINED
        info = fp.read(8)
        (cmaInst.instanceId, cmaInst.mirrorId) = struct.unpack("=Ii", info)
        self.__unPackIps(fp, cmaInst.listenIps)
        cmaInst.datadir = "%s/cm_agent" % dbNode.cmDataDir
        dbNode.cmagents.append(cmaInst)
       
    def __unpackGtmInfo(self, fp, dbNode):
        """
        unpack the info of gtm
        """ 
        gtmInst = instanceInfo()
        gtmInst.instanceRole = INSTANCE_ROLE_GTM
        gtmInst.hostname = dbNode.name
        info = fp.read(1036)
        (gtmInst.instanceId, gtmInst.mirrorId, gtmNum, gtmInst.datadir) = struct.unpack("=III1024s", info)
        gtmInst.datadir = gtmInst.datadir.strip('\x00')
        self.__unPackIps(fp, gtmInst.listenIps)
        info = fp.read(8)
        (gtmInst.port, gtmInst.instanceType) = struct.unpack("=II", info)
        if(gtmInst.instanceType == MASTER_INSTANCE):
            dbNode.gtmNum = 1
        elif(gtmInst.instanceType == STANDBY_INSTANCE):
            dbNode.gtmNum = 0
        else:
            raise Exception("Invalid gtm instance type: %d" % gtmInst.instanceType)
        self.__unPackIps(fp, gtmInst.haIps)
        info = fp.read(4)
        (gtmInst.haPort,) = struct.unpack("=I", info)
        info = fp.read(1024 + 4 + 128 * MAX_IP_NUM + 4)
        
        if(gtmNum == 1):
            dbNode.gtms.append(gtmInst)
        else:
            dbNode.gtms = []
    
    def __unpackGtmProxyInfo(self, fp, dbNode):
        """
        unpack the info of gtm proxy
        """
        proxyInst = instanceInfo()
        proxyInst.instanceRole = INSTANCE_ROLE_GTMPROXY
        proxyInst.hostname = dbNode.name
        info = fp.read(12)
        (proxyInst.instanceId, proxyInst.mirrorId, gtmProxyNum) = struct.unpack("=III", info)
        self.__unPackIps(fp, proxyInst.listenIps)
        info = fp.read(4)
        (proxyInst.port,) = struct.unpack("=I", info)

        if(gtmProxyNum == 1):
            dbNode.gtmProxyNum = 1
            dbNode.gtmProxys.append(proxyInst)
        else:
            dbNode.gtmProxyNum = 0
            dbNode.gtmProxys = []
        
    def __unpackCooInfo(self, fp, dbNode):
        """
        unpack the info of coordinator
        """
        cooInst = instanceInfo()
        cooInst.instanceRole = INSTANCE_ROLE_COODINATOR
        cooInst.hostname = dbNode.name
        cooInst.instanceType = INSTANCE_TYPE_UNDEFINED
        info = fp.read(1036)
        (cooInst.instanceId, cooInst.mirrorId, cooNum, cooInst.datadir) = struct.unpack("=IiI1024s", info)
        cooInst.datadir = cooInst.datadir.strip('\x00')
        self.__unPackIps(fp, cooInst.listenIps)
        info = fp.read(8)
        (cooInst.port, cooInst.haPort) = struct.unpack("=II", info)
        if(cooNum == 1):
            dbNode.cooNum = 1
            dbNode.coordinators.append(cooInst)
        else:
            dbNode.cooNum = 0
            dbNode.coordinators = []
    
    def __unpackDataNode(self, fp, dbNode):
        """
        unpack the info of datanode
        """
        info = fp.read(4)
        (dataNodeNums,) = struct.unpack("=I", info)
        dbNode.dataNum = 0
        
        dbNode.datanodes = []
        for i in range(dataNodeNums):
            dnInst = instanceInfo()
            dnInst.instanceRole = INSTANCE_ROLE_DATANODE
            dnInst.hostname = dbNode.name
            info = fp.read(1032)
            (dnInst.instanceId, dnInst.mirrorId, dnInst.datadir) = struct.unpack("=II1024s", info)
            dnInst.datadir = dnInst.datadir.strip('\x00')
            self.__unPackIps(fp, dnInst.listenIps)
            info = fp.read(8)
            (dnInst.port, dnInst.instanceType) = struct.unpack("=II", info)
            if(dnInst.instanceType == MASTER_INSTANCE):
                dbNode.dataNum += 1
            elif(dnInst.instanceType == STANDBY_INSTANCE or dnInst.instanceType == DUMMY_STANDBY_INSTANCE):
                pass
            else:
                raise Exception("Invalid datanode instance type: %d" % dnInst.instanceType)
            self.__unPackIps(fp, dnInst.haIps)
            info = fp.read(4)
            (dnInst.haPort,) = struct.unpack("=I", info)
            info = fp.read((1024 + 4 + 128 * MAX_IP_NUM + 4 + 4) * 2)
            dbNode.datanodes.append(dnInst)

    def initFromXml(self, xmlFile):
        """
        Init cluster from xml config file
        """
        if not os.path.exists(xmlFile):
            raise Exception("xml config file does not exist!")
        
        self.xmlFile = xmlFile
        
        # Set the environment variable, then the readcluster command can read from it.
        os.putenv(ENV_CLUSTERCONFIG, xmlFile)

        global xmlRootNode
        xmlRootNode = initParserXMLFile(xmlFile)
        
        # Read cluster name
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, "clusterName", "cluster")
        if(retStatus != 0):
            raise Exception("Read cluster name failed! error: %s" % retValue)
        self.name = retValue.strip()
        
        # Read application install path
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, "gaussdbAppPath", "cluster")
        if(retStatus != 0):
            raise Exception("Read application install path failed! error: %s" % retValue)
        self.appPath = os.path.normpath(retValue)

        # Read application log path
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, "gaussdbLogPath", "cluster")
        if(retStatus == 0):
            self.logPath = os.path.normpath(retValue)
        elif(retStatus == 2):
            self.logPath = ""
        else:
            raise Exception("Read application log path failed! error: %s" % retValue)

        if (self.logPath == ""):
            self.logPath = "/var/log/gaussdb"

        if (not os.path.isabs(self.logPath)):
            raise Exception("gaussdbLogPath need absolute path! gaussdbLogPath: %s" % self.logPath)

        self.__readClusterNodeInfo()
        self.__readExpandNodeInfo()

        self.__checkClusterConfig()
        
    def getClusterNodeNames(self):
        """
        Get node names in cluster
        """
        return [dbNode.name for dbNode in self.dbNodes]
    
    def getDbNodeByName(self, name):
        """
        Get node by name
        """
        for dbNode in self.dbNodes:
            if (dbNode.name == name):
                return dbNode
            
        return None
    
    def getMirrorInstance(self, mirrorId):
        """
        Get primary and standby instance
        """
        instances = []
        
        for dbNode in self.dbNodes:
            for inst in dbNode.cmservers:
                if (inst.mirrorId == mirrorId):
                    instances.append(inst)
            
            for inst in dbNode.gtms:
                if (inst.mirrorId == mirrorId):
                    instances.append(inst)
                    
            for inst in dbNode.gtmProxys:
                if (inst.mirrorId == mirrorId):
                    instances.append(inst)
            
            for inst in dbNode.coordinators:
                if (inst.mirrorId == mirrorId):
                    instances.append(inst)
            
            for inst in dbNode.datanodes:
                if (inst.mirrorId == mirrorId):
                    instances.append(inst)
                
        return instances
    
    def getPeerInstance(self, dbInst):
        """
        Get peer instance of specified instance
        """
        instances = []
        
        if (dbInst.instanceRole == INSTANCE_ROLE_CMSERVER):
            for dbNode in self.dbNodes:
                for inst in dbNode.cmservers:
                    if (inst.mirrorId == dbInst.mirrorId and inst.instanceId != dbInst.instanceId):
                        instances.append(inst)
        elif (dbInst.instanceRole == INSTANCE_ROLE_GTM):
            for dbNode in self.dbNodes:
                for inst in dbNode.gtms:
                    if (inst.mirrorId == dbInst.mirrorId and inst.instanceId != dbInst.instanceId):
                        instances.append(inst)
        elif (dbInst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            for dbNode in self.dbNodes:
                for inst in dbNode.gtmProxys:
                    if (inst.mirrorId == dbInst.mirrorId and inst.instanceId != dbInst.instanceId):
                        instances.append(inst)
        elif (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            for dbNode in self.dbNodes:
                for inst in dbNode.coordinators:
                    if (inst.mirrorId == dbInst.mirrorId and inst.instanceId != dbInst.instanceId):
                        instances.append(inst)
        elif (dbInst.instanceRole == INSTANCE_ROLE_DATANODE):
            for dbNode in self.dbNodes:
                for inst in dbNode.datanodes:
                    if (inst.mirrorId == dbInst.mirrorId and inst.instanceId != dbInst.instanceId):
                        instances.append(inst)
                        
        return instances
    
    def getClusterBackIps(self):
        """
        Get cluster backips
        """
        backIps = []
        backIpNum = []
        for dbNode in self.dbNodes:
            backIpNum.append(len(dbNode.backIps))
        if max(backIpNum) != min(backIpNum):
            raise Exception("The number of backIps on all nodes are diffrent!")
        for num in range(backIpNum[0]):    
            ips = []
            for dbNode in self.dbNodes:
                ips.append(dbNode.backIps[num])
            backIps.append(ips)
        return backIps

    def getNodeNameByBackIp(self, backIp):
        """
        Get Node Name
        """
        nodeName = ""
        for dbNode in self.dbNodes:
            if(backIp in dbNode.backIps):
                nodeName = dbNode.name
                break
        return nodeName
   
    def __readClusterNodeInfo(self):
        """
        Read cluster node info
        """
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, "nodeNames", "cluster")
        if (retStatus != 0):
            raise Exception("Read node names failed! error: %s" % retValue)
        nodeNames = retValue.split(",")
        if (len(nodeNames) == 0):
            raise Exception("Read cluster config failed!There is no nodes in cluster config file!")
        
        # Get basic info of node: name, ip and master instance number etc.
        self.dbNodes = []
        i = 1
        for name in nodeNames:
            dbNode = dbNodeInfo(i, name)
            self.__readNodeBasicInfo(dbNode)
            self.dbNodes.append(dbNode)
            i += 1
        # Get cm server info
        for dbNode in self.dbNodes:
            self.__readCmsConfig(dbNode)

        # Get gtm info
        for dbNode in self.dbNodes:
            self.__readGtmConfig(dbNode)
        
        # Get gtm proxy info
        for dbNode in self.dbNodes:
            self.__readGtmProxyConfig(dbNode)

        # Get coordinator info
        for dbNode in self.dbNodes:
            self.__readCooConfig(dbNode)

        # Get datanode info
        for dbNode in self.dbNodes:
            self.__readDataNodeConfig(dbNode)

        for dbNode in self.dbNodes:
            self.__readCmaConfig(dbNode)
            
    def __readExpandNodeInfo(self):
        """
        Read expand node info
        """
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, "sqlExpandNames", "cluster")
        if(retStatus != 0 or retValue.strip() == ""):
            return
        nodeNames = retValue.split(",")
        if (len(nodeNames) == 0):
            return
        
        for nodeName in nodeNames:
            dbNode = self.getDbNodeByName(nodeName)
            if (dbNode != None):
                self.newNodes.append(dbNode)
            else:
                raise Exception("Read expand nodes config failed!There is no node[%s] in cluster config file!" % nodeName)
    
    def __readNodeBasicInfo(self, dbNode):
        """
        Read basic info of specified node
        """
        # Get node ips
        dbNode.backIps = self.__readNodeIps(dbNode.name, "backIp")
        if (len(dbNode.backIps) == 0):
            raise Exception("There backip of node[%s] is empty!" % dbNode.name)
        dbNode.sshIps = self.__readNodeIps(dbNode.name, "sshIp")
        if (len(dbNode.sshIps) == 0):
            dbNode.sshIps = dbNode.backIps[:]
        
        # Get instance number 
        dbNode.cmsNum = self.__readNodeIntValue(dbNode.name, "cmsNum", True, 0)
        dbNode.gtmNum = self.__readNodeIntValue(dbNode.name, "gtmNum", True, 0)
        dbNode.gtmProxyNum = self.__readNodeIntValue(dbNode.name, "gtmProxyNum", True, 0)
        dbNode.cooNum = self.__readNodeIntValue(dbNode.name, "cooNum", True, 0)
        dbNode.dataNum = self.__readNodeIntValue(dbNode.name, "dataNum", True, 0)
        
        # read cm directory for server and agent
        dbNode.cmDataDir = self.__readNodeStrValue(dbNode.name, "cmDir")
        
        # Get base port
        # todo: comment
        if (dbNode.cmsNum > 0):
            dbNode.masterBasePorts[INSTANCE_ROLE_CMSERVER] = self.__readNodeIntValue(dbNode.name, "cmServerPortBase")
        dbNode.standbyBasePorts[INSTANCE_ROLE_CMSERVER] = self.__readNodeIntValue(dbNode.name, "cmServerPortStandby", True, STANDBY_BASEPORT_CMS)
        
        if (dbNode.gtmNum > 0):
            dbNode.masterBasePorts[INSTANCE_ROLE_GTM] = self.__readNodeIntValue(dbNode.name, "gtmPortBase")
        dbNode.standbyBasePorts[INSTANCE_ROLE_GTM] = self.__readNodeIntValue(dbNode.name, "gtmPortStandby", True, STANDBY_BASEPORT_GTM)
        
        if (dbNode.gtmProxyNum > 0):
            dbNode.masterBasePorts[INSTANCE_ROLE_GTMPROXY] = self.__readNodeIntValue(dbNode.name, "gtmProxyPortBase")
        
        if (dbNode.cooNum > 0):
            dbNode.masterBasePorts[INSTANCE_ROLE_COODINATOR] = self.__readNodeIntValue(dbNode.name, "cooPortBase")
        
        if (dbNode.dataNum > 0):
            dbNode.masterBasePorts[INSTANCE_ROLE_DATANODE] = self.__readNodeIntValue(dbNode.name, "dataPortBase")
        dbNode.standbyBasePorts[INSTANCE_ROLE_DATANODE] = self.__readNodeIntValue(dbNode.name, "dataPortStandby", True, STANDBY_BASEPORT_DATA)
        dbNode.dummyStandbyBasePort = self.__readNodeIntValue(dbNode.name, "dataPortDummyStandby", True, DUMMY_STANDBY_BASEPORT_DATA)

        #get sctp port range
        dbNode.sctpBeginPort = self.__readNodeIntValue(dbNode.name, "sctpBeginPort", True, SCTP_BEGIN_PORT)
        dbNode.sctpEndPort = self.__readNodeIntValue(dbNode.name, "sctpEndPort", True, SCTP_END_PORT)

    def __readCmsConfig(self, masterNode):
        """
        Read cm server config on node
        """
        cmsListenIps = None
        cmsHaIps = None
        if (masterNode.cmsNum > 0):
            cmsListenIps = self.__readInstanceIps(masterNode.name, "cmServerListenIp", masterNode.cmsNum * MIRROR_COUNT_CMS)
            cmsHaIps = self.__readInstanceIps(masterNode.name, "cmServerHaIp", masterNode.cmsNum * MIRROR_COUNT_CMS)
        
        for i in range(masterNode.cmsNum):
            key = "cmServerlevel"
            level = self.__readNodeIntValue(masterNode.name, key)
            key = "cmServerRelation"
            hostNames = self.__readNodeStrValue(masterNode.name, key).split(",")
            if (len(hostNames) != MIRROR_COUNT_CMS):
                raise Exception("Read cm server config on host[%s] failed!The info of %s is wrong" % (masterNode.name, key))
            
            instId = self.__assignNewInstanceId(INSTANCE_ROLE_CMSERVER)
            mirrorId = self.__assignNewMirrorId()
            instIndex = i * MIRROR_COUNT_CMS
            masterNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_CMSERVER, MASTER_INSTANCE, cmsListenIps[instIndex], cmsHaIps[instIndex], "", level)
            
            for j in range(1, MIRROR_COUNT_CMS):
                dbNode = self.getDbNodeByName(hostNames[j])
                if dbNode is None:
                    raise Exception("Read cm server config on host[%s] failed!There is no host named %s" % (masterNode.name, hostNames[j]))
                instId = self.__assignNewInstanceId(INSTANCE_ROLE_CMSERVER)
                instIndex += 1
                dbNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_CMSERVER, STANDBY_INSTANCE, cmsListenIps[instIndex], cmsHaIps[instIndex], "", level)
    
    def __readGtmConfig(self, masterNode):
        """
        Read gtm config on node
        """
        gtmListenIps = None
        gtmHaIps = None
        if (masterNode.gtmNum > 0):
            gtmListenIps = self.__readInstanceIps(masterNode.name, "gtmListenIp", masterNode.gtmNum * MIRROR_COUNT_GTM)
            gtmHaIps = self.__readInstanceIps(masterNode.name, "gtmHaIp", masterNode.gtmNum * MIRROR_COUNT_GTM)
            
        for i in range(masterNode.gtmNum):
            key = "gtmRelation"
            hostNames = self.__readNodeStrValue(masterNode.name, key).split(",") 
            if (len(hostNames) != MIRROR_COUNT_CMS):
                raise Exception("Read gtm config on host[%s] failed!The info of %s is wrong" % (masterNode.name, key))
            key = "gtmDir%d" % (i + 1)
            gtmInfoList = self.__readNodeStrValue(masterNode.name, key).split(",")
            if (len(gtmInfoList) != 2 * MIRROR_COUNT_CMS - 1):
                raise Exception("Read gtm config on host[%s] failed!The info of %s is wrong" % (masterNode.name, key))

            instId = self.__assignNewInstanceId(INSTANCE_ROLE_GTM)
            mirrorId = self.__assignNewMirrorId()
            instIndex = i * MIRROR_COUNT_GTM
            masterNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_GTM, MASTER_INSTANCE, gtmListenIps[instIndex], gtmHaIps[instIndex], gtmInfoList[0])
            
            for j in range(1, MIRROR_COUNT_GTM):
                dbNode = self.getDbNodeByName(hostNames[j])
                if dbNode is None:
                    raise Exception("Read gtm config on host[%s] failed!There is no host named %s" % (masterNode.name, hostNames[j]))
                instId = self.__assignNewInstanceId(INSTANCE_ROLE_GTM)
                instIndex += 1
                dbNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_GTM, STANDBY_INSTANCE, gtmListenIps[instIndex], gtmHaIps[instIndex], gtmInfoList[2 * j])
    
    def __readGtmProxyConfig(self, dbNode):
        """
        Read gtm proxy config on node
        """
        proxyListenIps = None
        if (dbNode.gtmProxyNum > 0):
            proxyListenIps = self.__readInstanceIps(dbNode.name, "gtmProxyListenIp", 1)
        
        for i in range(dbNode.gtmProxyNum):
            instId = self.__assignNewInstanceId(INSTANCE_ROLE_GTMPROXY)
            dbNode.appendInstance(instId, MIRROR_ID_GTMPROXY, INSTANCE_ROLE_GTMPROXY, INSTANCE_TYPE_UNDEFINED, proxyListenIps[i])
    
    def __readCooConfig(self, dbNode):
        """
        Read coodinator config on node
        """
        cooListenIps = None
        if (dbNode.cooNum > 0): 
            cooListenIps = self.__readInstanceIps(dbNode.name, "cooListenIp", 1)
        
        for i in range(dbNode.cooNum):
            key = "cooDir%d" % (i + 1)
            cooDir = self.__readNodeStrValue(dbNode.name, key)
            instId = self.__assignNewInstanceId(INSTANCE_ROLE_COODINATOR)
            
            dbNode.appendInstance(instId, MIRROR_ID_COO, INSTANCE_ROLE_COODINATOR, INSTANCE_TYPE_UNDEFINED, cooListenIps[i], None, cooDir)
            i += 1
    
    def __readDataNodeConfig(self, masterNode):
        """
        Read datanode config on node
        """
        dnListenIps = None
        dnHaIps = None
        if (masterNode.dataNum > 0):
            dnListenIps = self.__readInstanceIps(masterNode.name, "dataListenIp", masterNode.dataNum * MIRROR_COUNT_DATA, True)
            dnHaIps = self.__readInstanceIps(masterNode.name, "dataHaIp", masterNode.dataNum * MIRROR_COUNT_DATA, True)

        dnInfoLists = [[] for row in range(masterNode.dataNum)]
        totalDnInstanceNum = 0
        for i in range(masterNode.dataNum):
            key = "dataNode%d" % (i + 1)
            dnInfoList = self.__readNodeStrValue(masterNode.name, key).split(",")
            dnInfoListLen = len(dnInfoList)
            if ((dnInfoListLen != 2 * MIRROR_COUNT_DATA - 1)):
                raise Exception("Read datanode config on host[%s] failed!The info of %s is wrong" % (masterNode.name, key))
            totalDnInstanceNum += (dnInfoListLen + 1) / 2
            dnInfoLists[i].extend(dnInfoList)

        #check ip num
        if(dnListenIps != None and len(dnListenIps[0]) != 0):
            colNum = len(dnListenIps[0])
            rowNum = len(dnListenIps)
            for col in range(colNum):
                ipNum = 0
                for row in range(rowNum):
                    if(dnListenIps[row][col] != ""):
                        ipNum += 1
                    else:
                        break
                if(ipNum != totalDnInstanceNum):
                    raise Exception("The ip num of dataListenIp is not match with instance num, please check it!")

        if(dnHaIps != None and len(dnHaIps[0]) != 0):
            colNum = len(dnHaIps[0])
            rowNum = len(dnHaIps)
            for col in range(colNum):
                ipNum = 0
                for row in range(rowNum):
                    if(dnHaIps[row][col] != ""):
                        ipNum += 1
                    else:
                        break
                if(ipNum != totalDnInstanceNum):
                    raise Exception("The ip num of dnHaIps is not match with instance num, please check it!")

        instIndex = 0
        for i in range(masterNode.dataNum):
            dnInfoList = dnInfoLists[i]
            mirrorId = self.__assignNewMirrorId()
            #master datanode
            instId = self.__assignNewInstanceId(INSTANCE_ROLE_DATANODE)
            masterNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_DATANODE, MASTER_INSTANCE, dnListenIps[instIndex], dnHaIps[instIndex], dnInfoList[0])
            instIndex += 1
            
            #standby datanode
            dbNode = self.getDbNodeByName(dnInfoList[1])
            if dbNode is None:
                raise Exception("Read datanode config on host[%s] failed!There is no host named %s" % (masterNode.name, dnInfoList[1]))
            instId = self.__assignNewInstanceId(INSTANCE_ROLE_DATANODE)
            dbNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_DATANODE, STANDBY_INSTANCE, dnListenIps[instIndex], dnHaIps[instIndex], dnInfoList[2])
            instIndex += 1
            
            #dummy standby datanode
            dbNode = self.getDbNodeByName(dnInfoList[3])
            if dbNode is None:
                raise Exception("Read datanode config on host[%s] failed!There is no host named %s" % (masterNode.name, dnInfoList[3]))
            instId = self.__assignNewDummyInstanceId()
            dbNode.appendInstance(instId, mirrorId, INSTANCE_ROLE_DATANODE, DUMMY_STANDBY_INSTANCE, dnListenIps[instIndex], dnHaIps[instIndex], dnInfoList[4])
            instIndex += 1
    
    def __readCmaConfig(self, dbNode):
        """
        Read cm agent config on node
        """
        agentIps = self.__readInstanceIps(dbNode.name, "cmAgentConnectIP", 1)
        instId = self.__assignNewInstanceId(INSTANCE_ROLE_CMAGENT)
        dbNode.appendInstance(instId, MIRROR_ID_AGENT, INSTANCE_ROLE_CMAGENT, INSTANCE_TYPE_UNDEFINED, agentIps[0], None, "")
    
    def __assignNewInstanceId(self, instRole):
        """
        Assign a new id for instance
        """
        newId = self.__newInstanceId[instRole]
        self.__newInstanceId[instRole] += 1
        
        return newId

    def __assignNewDummyInstanceId(self):
        """
        Assign a new dummy standby instance id
        """
        self.__newDummyStandbyId += 1

        return self.__newDummyStandbyId
    
    def __assignNewMirrorId(self):
        """
        Assign a new mirror id
        """
        
        self.__newMirrorId += 1
        
        return self.__newMirrorId
    
    def __readNodeIps(self, nodeName, prefix):
        """
        Read ip for node, such as backIp<1~N>, sshIp<1~N> etc.
        """
        i = 1
        ipList = []
        while True:
            key = "%s%d" % (prefix, i)
            value = self.__readNodeStrValue(nodeName, key, True, "")
            if (value == ""):
                break
            ipList.append(value)
            i += 1
        
        return ipList
    
    def __readInstanceIps(self, nodeName, prefix, InstCount, isDataNode = False):
        """
        """
        multiIpList = self.__readNodeIps(nodeName, prefix)
        
        mutilIpCount = len(multiIpList)
        if (mutilIpCount == 0):
            return [[] for row in range(InstCount)]
        
        instanceIpList = [["" for col in range(mutilIpCount)] for row in range(InstCount)]
        for i in range(mutilIpCount):
            ipList = multiIpList[i].split(",")
            ipNum = len(ipList)
            if (ipNum != InstCount):
                raise Exception("Read %s of node[%s] failed!The count of ip is wrong" % (prefix, nodeName))
            for j in range(ipNum):
                instanceIpList[j][i] = ipList[j]
                
        return instanceIpList
    
    def __readNodeIntValue(self, nodeName, key, nullable=False, defValue=0):
        """
        Read integer value of specified node
        """
        value = defValue
        
        strValue = self.__readNodeStrValue(nodeName, key, nullable, "")
        if (strValue != ""):
            value = int(strValue)
            
        return value
    
    def __readNodeStrValue(self, nodeName, key, nullable=False, defValue=""):
        """
        Read string value of specified node
        """
        (retStatus, retValue) = readOneClusterConfigItem(xmlRootNode, key, "node", nodeName)
        if (retStatus == 0):
            return str(retValue).strip()
        elif (retStatus == 2 and nullable):
            return  defValue
        else:    
            raise Exception("Read %s of node[%s] failed!Return value:%d, Error: %s" % (key, nodeName, retStatus, retValue))
    
    def __checkClusterConfig(self):
        """
        Check instance count
        """
        cmsNum = 0
        gtmNum = 0
        cooNum = 0
        dataNum = 0
        
        for dbNode in self.dbNodes:
            cmsNum += len(dbNode.cmservers)
            gtmNum += len(dbNode.gtms)
            cooNum += len(dbNode.coordinators)
            dataNum += len(dbNode.datanodes)
        
        if (cmsNum == 0):
            raise Exception("There is no info about cm server.Please set it!")
        
        if (gtmNum == 0):
            raise Exception("There is no info about gtm.Please set it!")
        
        if (cooNum == 0):
            raise Exception("There is no info about coordinator.Please set it!")
        
        if (dataNum == 0):
            raise Exception("There is no info about datanode.Please set it!")
            
        for dbNode in self.dbNodes:
            dbInstList = []
            portList = []
            dbInstList.extend(dbNode.cmservers)
            dbInstList.extend(dbNode.coordinators)
            dbInstList.extend(dbNode.datanodes)
            dbInstList.extend(dbNode.gtms)
            for dbInst in dbInstList:
                portList.append(dbInst.port)
                portList.append(dbInst.haPort)
            for port in portList:
                if(portList.count(port) > 1):
                    raise Exception("Port %d is conflicting, plese check it!" % port)

            if(dbNode.sctpBeginPort < 1024 or dbNode.sctpBeginPort > 65535):
                raise Exception("%s:%s should be in 1024-65535." % (dbNode.name, dbNode.sctpBeginPort))
            if(dbNode.sctpEndPort < 1024 or dbNode.sctpEndPort > 65535):
                raise Exception("%s:%s should be in 1024-65535." % (dbNode.name, dbNode.sctpEndPort))
            sctpNeedRange = len(dbNode.datanodes) * 1024
            sctpRealRange = dbNode.sctpEndPort - dbNode.sctpBeginPort + 1
            if (sctpRealRange < sctpNeedRange):
                raise Exception("%s:the sctp port range is too small, at least %d, given %d" % (dbNode.name, sctpNeedRange, sctpRealRange))
            
        if (len(self.dbNodes) - len(self.newNodes) <= 1):
            raise Exception("Old nodes is less than 2. Please check the cluster config file!")
        for dbNode in self.newNodes:
            if (dbNode.cmsNum > 0 or dbNode.gtmNum > 0):
                raise Exception("Can't dilate cmserver or gtm on new node[%s].Please check the cluster config file!" % dbNode.name)
            if (dbNode.cooNum == 0 and dbNode.dataNum == 0):
                raise Exception("Can't dilate without coordinator or datanode on new node[%s].Please check the cluster config file!" % dbNode.name)
                
    def saveToStaticConfig(self, filePath, localNodeId, dbNodes = None):
        """
        Save cluster into to static config
        """
        try:
            if (dbNodes is None):
                dbNodes = self.dbNodes
            fp = open(filePath, "wb")
        
            info = struct.pack("I", 28) # len
            info += struct.pack("I", BIN_CONFIG_VERSION) # version
            info += struct.pack("q", time.time()) # time
            info += struct.pack("I", len(dbNodes)) # node count
            info += struct.pack("I", localNodeId) # local node
            
            crc = binascii.crc32(info)
            info = struct.pack("i", crc) + info
            fp.write(info)
            
            for dbNode in dbNodes:
                offset = (fp.tell() / PAGE_SIZE + 1) * PAGE_SIZE
                fp.seek(offset)
                
                info = self.__packNodeInfo(dbNode)
                fp.write(info)
            endBytes = PAGE_SIZE - fp.tell() % PAGE_SIZE
            if (endBytes != PAGE_SIZE):
                info = struct.pack("%dx" % endBytes)
                fp.write(info)
            fp.flush()
            fp.close()
        except Exception, e:
            raise Exception("Save static config file failed! Error: %s" % str(e))
    
    def __packNodeInfo(self, dbNode):
        """
        Pack the info of node
        """
        info = struct.pack("I", dbNode.id)
        info += struct.pack("64s", dbNode.name)
        info += self.__packIps(dbNode.backIps)
        info += self.__packIps(dbNode.sshIps)
        info += self.__packCmsInfo(dbNode)
        info += self.__packAgentInfo(dbNode)
        info += self.__packGtmInfo(dbNode)
        info += self.__packGtmProxyInfo(dbNode)
        info += self.__packCooInfo(dbNode)
        info += self.__packDataNode(dbNode)
        info += struct.pack("I", dbNode.sctpBeginPort) # sctp begin port
        info += struct.pack("I", dbNode.sctpEndPort) # sctp end port
        crc = binascii.crc32(info)
        
        return struct.pack("i", crc) + info
    
    def __packCmsInfo(self, dbNode):
        """
        Pack the info of cm server
        """
        n = len(dbNode.cmservers)
        
        info = ""
        if (n == 0):
            info += struct.pack("I", 0) # cm server id
            info += struct.pack("I", 0) # cm_server mirror id
            info += struct.pack("1024s", dbNode.cmDataDir) # datadir
            info += struct.pack("I", 0) # cm server level
            info += struct.pack("128x") # float ip
            info += self.__packIps([]) # listen ip
            info += struct.pack("I", 0) # listen port
            info += self.__packIps([]) # local ha ip
            info += struct.pack("I", 0) # local ha port
            info += struct.pack("I", 0) # is primary
            info += self.__packIps([]) # peer ha ip
            info += struct.pack("I", 0) # peer ha port
        elif(n == 1):
            cmsInst = dbNode.cmservers[0]
            instances = self.getPeerInstance(cmsInst)
            peerInst = instances[0]
            
            info += struct.pack("I", cmsInst.instanceId) # cm server id
            info += struct.pack("I", cmsInst.mirrorId) # cm_server mirror id
            info += struct.pack("1024s", dbNode.cmDataDir) # datadir
            info += struct.pack("I", cmsInst.level) # cm server level
            info += struct.pack("128s", self.cmsFloatIp)
            info += self.__packIps(cmsInst.listenIps) # listen ip
            info += struct.pack("I", cmsInst.port) # listen port
            info += self.__packIps(cmsInst.haIps) # local ha ip
            info += struct.pack("I", cmsInst.haPort) # local ha port
            info += struct.pack("I", cmsInst.instanceType) # instance type
            info += self.__packIps(peerInst.haIps) # peer ha ip
            info += struct.pack("I", peerInst.haPort) # peer ha port
        else:
            pass
        
        return info
    
    def __packAgentInfo(self, dbNode):
        """
        Pack the info of agent
        """
        n = len(dbNode.cmagents)
        
        info = ""
        if (n == 1):
            cmaInst = dbNode.cmagents[0]
            
            info += struct.pack("I", cmaInst.instanceId) # Agent id
            info += struct.pack("i", cmaInst.mirrorId) # Agent mirror id
            info += self.__packIps(cmaInst.listenIps) # agent ips
        
        return info
    
    def __packGtmInfo(self, dbNode):
        """
        Pack the info of gtm
        """
        n = len(dbNode.gtms)
        
        info = ""
        if (n == 0):
            info += struct.pack("I", 0) # gtm id
            info += struct.pack("I", 0) # gtm mirror id
            info += struct.pack("I", 0) # gtm count
            info += struct.pack("1024x") # datadir
            info += self.__packIps([]) # listen ip
            info += struct.pack("I", 0) # listn port
            info += struct.pack("I", 0) #  instance type
            info += self.__packIps([]) # loacl ha ip
            info += struct.pack("I", 0) # local ha port
            info += struct.pack("1024x") # peer gtm datadir
            info += self.__packIps([]) # peer ha ip
            info += struct.pack("I", 0) # peer ha port
        elif (n == 1):
            gtmInst = dbNode.gtms[0]
            instances = self.getPeerInstance(gtmInst)
            peerInst = instances[0]
            
            info += struct.pack("I", gtmInst.instanceId) # gtm id
            info += struct.pack("I", gtmInst.mirrorId) # gtm mirror id
            info += struct.pack("I", 1) # gtm count
            info += struct.pack("1024s", gtmInst.datadir) # datadir
            info += self.__packIps(gtmInst.listenIps) # listen ip
            info += struct.pack("I", gtmInst.port) # listn port
            info += struct.pack("I", gtmInst.instanceType) #  instance type
            info += self.__packIps(gtmInst.haIps) # loacl ha ip
            info += struct.pack("I", gtmInst.haPort) # local ha port
            info += struct.pack("1024s", peerInst.datadir) # peer gtm datadir
            info += self.__packIps(peerInst.haIps) # peer ha ip
            info += struct.pack("I", peerInst.haPort) # peer ha port
        else:
            pass
        
        return info
            
    def __packGtmProxyInfo(self, dbNode):   
        """
        Pack the info of gtm proxy
        """
        n = len(dbNode.gtmProxys)
        
        info = ""
        if (n == 0):
            info += struct.pack("I", 0) # gtm proxy id
            info += struct.pack("I", 0) # gtm proxy mirror id
            info += struct.pack("I", 0) # gtm proxy count
            info += self.__packIps([]) # listen ip
            info += struct.pack("I", 0) # listn port
        elif (n == 1):
            proxyInst = dbNode.gtmProxys[0]
            
            info += struct.pack("I", proxyInst.instanceId) # gtm proxy id
            info += struct.pack("I", proxyInst.mirrorId) # gtm proxy mirror id
            info += struct.pack("I", 1) # gtm proxy count
            info += self.__packIps(proxyInst.listenIps) # listen ip
            info += struct.pack("I", proxyInst.port) # listn port
        else:
            pass
        
        return info
    
    def __packCooInfo(self, dbNode):
        """
        Pack the info of coordinator
        """
        n = len(dbNode.coordinators)
        
        info = ""
        if (n == 0):
            info += struct.pack("I", 0) # coordinator id
            info += struct.pack("i", 0) # coordinator mirror id
            info += struct.pack("I", 0) # coordinator count
            info += struct.pack("1024x") # datadir
            info += self.__packIps([]) # listen ip
            info += struct.pack("I", 0) # listn port 
            info += struct.pack("I", 0) # ha port
        elif (n == 1):
            cooInst = dbNode.coordinators[0]
            
            info += struct.pack("I", cooInst.instanceId) # coordinator id
            info += struct.pack("i", cooInst.mirrorId) # coordinator mirror id
            info += struct.pack("I", 1) # coordinator count
            info += struct.pack("1024s", cooInst.datadir) # datadir
            info += self.__packIps(cooInst.listenIps) # listen ip
            info += struct.pack("I", cooInst.port) # listn port
            info += struct.pack("I", cooInst.haPort) # ha port
        else:
            pass
        
        return info
    
    def __packDataNode(self, dbNode):
        """
        Pack the info of datanode
        """
        
        info = struct.pack("I", len(dbNode.datanodes))
        for dnInst in dbNode.datanodes:
            instances = self.getPeerInstance(dnInst)
            
            info += struct.pack("I", dnInst.instanceId) # datanode id
            info += struct.pack("I", dnInst.mirrorId) # datanode id
            info += struct.pack("1024s", dnInst.datadir) # datadir
            info += self.__packIps(dnInst.listenIps) # listen ip
            info += struct.pack("I", dnInst.port) # port
            info += struct.pack("I", dnInst.instanceType) # instance type
            info += self.__packIps(dnInst.haIps) # loacl ha ip
            info += struct.pack("I", dnInst.haPort) # local ha port
            
            n = len(instances)
            for i in range(n):
                peerInst = instances[i]
                info += struct.pack("1024s", peerInst.datadir) # peer1 datadir
                info += self.__packIps(peerInst.haIps) # peer1 ha ip
                info += struct.pack("I", peerInst.haPort) # peer1 ha port
                info += struct.pack("I", peerInst.instanceType)# instance type
            for i in range(n, 2):
                info += struct.pack("1024x") # peer1 datadir
                info += self.__packIps([]) # peer1 ha ip
                info += struct.pack("I", 0) # peer1 ha port
                info += struct.pack("I", peerInst.instanceType)# instance type
        
        return info

    def __packIps(self, ips):
        """
        Pack the info of ips
        """
        n = len(ips)
        
        info = struct.pack("I", n)
        for i in range(n):
            info += struct.pack("128s",ips[i])
        for i in range(n, MAX_IP_NUM):
            info += struct.pack("128x")
        
        return info
