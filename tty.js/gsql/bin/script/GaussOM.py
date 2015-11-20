#!/usr/bin/env python
'''
Created on 2014-1-28

@author: 
'''
import commands
import getopt
import os
import sys
import socket
import time
import pwd
from datetime import datetime, timedelta

sys.path.append(sys.path[0] + "/../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.SshTool import SshTool
from script.util.DbClusterStatus import DbClusterStatus
from script.util.Common import ClusterCommand,DefaultValue,PlatformCommand
from script.util.OMCommand import OMCommand
from script.util.GaussStat import GaussStat

ACTION_START = "start"
ACTION_STOP = "stop"
ACTION_CLEAN = "clean"
ACTION_STATUS = "status"
ACTION_BACKUP = "backup"
ACTION_RESTORE = "restore"
ACTION_SWITCH = "switch"
ACTION_XLOGCHECK = "xlogcheck"
ACTION_BUILD = "build"
ACTION_HEALTH_CHECK = "healthcheck"
ACTION_PERFCHECK = "performancecheck"

LOCAL_HEALTH_CHECK = "./local/HealthCheck.py"
UTIL_GAUSS_STAT = "./util/GaussStat.py"

STOP_MODE_FAST = "fast"
STOP_MODE_IMMEDIATE = "immediate"
STOP_MODE_SMART = "smart"
ACTION_QUERY_SWITCHOVER = "switchquery"
ACTION_RESET_SWITCHOVER = "switchreset"

#############################################################################
# Global variables
#############################################################################

g_opts = None
g_logger = None
g_clusterInfo = None 
g_healthChecker = None
g_sshTool = None
g_gaussStat = None

class CmdOptions():    
    def __init__(self):
        self.script_name = os.path.split(__file__)[-1]
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.action = ""
        self.nodeName = ""
        self.time_out = None
        self.stopMode = STOP_MODE_FAST
        self.silenceMode = False
        self.cleanData = False
        self.show_detail = False
        self.dataDir = ""
        self.bakDir = ""
        self.bakParam = False
        self.bakBin = False
        self.outFile = ""
        self.logFile = ""
        self.localLog = ""
        self.confFile = DefaultValue.CLUSTER_CONFIG_PATH
        
        self.appPath = ""
        self.user = ""
        self.group = ""
        self.mpprcFile = ""

##################
#Check the specified items to determine whether the cluster healthy
class healthCheck():
    def __init__(self):
        self.fd_outputFile = None
        self.clusterHealthCheck = "Normal"
        self.user = ""
        self.group = "" 
        self.hostname = ""
        self.sqlFileName = "query.sql"
        self.sqlFileFp = None
        self.healthCheckItems = {
                            'clusterStatusCheck':['Normal', 'OK'],
                            'clusterServiceCheck':['Normal', 'OK'],  
                            'queryPerformanceCheck':['Normal', '0'],
                            'directoryPermissionCheck':['Normal', 'OK'],
                            'gaussdbVersionCheck':['Normal', 'OK'],
                            'gaussdbIntegrityCheck':['Normal', 'OK'],
                            'debugSwitchCheck':['Normal', 'OK'],
                            'environmentVariablesCheck':['Normal', 'OK'],
                            'osVersionCheck':['Normal', 'OK'],
                            'osKernelParameterCheck':['Normal', 'OK'],
                            'dbConnectionCheck':['Normal', 'OK'],
                            'omMonitorCheck':['Normal', 'OK'],
                            'lockNumCheck':['Normal', '0'],
                            'cursorNumCheck':['Normal', '0'],
                            'installDirUsageCheck':['Normal', 'OK'],
                            'dataDirUsageCheck':['Normal', 'OK'],
                            'connectionNumCheck':['Normal', 'OK'],
                            'gaussdbParameterCheck':['Normal', 'OK'],
                            'haCheck':['Normal', 'OK']
        }

    def checkClusterStatus(self):
        tmpDir = DefaultValue.getTmpDirFromEnv()
        tmpFile = os.path.join(tmpDir, "gauss_cluster_status.dat")
        cmd = ClusterCommand.getQueryStatusCmd(g_opts.user, 0, tmpFile, True)
        (status, output) = commands.getstatusoutput(cmd)
        
        if (status != 0):
            g_logger.debug("Get cluster status failed!")
            self.healthCheckItems['clusterStatusCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['clusterStatusCheck'][1] = "[%s] %s" % (self.hostname, output)
            if(os.path.exists(tmpFile)): os.remove(tmpFile)
            g_logger.debug("End check cluster status") 
            return 
            
        tmpFileFp = None
        tmpFileName = ""
        try:
            clusterStatus = DbClusterStatus()
            clusterStatus.initFromFile(tmpFile)  
            if(clusterStatus.isAllHealthy()):
                if(os.path.exists(tmpFile)): os.remove(tmpFile)
                g_logger.debug("End check cluster status") 
                return
        
            tmpFileName = os.path.join(tmpDir, "abnormal_node_status.dat")
            tmpFileFp = open(tmpFileName, "w+")
            for dbNode in clusterStatus.dbNodes:
                if (not dbNode.isNodeHealthy()):
                    dbNode.outputNodeStatus(tmpFileFp, g_opts.user, True)
            tmpFileFp.flush()
            tmpFileFp.seek(0)
            errorMessageStr = tmpFileFp.read()
            tmpFileFp.close()
            if(errorMessageStr == ""):
                errorMessageStr = "Get cluster status failed!"
            self.healthCheckItems['clusterStatusCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['clusterStatusCheck'][1] = "[%s] %s" % (self.hostname, errorMessageStr)      
            if(os.path.exists(tmpFileName)): os.remove(tmpFileName)   
            if(os.path.exists(tmpFile)): os.remove(tmpFile)
        except Exception,e:
            g_logger.debug("%s" % str(e))
            if(tmpFileFp):tmpFileFp.close()
            if(os.path.exists(tmpFileName)): os.remove(tmpFileName) 
            if(os.path.exists(tmpFile)): os.remove(tmpFile)
            self.healthCheckItems['clusterStatusCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['clusterStatusCheck'][1] = "[%s] %s" % (self.hostname, str(e))  
        g_logger.debug("End check cluster status")    

    def checkHAStatus(self):
        tmpDir = DefaultValue.getTmpDirFromEnv()
        tmpFile = os.path.join(tmpDir, "gauss_ha_status.dat")
        cmd = ClusterCommand.getQueryStatusCmd(g_opts.user, 0, tmpFile, True)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("Get ha status failed!Error: %s" % output)
            self.healthCheckItems['haCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['haCheck'][1] = "[%s] %s" % (self.hostname, output)
            return
            
        errorMessageStr = ""
        try:
            clusterStatus = DbClusterStatus()
            clusterStatus.initFromFile(tmpFile)
            if(os.path.exists(tmpFile)): os.remove(tmpFile)
            for dbNode in clusterStatus.dbNodes:
                for instance in dbNode.gtms:
                    if(instance.connStatus != "Normal"):
                        errorMessageStr += "Node:%s  DataPath:%s Conn Status:%s\n" % (dbNode.name, instance.datadir, instance.connStatus)
                for instance in dbNode.datanodes:
                    if(instance.haStatus != "Normal" and instance.haStatus != ""):
                        errorMessageStr += "Node:%s  DataPath:%s Conn Status:%s" % (dbNode.name, instance.datadir, instance.connStatus)
            if(errorMessageStr != ""):
                raise Exception("%s" % errorMessageStr)      
        except Exception,e:
            g_logger.debug("%s" % str(e))
            if(os.path.exists(tmpFile)): os.remove(tmpFile)
            self.healthCheckItems['haCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['haCheck'][1] = "[%s] %s" % (self.hostname, str(e))  
        g_logger.debug("End check ha status")    
            
    def checkClusterService(self):
        try:
            failFlag = "clusterServiceCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t clusterServiceCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                if(status[host[0]] == 0 and output.find(failFlag) == -1):
                    g_logger.debug("End check cluster service")
                    return
                else:
                    g_logger.debug("[%s] Check cluster service on all host failed!" % self.hostname)
            self.healthCheckItems['clusterServiceCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['clusterServiceCheck'][1] = "[%s] Check cluster service on all host failed!" % self.hostname 
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['clusterServiceCheck'][0] = 'Abnormal' 
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['clusterServiceCheck'][1] = "[%s] Check cluster service failed! Error: %s" % (self.hostname, str(e))
        g_logger.debug("End check cluster service")    
    
    def checkQueryPerformance(self):
        try:
            failFlag = "queryPerformanceCheck is failed!"
            activeQueryNum = 0
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):              
                host = []
                cninfo = cnInfos[i]
                if(str(cninfo[0]) == "127.0.0.1"):
                    continue
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t queryPerformanceCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                for node in status.keys():
                    if(status[node] == 0 and outputMap[node].find(failFlag) == -1):
                        resList = outputMap[node].split(":")
                        if(len(resList) != 2):
                            g_logger.debug("The query performance check result is invalid(%s)" % outputMap[node])
                            self.healthCheckItems['queryPerformanceCheck'][0] = 'Abnormal' 
                            self.clusterHealthCheck = "Abnormal"
                            self.healthCheckItems['queryPerformanceCheck'][1] = "[%s] Check query performance failed!" % str(cninfo[0])
                            return
                        else:
                            activeQueryNum = activeQueryNum + int(resList[1].strip()) - 1

                    else:
                        g_logger.debug("Check query performance failed.Error: %s" % outputMap[node])
                        self.healthCheckItems['queryPerformanceCheck'][0] = 'Abnormal' 
                        self.clusterHealthCheck = "Abnormal"
                        self.healthCheckItems['queryPerformanceCheck'][1] = "[%s] Check query performance failed!" % str(cninfo[0])
                        return
            self.healthCheckItems['queryPerformanceCheck'][1] = "active connection num is %s" % str(activeQueryNum)               
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['queryPerformanceCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['queryPerformanceCheck'][1] = "[%s] Check query performance failed! Error: %s" % (self.hostname, str(e))  
        g_logger.debug("End check query performance")    

    def checkLockNum(self):
        try:
            failFlag = "lockNumCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t lockNumCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                for node in status.keys():
                    if(status[node] == 0 and outputMap[node].find(failFlag) == -1):
                        resList = outputMap[node].split(":")
                        if(len(resList) == 2):
                            self.healthCheckItems['lockNumCheck'][1] = "lock num is %s" % resList[1].strip()
                            g_logger.debug("End check lock num")
                            return
                        else:
                            g_logger.debug("Invalid result: %s" % outputMap[node])
                    else:
                        g_logger.debug("Check lock number failed.Error: %s" % outputMap[node])
            raise Exception("Check lock num failed!")
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['lockNumCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['lockNumCheck'][1] = "[%s] Check lock num failed! Error: %s" % (self.hostname, str(e))  
        g_logger.debug("End check lock num")    

    def checkCursorNum(self):
        try:
            failFlag = "cursorNumCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t cursorNumCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                for node in status.keys():
                    if(status[node] == 0 and outputMap[node].find(failFlag) == -1):
                        resList = outputMap[node].split(":")
                        if(len(resList) == 2):
                            self.healthCheckItems['cursorNumCheck'][1] = "cursor num is %s" % resList[1].strip()
                            g_logger.debug("End check cursor num") 
                            return
                        else:
                            g_logger.debug("Invalid result: %s" % outputMap[node])
                    else:
                        g_logger.debug("Check cursor number failed.Error: %s" % outputMap[node])
            raise Exception("Check cursor num failed!")
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['cursorNumCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['cursorNumCheck'][1] = "[%s] Check cursor num failed! Error: %s" % (self.hostname, str(e))  
        g_logger.debug("End check cursor num")     

    def checkConnectionNum(self):
        try:
            failFlag = "connectionNumCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                if(str(cninfo[0]) == "127.0.0.1"):
                    continue
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t connectionNumCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                if(status[host[0]] == 0 and output.find(failFlag) == -1):
                    continue
                else:
                    parRes = ""
                    for node in outputMap.keys():
                        if(outputMap[node].find(failFlag) >= 0):
                            parRes += "%s:\n%s" % (node, outputMap[node])
                    g_logger.debug("%s" % parRes)       
                    self.healthCheckItems['connectionNumCheck'][0] = 'Abnormal'
                    self.clusterHealthCheck = "Abnormal"
                    if(self.healthCheckItems['connectionNumCheck'][1] == 'OK'):
                        self.healthCheckItems['connectionNumCheck'][1] = "%s" % parRes
                    else:
                        self.healthCheckItems['connectionNumCheck'][1] += "%s" % parRes
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['connectionNumCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['connectionNumCheck'][1] += "\n[%s] connectionNumCheck is failed!" % self.hostname       
        g_logger.debug("End check connection num")    

    def checkGaussDBParameter(self):
        try:
            failFlag = "gaussdbParameterCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python  %s/%s -t gaussdbParameterCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                parRes = ""
                for node in outputMap.keys():
                    if(outputMap[node].find(failFlag) >= 0):
                        parRes += "%s:\n%s" % (node, outputMap[node])
                if(status[host[0]] == 0 and output.find(failFlag) == -1):
                    continue
                else:
                    g_logger.debug("%s" % parRes)
                    self.healthCheckItems['gaussdbParameterCheck'][0] = 'Abnormal'
                    self.clusterHealthCheck = "Abnormal"
                    if(self.healthCheckItems['gaussdbParameterCheck'][1] == 'OK'):
                        self.healthCheckItems['gaussdbParameterCheck'][1] = "%s" % parRes
                    else:
                        self.healthCheckItems['gaussdbParameterCheck'][1] += "%s" % parRes                    
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['gaussdbParameterCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['gaussdbParameterCheck'][1] = "%s" % str(e) 
        g_logger.debug("End check gaussdb parameter")    

    def checkInstallDirUsage(self):
        try:
            failFlag = "installDirUsageCheck is failed!"
            cmd = "python %s/%s -t installDirUsageCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['installDirUsageCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['installDirUsageCheck'][1] = "%s" % str(e) 
        g_logger.debug("End check install path usage")    
        


    def checkDataDirUsage(self):
        try:
            failFlag = "dataDirUsageCheck is failed!"
            cmd = "python %s/%s -t dataDirUsageCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['dataDirUsageCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['dataDirUsageCheck'][1] = "%s" % str(e) 
        g_logger.debug("End check data path usage")    

    def checkGaussdbIntegrity(self):
        try:
            failFlag = "gaussdbIntegrityCheck is failed!"
            relativeGaussdbPath = "./bin/gaussdb"
            absoluteGaussdbPath = os.path.join(g_clusterInfo.appPath, relativeGaussdbPath)  
            gaussdbSHA256 = PlatformCommand.getFileSHA256(absoluteGaussdbPath)
            cmd = "python %s/%s -t gaussdbIntegrityCheck -U %s --sha256=%s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user, gaussdbSHA256)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['gaussdbIntegrityCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['gaussdbIntegrityCheck'][1] = "%s" % str(e) 
        g_logger.debug("End check gaussdb integrity")    
       
    def checkDirectoryPermission(self):
        try:
            failFlag = "directoryPermissionCheck is failed!"
            cmd = "python %s/%s -t directoryPermissionCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("Check directory permission failed. Error:%s" % str(e))
            self.healthCheckItems['directoryPermissionCheck'][0] = 'Abnormal'
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['directoryPermissionCheck'][1] = "%s" % str(e)
        g_logger.debug("End check directory permission")  

    def checkGaussDBVersion(self):
        try:
            failFlag = "gaussdbVersionCheck is failed!"  
            postgresVersion = DefaultValue.getAppVersion(g_clusterInfo.appPath)     
            cmd = "python %s/%s -t gaussdbVersionCheck -U %s --gaussdbversion=%s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user, postgresVersion)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['gaussdbVersionCheck'][0] = 'Abnormal'
            self.healthCheckItems['gaussdbVersionCheck'][1] = "%s" % str(e)  
        g_logger.debug("End check gaussdb version")    
                
    def checkDebugSwitch(self):
        try:
            failFlag = "debugSwitchCheck is failed!"
            cmd = "python %s/%s -t debugSwitchCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['debugSwitchCheck'][0] = 'Abnormal'
            self.healthCheckItems['debugSwitchCheck'][1] = "%s" % str(e)
        g_logger.debug("End check debug switch")    
                
    def checkEnvironmentVariables(self):
        try:
            failFlag = "environmentVariablesCheck is failed!" 
            cmd = "python %s/%s -t environmentVariablesCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['environmentVariablesCheck'][0] = 'Abnormal'
            self.healthCheckItems['environmentVariablesCheck'][1] = "%s" % str(e)  
        g_logger.debug("End check environment variables")    
    
    def checkOSVersion(self):
        try:
            failFlag = "osVersionCheck is failed!"
            cmd = "python %s/%s -t osVersionCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['osVersionCheck'][0] = 'Abnormal'
            self.healthCheckItems['osVersionCheck'][1] = "%s" % str(e)  
        g_logger.debug("End check os version")    

    def checkOSKernelParameter(self):
        try:
            cmd = "python %s/%s -t osKernelParameterCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            failFlag = "osKernelParameterCheck is failed!"
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['osKernelParameterCheck'][0] = 'Abnormal'
            self.healthCheckItems['osKernelParameterCheck'][1] = "%s" % str(e)
        g_logger.debug("End check os kernel parameter")    
        
    def checkDBConnection(self):
        try:
            failFlag = "dbConnectionCheck is failed!"
            cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
            for i in range(0, len(cnInfos)):
                host = []
                cninfo = cnInfos[i]
                host.append(cninfo[0])
                port = cninfo[1]
                cmd = "python %s/%s -t dbConnectionCheck --port=%s -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, str(port), g_opts.user)
                (status, output) = g_sshTool.getSshStatusOutput(cmd, host)
                outputMap = g_sshTool.parseSshOutput(host)
                if(status[host[0]] == 0 and output.find(failFlag) == -1):
                    g_logger.debug("End check dbConnection") 
                    return
                else:
                    parRes = ""
                    for node in outputMap.keys():
                        if(outputMap[node].find(failFlag) >= 0):
                            parRes += "%s:\n%s" % (node, outputMap[node])
                    g_logger.debug("%s" % parRes)       
            self.healthCheckItems['dbConnectionCheck'][0] = 'Abnormal' 
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['dbConnectionCheck'][1] = "[%s] Check db connection on all host failed!"  % self.hostname  
   
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.healthCheckItems['dbConnectionCheck'][0] = 'Abnormal' 
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['dbConnectionCheck'][1] = "[%s] Check db connection failed! Error: %s" % (self.hostname, str(e))

        g_logger.debug("End check dbConnection")   
               
    def checkOMMonitor(self):
        try:
            failFlag = "omMonitorCheck is failed!"
            cmd = "python %s/%s -t omMonitorCheck -U %s" % (g_opts.script_dir, LOCAL_HEALTH_CHECK, g_opts.user)
            (status, output) = g_sshTool.getSshStatusOutput(cmd)
            outputMap = g_sshTool.parseSshOutput(g_sshTool.hostNames)
            parRes = ""
            for node in status.keys():
                if(status[node] != 0 or  outputMap[node].find(failFlag) >= 0):
                    parRes += "%s:\n%s" % (node, outputMap[node])
            if(parRes !=""):
                raise Exception("%s" % parRes)
        except Exception, e:
            g_logger.debug("%s" % str(e))
            self.clusterHealthCheck = "Abnormal"
            self.healthCheckItems['omMonitorCheck'][0] = 'Abnormal'
            self.healthCheckItems['omMonitorCheck'][1] = "%s" % str(e)
        g_logger.debug("End check om_monitor thread")    
            
#############################################################################
# Parse and check parameters
#############################################################################
def usage():
    """
GaussOM.py is a utility to manage GaussDB server.

Usage:
  python GaussOM.py --help
  python GaussOM.py -t start [-U user] [-n nodename] [-T time_out] [-l logfile]
  python GaussOM.py -t stop [-U user] [-n nodename] [-m mode] [-l logfile]
  python GaussOM.py -t clean [-U user] [-d] [-l logfile]
  python GaussOM.py -t status [-U user] [-n nodename] [-f output] [-d] [-l logfile]
  python GaussOM.py -t backup [-U user] -P position [-n nodename][-p] [-b] [-a] [-l logfile]
  python GaussOM.py -t restore [-U user] -P position  [-n nodename] [-p] [-b] [-a] [-l logfile]
  python GaussOM.py -t switch [-U user] -n nodename -D datadir [-l logfile]
  python GaussOM.py -t xlogcheck [-U user] -n nodename -D datadir [-l logfile]
  python GaussOM.py -t healthcheck [-U user] [-f output]  [-d] [-l logfile]
  python GaussOM.py -t performancecheck [-U user] [-f output] [-d] [-l logfile]
  python GaussOM.py -t switchquery [-U user] [-l logfile]
  python GaussOM.py -t switchreset [-U user] [-T time_out] [-l logfile]

Common options:
  -t                                the type of om command
  -U                                the user name of cluster
  -n --node=nodename                the node to perform the operation
  -l --logfile=logfile              the log file of operation
  -h --help                         show this help, then exit

Options for start and stop
  -T --timeout=SECS                 time to wait when start cluster or node
  -m --mode=f[ast]|i[mmediate]|s[smart]      stop on fast , immediate or smart mode

Options for clean
  -d --cleandata                    clean instance data file
Options for status
  -d --detail                       show the detail info about status
  -f --output_file                  output the result to specified file

Options for backup and restore
  -P --position=BACKUP_PATH         the directory to backup the application and configurations
  -p --parameter                    backup or restore parameter files only(default)
  -b --binary_file                  backup or restore binary files only
  -a --all                          backup or restore both parameter files and binary files

Options for switch
  -D --datadir=INSTANCE_DATA        the data directory of instance

Options for xlog check
  -D --datadir=INSTANCE_DATA        the data directory of instance

Options for health check
  -f --output_file                  output the result to specified file
  -d --detail                       show the detail info about health check

Options for performance check
  -f --output_file                  output the result to specified file
  -d --detail                       show the detail info about performance check
    """

    print usage.__doc__
 
    
def initGlobal():
    """
    Init logger
    """
    global g_logger
    global g_clusterInfo
    
    try:
        g_logger = GaussLog(g_opts.logFile)

        g_clusterInfo = dbClusterInfo()
        g_clusterInfo.initFromStaticConfig(g_opts.user)
        
        dirName = os.path.dirname(g_opts.logFile)
        g_opts.localLog = os.path.join(dirName, DefaultValue.LOCAL_LOG_FILE)
        g_opts.appPath = g_clusterInfo.appPath
        (g_opts.user, g_opts.group) = PlatformCommand.getPathOwner(g_opts.appPath)
        if (g_opts.user == "" or g_opts.group == ""):
            g_logger.logExit("Get user info  failed!")
    except Exception, e:
        g_logger.logExit(str(e))

def parseCommandLine():
    """
    Parse command line and save to global variable
    """
    try: 
        (opts, args) = getopt.getopt(sys.argv[1:],
        "t:m:n:U:D:T:P:f:l:abc:dph",
        ["node=", "timeout=", "logfile=", "help",
        "mode=",
        "cleandata",
        "output_file=", "detail",
        "position=", "parameter", "binary_file", "all",
        "datadir="])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value      
    
def checkParameter():
    """
    Check parameter from command line
    """
    if (g_opts.action == ""):
        GaussLog.exitWithError("Parameter input error, need '-t' parameter.")
    
    if (g_opts.action == ACTION_START):
        checkStartParameter()
    elif (g_opts.action == ACTION_STOP):
        checkStopParameter()
    elif(g_opts.action == ACTION_HEALTH_CHECK):
        checkHealthCheckParameter()
    elif (g_opts.action == ACTION_CLEAN):
        checkCleanParameter()
    elif (g_opts.action == ACTION_STATUS):
        checkStatusParameter()
    elif (g_opts.action == ACTION_BACKUP or g_opts.action == ACTION_RESTORE):
        checkBackupParameter()
    elif (g_opts.action == ACTION_SWITCH):
        checkSwitchParameter()
    elif (g_opts.action == ACTION_XLOGCHECK):
        checkXLogCheckParameter()
    elif (g_opts.action == ACTION_PERFCHECK):
        checkPerfCheckParameter()
    elif (g_opts.action == ACTION_QUERY_SWITCHOVER):
        checkQuerySwitchover()
    elif (g_opts.action == ACTION_RESET_SWITCHOVER):
        checkResetSwitchover()
    else:
        GaussLog.exitWithError("Invalid Action : %s" % g_opts.action)

    #check mpprc file path
    g_opts.mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
    if(g_opts.mpprcFile == None):
        g_opts.mpprcFile = ""
    if(g_opts.mpprcFile != ""):
        if (not os.path.exists(g_opts.mpprcFile)):
            GaussLog.exitWithError("mpprc file does not exist: %s" % g_opts.mpprcFile)
        if (not os.path.isabs(g_opts.mpprcFile)):
            GaussLog.exitWithError("mpprc file need absolute path:%s" % g_opts.mpprcFile)
    
    # check if user exist and is the right user
    if (g_opts.user == ""):
        g_opts.user = pwd.getpwuid(os.getuid()).pw_name
        if (g_opts.user == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_opts.user)    
    cmd = "id -un" 
    (status, output) = commands.getstatusoutput(cmd)
    if(output != g_opts.user):
        GaussLog.exitWithError("Invalid User : %s" % g_opts.user)        

    # check log file
    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.OM_LOG_FILE, g_opts.user, "")
    if (not os.path.isabs(g_opts.logFile)):
        GaussLog.exitWithError("Parameter input error, log path need absolute path.")

def checkQuerySwitchover():
    """
    Check parameter for query switchover
    """ 
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:l:h", ["logfile=", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for switchquery: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for switchquery: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        if (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for switchquery: %s" % key)

def checkResetSwitchover():
    """
    Check parameter for reset switchover
    """ 
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:T:l:h", ["timeout=", "logfile=", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for switchreset: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for switchreset: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        if (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-T" or key == "--timeout"):
            g_opts.time_out = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for switchreset: %s" % key)
    
    if (g_opts.time_out is None):
        g_opts.time_out = DefaultValue.TIMEOUT_CLUSTER_SWITCHRESET
    else:
        if (not g_opts.time_out.isdigit()):
            GaussLog.exitWithError("Parameter input error, '-T' parameter should be integer.")
        g_opts.time_out = int(g_opts.time_out)

def checkStartParameter():
    """
    Check parameter for start cluster and node
    """ 
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:T:l:h", ["node=", "timeout=", "logfile=", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for start: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for start: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        if (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-T" or key == "--timeout"):
            g_opts.time_out = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for start: %s" % key)
    
    if (g_opts.time_out is None):
        g_opts.time_out = DefaultValue.TIMEOUT_CLUSTER_START
    else:
        if (not g_opts.time_out.isdigit()):
            GaussLog.exitWithError("Parameter input error, '-T' parameter should be integer.")
        g_opts.time_out = int(g_opts.time_out)

    
def checkStopParameter():
    """
    Check parameter for stop clster and node
    """ 
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:m:l:h", ["node=", "mode=", "logfile=", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for stop: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for stop: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-m" or key == "--mode"):
            g_opts.stopMode = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for stop: %s" % key)
            
    if (g_opts.stopMode not in [STOP_MODE_FAST, STOP_MODE_IMMEDIATE,STOP_MODE_SMART ,"f", "i","s"]):
        GaussLog.exitWithError("Parameter input error for stop.Invalid stop mode: %s" % g_opts.stopMode) 
            
def checkHealthCheckParameter():
    """
    Check parameter for health check
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:l:f:dh", ["output_file=", "detail", "logfile=", "help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Parameter input error for health check: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for health check: %s" % str(args[0]))   

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        elif (key == "-f" or key == "--output_file"):
            g_opts.outFile = value
        elif (key == "-d" or key == "--detail"):
            g_opts.show_detail = True
        else:
            usage()
            GaussLog.exitWithError("Unknown parameter for health check: %s" % key)
            
    if (g_opts.outFile != '' and not os.path.isabs(g_opts.outFile)):
        GaussLog.exitWithError("Parameter input error, health check report file need absolute path.")

def checkCleanParameter():
    """
    Check parameter for clean
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:l:hd", ["logfile=", "help","cleandata"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for clean: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for clean: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-d" or key == "--cleandata"):
            g_opts.cleanData = True
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for clean: %s" % key)
    
def checkStatusParameter():
    """
    Check parameter for status
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:f:l:dh", ["node=", "output_file=", "logfile=", "detail", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for status: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for status: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-f" or key == "--output_file"):
            g_opts.outFile = value
        elif (key == "-d" or key == "--detail"):
            g_opts.show_detail = True
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for status: %s" % key)
            
    if (g_opts.outFile != '' and not os.path.isabs(g_opts.outFile)):
        GaussLog.exitWithError("Parameter input error, status check report file need absolute path.")
        
def checkBackupParameter():
    """
    Check parameter for backup or restore
    """  
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:P:l:pbah", ["node=", "position=", "logfile=", "parameter", "binary_file", "all", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for backup or restore: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for backup or restore: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-P" or key == "--position"):
            g_opts.bakDir = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        elif (key == "-p" or key == "--parameter"):
            g_opts.bakParam = True
        elif (key == "-b" or key == "--binary_file"):
            g_opts.bakBin = True
        elif (key == "-a" or key == "--all"):
            g_opts.bakParam = True
            g_opts.bakBin = True
        else:
            GaussLog.exitWithError("Unknown parameter for backup or restore: %s" % key)
    if (g_opts.bakParam == False and g_opts.bakBin == False):
    	g_opts.bakParam = True
            
def checkSwitchParameter():
    """
    Check parameter for switch over
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:D:l:h", ["node=", "datadir=", "logfile=","help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for switch: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for switch: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-D" or key == "--datadir"):
            g_opts.dataDir = os.path.normpath(value)
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for switch: %s" % key)
            
    if (g_opts.nodeName == ""):
        GaussLog.exitWithError("Parameter input error, need '-n' parameter.")
        
    if (g_opts.dataDir == ""):
        GaussLog.exitWithError("Parameter input error, need '-D' parameter.")
    

def checkXLogCheckParameter():
    """
    Check parameter for check
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:n:D:l:h", ["node=", "datadir=", "logfile=","help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for check: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for check: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-n" or key == "--node"):
            g_opts.nodeName = value
        elif (key == "-D" or key == "--datadir"):
            g_opts.dataDir = os.path.normpath(value)
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        else:
            GaussLog.exitWithError("Unknown parameter for check: %s" % key)
            
    if (g_opts.nodeName == ""):
        GaussLog.exitWithError("Parameter input error, need '-n' parameter.")
        
    if (g_opts.dataDir == ""):
        GaussLog.exitWithError("Parameter input error, need '-D' parameter.")  
            
def checkPerfCheckParameter():
    """
    Check parameter for performance check
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:f:l:U:dh", ["file=", "logfile=", "detail", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for perfcheck: %s" % str(e))


    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for perfcheck: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "-f" or key == "--file"):
            g_opts.outFile = value
        elif (key == "-l" or key == "--logfile"):
            g_opts.logFile = value
        elif (key == "-d" or key == "--detail"):
            g_opts.show_detail = True
        else:
            GaussLog.exitWithError("Unknown parameter for perfcheck: %s" % key)

    if (g_opts.outFile != '' and not os.path.isabs(g_opts.outFile)):
        GaussLog.exitWithError("Parameter input error, performance check report file need absolute path.")

def checkTmpDir():
    """
    Check tmp dir
    """
    tmpDir = DefaultValue.getTmpDirFromEnv()
    if(not os.path.exists(tmpDir)):
        raise Exception("The directory(%s) does not exist, please check it!" % tmpDir)
        
def doOperation():
    """
    Do operation
    """
    if (g_opts.action == ACTION_START):
        doStart()
    elif (g_opts.action == ACTION_STOP):
        doStop()
    elif(g_opts.action == ACTION_HEALTH_CHECK):
        doHealthCheck()
    elif (g_opts.action == ACTION_CLEAN):
        doClean()
    elif (g_opts.action == ACTION_STATUS):
        doStatus()
    elif (g_opts.action == ACTION_BACKUP or g_opts.action == ACTION_RESTORE):
        doBackupOrRestore()
    elif (g_opts.action == ACTION_SWITCH):
        doSwitch()
    elif (g_opts.action == ACTION_XLOGCHECK):
        doXLogCheck()
    elif (g_opts.action == ACTION_PERFCHECK):
        doPerfCheck()
    elif (g_opts.action == ACTION_QUERY_SWITCHOVER):
        doQuerySwitchover()
    elif (g_opts.action == ACTION_RESET_SWITCHOVER):
        doResetSwitchover()

def doQuerySwitchover():
    """
    Query switchover
    """
    g_logger.log("Begin operation:switchquery...")

    cmd = ClusterCommand.getQuerySwitchOverCmd(g_opts.user)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("Cmd:%s" % cmd)
        g_logger.logExit("Query switchover failed!Error: %s" % output)

    print("%s" % output)
    g_logger.log("End operation:switchquery")
	
def doResetSwitchover():
    """
    Reset switchover
    """
    g_logger.log("Begin operation:switchreset...")
	
    cmd = ClusterCommand.getResetSwitchOverCmd(g_opts.user, g_opts.time_out)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("Cmd:%s" % cmd)
        g_logger.logExit("Reset switchover failed!Error: %s" %  output)

    print("%s" % output)
    g_logger.log("End operation:switchreset")
        
def doStart():
    """
    Start cluster or node
    """
    g_logger.debug("Begin operation:start...")
    nodeId = 0
    startType = "cluster"
    if (g_opts.nodeName != ""):
        startType = "node"
        dbNode = g_clusterInfo.getDbNodeByName(g_opts.nodeName)
        if(dbNode == None):
            g_logger.log("No node named %s" % g_opts.nodeName)
            sys.exit(1)
        nodeId = dbNode.id
    endTime = None
    if (g_opts.time_out > 0):
        endTime = datetime.now() + timedelta(seconds=g_opts.time_out)
    
    g_logger.log("Begin start %s..." % startType)
    g_logger.log("======================================================================")
    cmd = ClusterCommand.getStartCmd(g_opts.user, nodeId, g_opts.time_out)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.log("Start %s failed!Error: %s" % (startType, output))
        g_logger.log("The cluster may continue to start in the background ...")
        g_logger.log("If you want to see the cluster status, please try command python GaussOM.py -t status ...")
        g_logger.log("If you want to stop the cluster, please try command python GaussOM.py -t stop ...")
        sys.exit(1)

    if (nodeId == 0):
        g_logger.log("Start primary instance successfully, wait for standby instances...")
    else:
        g_logger.log("Start %s successfully" % startType)
        g_logger.log("======================================================================")
        g_logger.log("End start %s..." % startType)
        return
    
    g_logger.log("======================================================================")
    
    dotCount = 0
    startStatus = 0
    startResult = ""
    while True:
        time.sleep(5)
        sys.stdout.write(".")
        dotCount += 1
        if (dotCount >= 12):
            dotCount = 0
            sys.stdout.write("\n")
        
        startStatus = 0
        startResult = ""
        (startStatus, startResult) = OMCommand.doCheckStaus(g_opts.user, nodeId)
        if (startStatus == 0):
            if (dotCount != 0): sys.stdout.write("\n")
            g_logger.log("Start %s successfully" % startType)
            break

        if (endTime is not None and datetime.now() >= endTime):
            if (dotCount != 0): sys.stdout.write("\n")
            g_logger.log("Timeout!Start %s failed in (%s)s!" % (startType, g_opts.time_out))
            g_logger.log("It will continue to start in the background ...")
            g_logger.log("If you want to see the cluster status, please try command python GaussOM.py -t status ...")
            g_logger.log("If you want to stop the cluster, please try command python GaussOM.py -t stop ...")
            break
    g_logger.log("======================================================================")
    g_logger.log(startResult)
    
    g_logger.log("End start %s..." % startType)
    g_logger.debug("End operation:start")

def doStop():
    """
    Stop cluster or node
    """
    g_logger.debug("Begin operation:stop...")
    nodeId = 0
    stopType = "cluster"
    if (g_opts.nodeName != ""):
        stopType = "node"
        dbNode = g_clusterInfo.getDbNodeByName(g_opts.nodeName)
        if(dbNode == None):
            g_logger.log("No node named %s" % g_opts.nodeName)
            sys.exit(1)
        nodeId = dbNode.id
    
    g_logger.log("Begin stop %s..." % stopType)
    g_logger.log("=========================================")
    cmd = ClusterCommand.getStopCmd(g_opts.user, nodeId, g_opts.stopMode)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.log("Stop %s failed!Try to stop forcibly!" % stopType)
        cmd = ClusterCommand.getStopCmd(g_opts.user, nodeId, "i")
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug(output)
            g_logger.log("Stop %s forcibly failed!" % stopType)
        else:
            g_logger.log("Stop %s forcibly successfully!" % stopType)
    else:
        g_logger.log("Stop %s successfully!" % stopType)

    g_logger.log("=========================================")
    g_logger.log("End stop %s..." % stopType)
    g_logger.debug("End operation:stop")
    
def doClean():
    """
    Clean application and instance data
    """
    g_logger.log("Begin operation:clean...")
    if (g_opts.cleanData == True):
        cmd = "python %s -U %s -d" % (OMCommand.Gauss_UnInstall, g_opts.user)
    else:
        cmd = "python %s -U %s" % (OMCommand.Gauss_UnInstall, g_opts.user)
    status = os.system(cmd)
    if (status != 0):
        g_logger.logExit("Do clean failed!")
        
    #Can not write log here, because mabye log file has been removed during uninstall.
    print("End operation:clean")

def doStatus():
    """
    Get the status of cluster or node
    """
    g_logger.debug("Begin operation:status...")
    tmpDir = DefaultValue.getTmpDirFromEnv()
    tmpFile = os.path.join(tmpDir, "gauss_cluster_status.dat")
    cmd = ClusterCommand.getQueryStatusCmd(g_opts.user, 0, tmpFile, True)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
        g_logger.debug("Error:%s" % output)
        g_logger.logExit("Get cluster status failed!")
    
    fp = None
    try:
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(tmpFile)
        output = sys.stdout
        if (g_opts.outFile):
            dirName = os.path.dirname(g_opts.outFile)
            if (not os.path.isdir(dirName)):
                os.makedirs(dirName, 0750)
            fp = open(g_opts.outFile, "w")
            output = fp
        
        if (g_opts.nodeName != ""):
            nodeStatus = clusterStatus.getDbNodeStatusByName(g_opts.nodeName)
            if (nodeStatus is None):
                g_logger.logExit("There is no node named %s" % g_opts.nodeName)
            nodeStatus.outputNodeStatus(output, g_opts.user, g_opts.show_detail)
        else:
            clusterStatus.outputClusterStauts(output, g_opts.user, g_opts.show_detail)
        (status, output) = commands.getstatusoutput("chmod 640 %s" % g_opts.outFile)   
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
    except Exception,e:
        if(fp):fp.close()
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
        g_logger.logExit("Get the status failed!Error: %s" % str(e))

    g_logger.debug("End operation:status.")
            
def doBackupOrRestore():
    """
    do backup or restore operation
    """
    g_logger.debug("Begin operation:BackupOrRestore...")
    bakParam = ""
    if (g_opts.bakParam):
        bakParam = " -p"
    if (g_opts.bakBin):
        bakParam = " -b"
    if (g_opts.bakParam and g_opts.bakBin):
        bakParam = " -a"
    
    cmd = "python %s -t %s -U %s %s" % (OMCommand.Gauss_Backup, g_opts.action, g_opts.user, bakParam)
    if (g_opts.nodeName != ""):
        cmd += " -n %s" % g_opts.nodeName
    if (g_opts.bakDir != ""):
        cmd += " -P %s" % g_opts.bakDir
        
    status = os.system(cmd)
    if (status != 0):
        g_logger.logExit("Do backup or restore failed!")
    g_logger.debug("End operation:BackupOrRestore")    
        
def doConfig():
    """
    Set the parameter of instances
    """
    pass
    
def doSwitch():
    """
    Switch instance to standby
    """
    g_logger.log("Begin operation:switch...")
    dbNode = g_clusterInfo.getDbNodeByName(g_opts.nodeName)
    if(dbNode == None):
        g_logger.log("No node named %s" % g_opts.nodeName)
        sys.exit(1)
        
    nodeId = dbNode.id
    if (nodeId < 1):
        g_logger.logExit("There is no node named %s!" % g_opts.nodeName) 

    foundInstance = False
    for instance in dbNode.datanodes:
        #instance type mayby changed during runing, so we just ignore dummy standby instance here
        if(instance.datadir == g_opts.dataDir and instance.instanceType != DUMMY_STANDBY_INSTANCE):
            foundInstance = True
            break
    if(foundInstance == False):
        g_logger.logExit("There is no datanode instance using data directory %s on %s!" % (g_opts.dataDir, g_opts.nodeName))
        
    cmd = ClusterCommand.getSwitchOverCmd(g_opts.user, nodeId, g_opts.dataDir)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("Switch instance failed!Error: %s" % output)
    
    g_logger.log("End operation:switch")
    
def doXLogCheck():
    """
    Check the ha status
    """
    g_logger.debug("Begin operation:XLogCheck...")
    tmpDir = DefaultValue.getTmpDirFromEnv()
    tmpFile = os.path.join(tmpDir, "gauss_cluster_status.dat")
    cmd = ClusterCommand.getQueryStatusCmd(g_opts.user, 0, tmpFile)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
        g_logger.debug("Error:%s" % output)
        g_logger.logExit("Get cluster status failed!")
    
    fp = None
    try:
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(tmpFile)
        nodeStauts = clusterStatus.getDbNodeStatusByName(g_opts.nodeName)
        if (nodeStauts is None):
            g_logger.logExit("There is no ha status about node[%s]." % g_opts.nodeName)
            
        instStatus = nodeStauts.getInstanceByDir(g_opts.dataDir)
        if (instStatus is None):
            g_logger.logExit("There is no ha status about instance[%s]." % g_opts.dataDir)
            
        if (instStatus.haStatus == ""):
             g_logger.logExit("There is no ha status about instance[%s]." % g_opts.dataDir)

        if (instStatus.type == DbClusterStatus.INSTANCE_TYPE_GTM):
            g_logger.logExit("There is no ha status for gtm")

        if (instStatus.type == DbClusterStatus.INSTANCE_TYPE_COORDINATOR):
            g_logger.logExit("There is no ha status for coodinator")
        
        g_logger.log("%-20s: %s" % ("    HA_status", instStatus.haStatus))
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
    except Exception,e:
        if(fp):fp.close()
        if(os.path.exists(tmpFile)): os.remove(tmpFile)
        g_logger.logExit("Get the status failed!Error: %s" % str(e))
    g_logger.debug("End operation:XLogCheck")    
    
def doPerfCheck():
    g_logger.debug("Begin operation:PerformanceCheck")
    outFile = None
    cnInfos = []
    
    try:
        success = False
        showDetail = ""
        if(g_opts.show_detail):
            showDetail = "-d"
        if(g_opts.outFile != ""):
            dirName = os.path.dirname(g_opts.outFile)
            if (not os.path.isdir(dirName)):
                os.makedirs(dirName, 0750)
            outFile = open(g_opts.outFile, "w")

        cnInfos = ClusterCommand.getCooConnections(g_clusterInfo)
        for cnInfo in cnInfos:
            host = cnInfo[0]
            port = cnInfo[1]
            cmd = "ssh %s -o BatchMode=yes \'" % (str(host))
            if(g_opts.mpprcFile != ""):
                cmd += "source %s;" % g_opts.mpprcFile
            cmd += "python %s/%s -p %s -u %s -c %s %s" % (g_opts.script_dir, UTIL_GAUSS_STAT, g_opts.appPath, g_opts.user, str(port), showDetail)
            cmd += "\'"
            g_logger.debug("execution command %s on (%s:%s)..." % (cmd, str(host), str(port)))
            (status, output) = commands.getstatusoutput(cmd)
            if(status == 0):
                g_logger.debug("collect statistics success on (%s:%s)." % (str(host), str(port)))
                success = True
                if(outFile != None):
                    outFile.write(output)
                    outFile.flush()
                    outFile.close()
                else:
                    print output
                break
            else:
                g_logger.debug("collect statistics failed on (%s:%s). output:\n%s" % (str(host), str(port), output))
        
        if(success == False):
            g_logger.logExit("Collect statistics on all nodes failed.")
        (status, output) = commands.getstatusoutput("chmod 640 %s" % g_opts.outFile)   
    except Exception,e:
        g_logger.logExit("Performance Check failed!Error: %s" % str(e))
    finally:
        if(outFile != None):
            outFile.close()
    g_logger.debug("End operation:PerformanceCheck")
#############################################################################
# Upgrade functions
#############################################################################

def doHealthCheck():
    global g_healthChecker
    global g_sshTool
    fp = None
    g_logger.debug("Begin do health check...")
    try:
        g_sshTool = SshTool(g_clusterInfo.getClusterNodeNames(), g_logger.logFile)
        g_healthChecker = healthCheck()
        if(not os.path.exists(g_clusterInfo.appPath)):
            raise Exception("Local install path(%s) doesn't exist." % g_clusterInfo.appPath)
        (g_healthChecker.user, g_healthChecker.group) = PlatformCommand.getPathOwner(g_clusterInfo.appPath)
        if(g_healthChecker.user == "" or g_healthChecker.group == ""):
            raise Exception("Get user information failed!user:%s group:%s" % (g_healthChecker.user, g_healthChecker.group))
        g_healthChecker.hostname = socket.gethostname()
        
        g_logger.debug("Begin check cluster status...")
        g_healthChecker.checkClusterStatus()
        g_logger.debug("Begin check gaussdb integrity...")
        g_healthChecker.checkGaussdbIntegrity()
        g_logger.debug("Begin check directory permission...")
        g_healthChecker.checkDirectoryPermission()
        g_logger.debug("Begin check install path usage...")
        g_healthChecker.checkInstallDirUsage()
        g_logger.debug("Begin check data path usage...")
        g_healthChecker.checkDataDirUsage()
        g_logger.debug("Begin check gaussdb version...")
        g_healthChecker.checkGaussDBVersion()
        g_logger.debug("Begin check debug switch...")
        g_healthChecker.checkDebugSwitch()
        g_logger.debug("Begin check environment variables...")
        g_healthChecker.checkEnvironmentVariables()
        g_logger.debug("Begin check os version...")
        g_healthChecker.checkOSVersion()
        g_logger.debug("Begin check os kernel parameter...")
        g_healthChecker.checkOSKernelParameter()
        g_logger.debug("Begin check om_monitor thread...")
        g_healthChecker.checkOMMonitor()
        g_logger.debug("Begin check query performance...")
        g_healthChecker.checkQueryPerformance()
        g_logger.debug("Begin check dbConnection ...")
        g_healthChecker.checkDBConnection()
        g_logger.debug("Begin check cluster service...")
        g_healthChecker.checkClusterService()
        g_logger.debug("Begin check lock num...")
        g_healthChecker.checkLockNum()
        g_logger.debug("Begin check cursor num...")
        g_healthChecker.checkCursorNum()
        g_logger.debug("Begin check connection num...")
        g_healthChecker.checkConnectionNum()
        g_logger.debug("Begin check gaussdb parameter...")
        g_healthChecker.checkGaussDBParameter()
        g_logger.debug("Begin check ha status...")
        g_healthChecker.checkHAStatus()
        
        output = sys.stdout
        if (g_opts.outFile):
            dirName = os.path.dirname(g_opts.outFile)
            if (not os.path.isdir(dirName)):
                os.makedirs(dirName, 0750)
            fp = open(g_opts.outFile, "w")
            output = fp
        print >> output, "Health Check Result: %s" % g_healthChecker.clusterHealthCheck
        if(g_opts.show_detail):
            print >> output, "Normal Items"
            for(key, value) in g_healthChecker.healthCheckItems.items():
                if(value[0] == "Normal"):
                    print >> output, "Name: %s\nReason: %s" % (key, value[1]) 
            print >> output, "Abnormal Items"
            for(key, value) in g_healthChecker.healthCheckItems.items():
                if(value[0] == "Abnormal"):
                    print >> output, "Name: %s\nReason: \n%s" % (key, value[1])
        if(fp):
            fp.flush()
            fp.close()
        g_logger.debug("End do health check.")
        (status, output) = commands.getstatusoutput("chmod 640 %s" % g_opts.outFile)
    except Exception, e:
        if(fp):
            fp.flush()
            fp.close()
        g_logger.logExit("Do health check failed:Error:\n%s" % str(e))
    g_logger.debug("End operation:healthcheck")    
    
if __name__ == '__main__':
    """
    main function
    """
    if(os.getgid() == 0):
        GaussLog.exitWithError("Can not use root privilege user to run this script.")
    
    try:
        g_opts = CmdOptions()
        parseCommandLine()
        checkParameter()
        initGlobal()
        checkTmpDir()
        
        doOperation()
        g_logger.closeLog()
    except Exception, e:
        GaussLog.exitWithError("Error: %s" % str(e))
        
    sys.exit(0)
