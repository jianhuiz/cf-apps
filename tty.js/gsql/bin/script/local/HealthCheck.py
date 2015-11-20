#!/usr/bin/env python

'''
Created on 2014-3-19

@author: 
'''
import commands
import getopt
import os
import sys
import socket
import math
import subprocess

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.SshTool import SshTool
from script.util.Common import PlatformCommand, DefaultValue

ACTION_IntegrityCheck = "gaussdbIntegrityCheck"
ACTION_DirectoryPermission = "directoryPermissionCheck"
ACTION_GaussdbVersionCheck = "gaussdbVersionCheck"
ACTION_DebugSwitchCheck = "debugSwitchCheck"
ACTION_EnvironmentParameterCheck = "environmentVariablesCheck"
ACTION_OSVersionCheck = "osVersionCheck"
ACTION_OMMonitorCheck = "omMonitorCheck"
ACTION_QueryPerformanceCheck =  "queryPerformanceCheck"
ACTION_ClusterServiceCheck =  "clusterServiceCheck"
ACTION_DBConnectionCheck = "dbConnectionCheck"
ACTION_OSKernelParameterCheck = "osKernelParameterCheck"
ACTION_LockNumCheck = "lockNumCheck"
ACTION_CursorNumCheck = "cursorNumCheck"
ACTION_InstallDirUsageCheck = "installDirUsageCheck"
ACTION_DataDirUsageCheck = "dataDirUsageCheck"
ACTION_ConnectionNumCheck = "connectionNumCheck"
ACTION_GaussdbParameterCheck = "gaussdbParameterCheck"



GTM_CONF = "gtm.conf"
GTM_PROXY_CONF = "gtm_proxy.conf"
POSTGRESQL_CONF = "postgresql.conf"
CM_SERVER_CONF = "cm_server.conf"
CM_AGENT_CONF = "cm_agent.conf"

#############################################################################
# Global variables
#############################################################################
class CmdOptions():
    script_name = os.path.split(__file__)[-1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    action = ""
    sha256 = ""
    gaussdbversion = ""
    port = ""
    debugswitch = ""
    environmentparameter = ""
    osversion = ""
    configpath = "" 
    logFile = ""
    user = ""
 
g_opts = CmdOptions()
g_logger = None
g_clusterInfo = None
g_healthChecker = None
    
class LocalHealthCheck():
    def __init__(self, sha256 = "", gaussdbversion = ""):
        self.sha256 = sha256
        self.gaussdbversion = gaussdbversion
        self.pid = os.getpid()
        self.user = ""
        self.group = ""
        self.sqlFileName = ""
        self.sqlFileFp = None
        self.nodeInfo = None
        
    def CheckIntegrity(self):
        relativeGaussdbPath = "./bin/gaussdb"
        absoluteGaussdbPath = os.path.join(g_clusterInfo.appPath, relativeGaussdbPath)  
        localGaussdbSHA256 = PlatformCommand.getFileSHA256(absoluteGaussdbPath)
        if(localGaussdbSHA256 != self.sha256):
            g_logger.log("sha256 %s %s are different" % (self.sha256, localGaussdbSHA256))
            g_logger.log("%s is failed!" % g_opts.action)
    
    def CheckDirectoryPermissions(self):
        abnormal_flag = False
        resultList = []
        if(PlatformCommand.checkDirWriteable(g_clusterInfo.appPath, g_healthChecker.user)):
            resultList.append(0)
        else:
            g_logger.log("Can't write into %s." % g_clusterInfo.appPath)
            resultList.append(-1)
        tmpDir = DefaultValue.getTmpDirFromEnv()
        if(PlatformCommand.checkDirWriteable(tmpDir, g_healthChecker.user)):
            resultList.append(0)
        else:
            g_logger.log("Can't write into %s." % g_clusterInfo.appPath)
            resultList.append(-1)
        for inst in self.nodeInfo.cmservers:
            resultList.append(self.__checkSingleDirectoryPermission(inst))
        for inst in self.nodeInfo.cmagents:
            resultList.append(self.__checkSingleDirectoryPermission(inst))           
        for inst in self.nodeInfo.coordinators:
            resultList.append(self.__checkSingleDirectoryPermission(inst))                
        for inst in self.nodeInfo.datanodes:
            resultList.append(self.__checkSingleDirectoryPermission(inst))           
        for inst in self.nodeInfo.gtms:
            resultList.append(self.__checkSingleDirectoryPermission(inst))
        for inst in self.nodeInfo.gtmProxys:
            resultList.append(self.__checkSingleDirectoryPermission(inst))   
        if(-1 in resultList):
            abnormal_flag = True
        if(abnormal_flag == True):
            g_logger.log("%s is failed!" % g_opts.action)
    def __checkSingleDirectoryPermission(self, inst):
        #check data dir
        if(not os.path.exists(inst.datadir)):
            g_logger.log("Data dir(%s) of instance(%s) is not existed." % (inst.datadir, str(inst)))
            return -1
        if(PlatformCommand.checkDirWriteable(inst.datadir, g_healthChecker.user) != True):
            g_logger.log("Can't write into %s." % inst.datadir)
            return -1

        #check xlog dir
        if(inst.instanceRole == INSTANCE_ROLE_DATANODE or inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            xlogDir = "%s/pg_xlog" % inst.datadir
            if(not os.path.exists(xlogDir)):
                g_logger.log("xlog dir(%s) of instance(%s) is not existed." % (xlogDir, str(inst)))
                return -1
            if(PlatformCommand.checkDirWriteable(xlogDir, g_healthChecker.user) != True):
                g_logger.log("Can't write into %s." % xlogDir)
                return -1
        #all check is ok, we will return 0
        return 0
        
    def CheckGaussdbVersion(self):
        localPostgresVersion = DefaultValue.getAppVersion(g_clusterInfo.appPath)
        if(localPostgresVersion != self.gaussdbversion):
            g_logger.log("gaussdbversion %s %s are different" % (self.gaussdbversion, localPostgresVersion))
            g_logger.log("%s is failed!" % g_opts.action)
    
    def CheckDebugSwitch(self):
        abnormal_flag = False    
        resultList = []      
        for inst in self.nodeInfo.cmservers:
            resultList.append(self.__checkSingleParaFile(inst))
        for inst in self.nodeInfo.cmagents:
            resultList.append(self.__checkSingleParaFile(inst))           
        for inst in self.nodeInfo.coordinators:
            resultList.append(self.__checkSingleParaFile(inst))                
        for inst in self.nodeInfo.datanodes:
            resultList.append(self.__checkSingleParaFile(inst))               
        for inst in self.nodeInfo.gtms:
            resultList.append(self.__checkSingleParaFile(inst))
        for inst in self.nodeInfo.gtmProxys:
            resultList.append(self.__checkSingleParaFile(inst))
            
        if(-1 in resultList):
            abnormal_flag = True
        if(abnormal_flag == True):
            g_logger.log("%s is failed!" % g_opts.action)
            
    def __checkSingleParaFile(self, inst):
        if(not os.path.exists(inst.datadir) or len(os.listdir(inst.datadir)) == 0):
            g_logger.log("Data dir(%s) of instance(%s) is not existed or empty." % (inst.datadir, str(inst)))
            return -1
        paraPath = ""
        if(inst.instanceRole == INSTANCE_ROLE_CMSERVER):
            paraPath = os.path.join(inst.datadir, CM_SERVER_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_CMAGENT):
            paraPath = os.path.join(inst.datadir, CM_AGENT_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_GTM):
            paraPath = os.path.join(inst.datadir, GTM_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_GTMPROXY):
            paraPath = os.path.join(inst.datadir, GTM_PROXY_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_COODINATOR):
            paraPath = os.path.join(inst.datadir, POSTGRESQL_CONF)
        elif(inst.instanceRole == INSTANCE_ROLE_DATANODE):
            paraPath = os.path.join(inst.datadir, POSTGRESQL_CONF)
        else:
            g_logger.log("Invalid instance type: %s" % inst.instanceRole) 
            return -1
        if(not os.path.exists(paraPath)):
            g_logger.log("%s does not exists!" % paraPath) 
            return -1
        cmd = "grep 'log_min_messages' %s | awk '{print $3}' | tail -n 1" % paraPath
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            properValueList = ['info','notice','warning', 'error', 'log', 'fatal', 'panic'] 
            if(output == ""):
                g_logger.log("log_min_messages is not found in %s." % paraPath) 
                return -1
            elif (output.lower() in properValueList) == False :
                g_logger.log("The value of log_min_messages(%s) is not proper in %s!" % (output, paraPath))
                return -1
        else:
            g_logger.log("Get log_min_messages value failed!")
            return -1
        return 0
        
    def CheckEnvironmentParameter(self):
        gausshome = self.GetEnvironmentParameterValue("GAUSSHOME")
        ld_library_path = (self.GetEnvironmentParameterValue("LD_LIBRARY_PATH")).split(":")
        path = (self.GetEnvironmentParameterValue("PATH")).split(":")
        gauss_version = self.GetEnvironmentParameterValue("GAUSS_VERSION")
        abnormal_flag = False

        if(gausshome == ""):
            abnormal_flag = True
            g_logger.log("The value of GAUSSHOME is null!")
        libPath = "%s/lib" % gausshome  
        if(libPath not in ld_library_path):
            abnormal_flag = True
            g_logger.log("Can't find gaussdb lib path in LD_LIBRARY_PATH!")  
        binPath = "%s/bin" % gausshome 
        if(binPath not in path):
            abnormal_flag = True
            g_logger.log("Can't find gaussdb bin path in PATH!")
        if(abnormal_flag == True):
            g_logger.log("%s is failed!" % g_opts.action)
    
    def CheckOSVersion(self):
        if(not PlatformCommand.checkOsVersion()):
            g_logger.log("The OS is not SuSE11 64bit!")
            g_logger.log("%s is failed!" % g_opts.action)

    def CheckOSKernelParameter(self):
        isAbnormal = False

        #check file handle number
        fileHandleNum = 0
        if(os.getgid() == 0):
            cmd = """su - %s -c "ulimit -a |  grep -F 'open files' " 2>/dev/null""" % g_opts.user
        else:
            cmd = """ulimit -a |  grep -F 'open files' 2>/dev/null""" 
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            resLines = output.split('\n')
            resList = resLines[0].split(' ')
            fileHandleNum = int(resList[-1].strip())
            if(fileHandleNum < 1000000):
                isAbnormal = True
                g_logger.log("File handle number(%s) is smaller than 1000000." % fileHandleNum)
        else:
            isAbnormal = True
            g_logger.log("Get file handle number failed!Error:%s" % output)
        
        if(isAbnormal):
            g_logger.log("%s is failed!" % g_opts.action)
            
    def GetEnvironmentParameterValue(self, environmentValueName):
        if(os.getgid() == 0):
            cmd = "su - %s -c 'echo $%s' 2>/dev/null" % (g_healthChecker.user, environmentValueName)
        else:
            cmd = "echo $%s 2>/dev/null" % environmentValueName
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            return output
        return ""
        
    def CheckOMMonitor(self):
        pidList = []
        cmd = "pgrep -U %s om_monitor" % g_healthChecker.user
        (status, output) = commands.getstatusoutput(cmd)
        if (status == 0):
            pidList = output.split("\n")
            for pid in pidList:
                if((pid.strip()).isdigit() == True):
                    return           
        g_logger.log("%s is failed!" % g_opts.action)
        g_logger.log("Check om_monitor process failed: %s" % output) 
        
    def CheckQueryPerformance(self):
        resFp = None
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            self.sqlFileName = os.path.join(tmpDir, "query_%s.sql" % self.pid)
            self.sqlFileFp = open(self.sqlFileName, 'w')
            print >> self.sqlFileFp, "SELECT count(query) FROM pg_stat_activity WHERE state != 'idle';"      
            self.sqlFileFp.flush()
            self.sqlFileFp.close()
            os.chmod(self.sqlFileName, 0640)
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            resFp = open(sqlResultFile, 'w+')
            if(os.getgid() == 0):
                cmd = "su - %s -c 'gsql -U %s -h localhost -p %s -d postgres -X -f %s'" % (g_healthChecker.user, 
                                                        g_healthChecker.user, g_opts.port, self.sqlFileName)
            else:
                cmd = "gsql -U %s -h localhost -p %s -d postgres -X -f %s" % ( g_healthChecker.user, g_opts.port, self.sqlFileName)
            p = subprocess.Popen(cmd, stdout = resFp, stderr = subprocess.PIPE, shell=True)
            times = 1
            while (times <= 20):
                ret = p.poll()
                if (ret == 0):
                    break
                time.sleep(1)
                times += 1
            resFp.seek(0)
            sqlResult = resFp.read()
            resFp.close()
            if (times <= 20):
                resList = sqlResult.split('\n')
                if(resList[3].strip() == "(1 row)"):
                    print "queryPerformanceCheck:%s" % resList[2].strip()
                else:
                    raise Exception("Execute sql failed.Error: %s" % sqlResult)
            else:                       
                cmd = "ps -ef | grep -F '%s' | grep -F '%s' | grep -F -v 'su - %s' | grep -v 'grep'| awk '{print $2}' |tail -n 1" % (g_healthChecker.user, self.sqlFileName, g_healthChecker.user)
                (status, output) = commands.getstatusoutput(cmd)
                if (status == 0):
                    pid = output.strip()
                    PlatformCommand.KillProcess(pid)
                raise Exception("Execute sql failed.Error: %s" % sqlResult)   
            if(os.path.exists(self.sqlFileName)): os.remove(self.sqlFileName)                                 
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)                                
        except Exception, e:
            if(self.sqlFileFp != None):
                self.sqlFileFp.close()
            if(resFp != None):
                resFp.close()
            if(os.path.exists(self.sqlFileName)): os.remove(self.sqlFileName)
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))
            
    def CheckClusterService(self):
        sqlResultFile = ""
        try:  
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            self.exec_sql(sqlResultFile, '''start transaction;drop table if exists om_test; drop table if exists om_test; create table om_test(a integer);\
            insert into om_test values(1); delete from om_test where a=1; drop table if exists om_test; commit;''', 
            g_healthChecker.user, g_opts.port, "COMMIT", 60)  
            fp = open(sqlResultFile, 'r')
            sqlResult = fp.read()
            if(sqlResult.find("DROP TABLE") < 0 or sqlResult.find("CREATE TABLE") < 0 or sqlResult.find("INSERT") < 0 or sqlResult.find("DELETE") < 0):
                fp.close()
                raise Exception("Execute sql failed. Error: %s" % sqlResult)
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)          
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)      
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))
            
    def CheckDBConnection(self):
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            self.exec_sql(sqlResultFile, "select pg_sleep(1);", g_healthChecker.user, g_opts.port, "pg_sleep")
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)                       
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def CheckLockNum(self):
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            lockNum = str(self.get_sql_result(sqlResultFile, "select  count(*) from pg_locks;", g_healthChecker.user, g_opts.port))
            print "lockNumCheck:%s" % lockNum
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def CheckCursorNum(self):
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            cursorNum = str(self.get_sql_result(sqlResultFile, "select  count(*) from pg_cursors;", g_healthChecker.user, g_opts.port))
            print "cursorNumCheck:%s" % cursorNum
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def CheckConnectionNum(self):
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            maxConnections = 0
            usedConnections = 0
            #get max connection number
            maxConnections = int(self.get_sql_result(sqlResultFile, "show max_connections;", g_healthChecker.user, g_opts.port))
            #get used connection number
            usedConnections = int(self.get_sql_result(sqlResultFile, "SELECT count(*) FROM pg_stat_activity;", g_healthChecker.user, g_opts.port))

            #check if have used more then 90% connections
            if(maxConnections > 0 and usedConnections > 0):
                if(usedConnections >= maxConnections * 0.9):
                    raise Exception("Current used connections(%s) is more than 90%% of max connections(%s)!" % (usedConnections, maxConnections))
                              
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def CheckGaussdbParameter(self):
        sqlResultFile = ""
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            sqlResultFile = os.path.join(tmpDir, "query_%s.output" % self.pid)
            #get max connection number
            maxConnections = int(self.get_sql_result(sqlResultFile, "show max_connections;", g_healthChecker.user, g_opts.port))
            
            # check sem
            strCmd = "cat /proc/sys/kernel/sem"
            status, output = commands.getstatusoutput(strCmd)
            if (status != 0):
                raise Exception("can not get sem parameters.Error: %s" % output)
            paramList = output.split("\t")
            if(int(paramList[0]) < 17):
                raise Exception("The system limit for the maximum number of semaphores per set (SEMMSL) is too low, current SEMMSL value is: " + 
                paramList[0] + ", please check it!") 
            if(int(paramList[3]) < math.ceil((maxConnections + 150) / 16)):
                raise Exception("The system limit for the maximum number of semaphore sets (SEMMNI) is too low, current SEMMNI value is: " + paramList[3] + ", please check it!")
            if(int(paramList[1]) < math.ceil((maxConnections + 150) / 16) * 17):
                raise Exception("The system limit for the maximum number of semaphores (SEMMNS) is too low, current SEMMNS value is: " + 
                paramList[1] + ", please check it!")

            #get shared_buffers size
            GB = 1 * 1024 * 1024 * 1024
            MB = 1 * 1024 * 1024
            kB = 1 * 1024
            shared_buffers = 0
            
            shared_buffer_size = str(self.get_sql_result(sqlResultFile, "show shared_buffers;", g_healthChecker.user, g_opts.port))
            if((shared_buffer_size[0:-2].isdigit() == True) and cmp(shared_buffer_size[-2:], "GB") == 0):
                shared_buffers = int(shared_buffer_size[0:-2]) * GB
            if((shared_buffer_size[0:-2].isdigit() == True) and cmp(shared_buffer_size[-2:], "MB") == 0):
                shared_buffers = int(shared_buffer_size[0:-2]) * MB
            if((shared_buffer_size[0:-2].isdigit() == True) and cmp(shared_buffer_size[-2:], "kB") == 0):
                shared_buffers = int(shared_buffer_size[0:-2]) * kB
            if((shared_buffer_size[0:-1].isdigit() == True) and cmp(shared_buffer_size[-1:], "B") == 0):
                shared_buffers = int(shared_buffer_size[0:-1])

            # check shared_buffers
            strCmd = "cat /proc/sys/kernel/shmmax"
            status, shmmax = commands.getstatusoutput(strCmd)
            if (status != 0):
                raise Exception("can not get shmmax parameters.")
            strCmd = "cat /proc/sys/kernel/shmall"
            status, shmall = commands.getstatusoutput(strCmd)
            if (status != 0):
                raise Exception("can not get shmall parameters.")
            strCmd = "getconf PAGESIZE"
            status, PAGESIZE = commands.getstatusoutput(strCmd)
            if (status != 0):
                raise Exception("can not get PAGESIZE.")
            if(shared_buffers < 128 * kB):
                raise Exception("shared_buffers should bigger than or equal to 128kB, please check it!")
            if(shared_buffers > int(shmmax)):
                raise Exception("shared_buffers should smaller than shmmax, please check it!")
            if(shared_buffers > int(shmall) * int(PAGESIZE)): 
                raise Exception("shared_buffers should smaller than shmall*PAGESIZE, please check it!")
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
        except Exception, e:
            if(os.path.exists(sqlResultFile)): os.remove(sqlResultFile)
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def exec_sql(self, sqlResultFile, sql, user, port, checkStr, timeOut = 20):
        resFp = None
        try:
            resFp = open(sqlResultFile, 'w+')
            if(os.getgid() == 0):
                cmd = "su - %s -c 'gsql -h localhost -p %s -d postgres -X -c \"%s\"' 2>/dev/null" % (user, port, sql)
            else:
                cmd = "gsql -h localhost -p %s -d postgres -X -c \"%s\" 2>/dev/null" % (port, sql)
            p = subprocess.Popen(cmd, stdout = resFp, stderr = subprocess.PIPE, shell=True)
            times = 1
            while (times <= timeOut):
                ret = p.poll()
                if (ret == 0):
                    break
                time.sleep(1)
                times += 1
            resFp.seek(0)
            sqlResult = resFp.read()
            resFp.close()
            if (times <= timeOut and sqlResult.find(checkStr) < 0):
                raise Exception("Execute sql failed.Error: %s" % sqlResult)
            if (times > timeOut):
                cmd = "ps -ef | grep -F '%s' | grep -F '%s' | grep -F -v 'su - %s' | grep -v 'grep'| awk '{print $2}' |tail -n 1" % (user, sql, user)
                (status, output) = commands.getstatusoutput(cmd)
                if (status == 0):
                    pid = output.strip()
                    PlatformCommand.KillProcess(pid)
                raise Exception("Execute sql failed. Error: %s" % sqlResult)
        except Exception, e:
            if(resFp): resFp.close()
            raise Exception("Execute sql failed.Error: %s" % str(e))

    def get_sql_result(self, sqlResultFile, sql, user, port, timeOut = 20):
        resFp = None
        returnValue = None
        try:
            resFp = open(sqlResultFile, 'w+')
            if(os.getgid() == 0):
                cmd = "su - %s -c 'gsql -h localhost -p %s -d postgres -X -c \"%s\"' 2>/dev/null" % (user, port, sql)
            else:
                cmd = "gsql -h localhost -p %s -d postgres -X -c \"%s\" 2>/dev/null" % (port, sql)
            p = subprocess.Popen(cmd, stdout = resFp, stderr = subprocess.PIPE, shell=True)
            times = 1
            while (times <= timeOut):
                ret = p.poll()
                if (ret == 0):
                    break
                time.sleep(1)
                times += 1
            resFp.seek(0)
            sqlResult = resFp.read()
            resFp.close()
            if (times <= timeOut):
                resList = sqlResult.split('\n')
                if(resList[3].strip() == "(1 row)"):
                    returnValue = resList[2].strip()
                else:
                    raise Exception("Execute sql failed.Error: %s" % sqlResult)
            else:                       
                cmd = "ps -ef | grep -F '%s' | grep -F '%s' | grep -F -v 'su - %s' | grep -F -v 'grep'| awk '{print $2}' |tail -n 1" % (user, sql, user)
                (status, output) = commands.getstatusoutput(cmd)
                if (status == 0):
                    pid = output.strip()
                    PlatformCommand.KillProcess(pid)
                raise Exception("Execute sql failed.Error: %s" % sqlResult)
            return returnValue
        except Exception, e:
            if(resFp): resFp.close()
            raise Exception("Execute sql failed.Error: %s" % str(e))

    def CheckInstallDirUsage(self):
        try:
            if(self.__checkSingleDirUsage(g_clusterInfo.appPath) == False):
                raise Exception("%s is failed!" % g_opts.action)
        except Exception, e:
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def CheckDataDirUsage(self):
        resultList = []
        try:
            for inst in self.nodeInfo.cmservers:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))
            for inst in self.nodeInfo.cmagents:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))           
            for inst in self.nodeInfo.coordinators:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))                
            for inst in self.nodeInfo.datanodes:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))           
            for inst in self.nodeInfo.gtms:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))
            for inst in self.nodeInfo.gtmProxys:
                resultList.append(self.__checkSingleDirUsage(inst.datadir))   
            if(False in resultList):
                raise Exception("%s is failed!" % g_opts.action)
        except Exception, e:
            g_logger.log("%s is failed!Error:%s" % (g_opts.action,str(e)))

    def __checkSingleDirUsage(self, path, percentage = 90):
        cmd = "df -P -B 1048576 %s | awk '{print $5}' | tail -1" % path
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            resList = output.split('\n')
            rateNum = int(resList[0].split('%')[0])
            if(rateNum >= percentage):
                g_logger.log("Check path(%s) usage failed!Error: the usage(%s%%) is not less than (%s%%)" % (path, rateNum, percentage))
                return False
        else:
            g_logger.log("Check path(%s) usage failed!Error: %s" % (path, output))
            return False
        return True

            
def usage():
    """
HealthCheck.py is a utility to check if GaussDB is healthy. 
Internal use only. 
    """
    print usage.__doc__

def exitWithError(msg, status = 1):
    sys.stderr.write("%s\n" % msg)
    sys.exit(1)

def initGlobal():
    """
    Init global variables
    """
    global g_logger
    global g_clusterInfo
    global g_healthChecker
    
    try:
        g_logger = GaussLog(g_opts.logFile)
        g_clusterInfo = dbClusterInfo()
        g_clusterInfo.initFromStaticConfig(g_opts.user)
        g_healthChecker = LocalHealthCheck(sha256 = g_opts.sha256, gaussdbversion = g_opts.gaussdbversion)
        if(not os.path.exists(g_clusterInfo.appPath)):
            raise Exception("Local install path(%s) doesn't exist." % g_clusterInfo.appPath)
        cmd = "stat -c '%%U:%%G' %s" % g_clusterInfo.appPath
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("Get user info failed: %s" % output)
        userInfo = output.split(":")
        if (len(userInfo) != 2):
            raise Exception("Get user info failed: %s" % output)
        g_healthChecker.user = userInfo[0]
        g_healthChecker.group = userInfo[1]
        hostName = socket.gethostname()
        g_healthChecker.nodeInfo = g_clusterInfo.getDbNodeByName(hostName)

    except Exception, e:
        g_logger.logExit(str(e))
        
def parseCommandLine():
    """
    Parse command line and save to global variable
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:l:", ["sha256=", "gaussdbversion=", "port=", "help"])
    except Exception, e:
        usage()
        exitWithError("Error: %s" % str(e))

    if(len(args) > 0):
        exitWithError("Parameter input error: %s" % str(args[0]))

    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "--sha256"):
            g_opts.sha256 = value
        elif (key == "--gaussdbversion"):
            g_opts.gaussdbversion = value
        elif (key == "--port"):
            g_opts.port = value
        elif (key == "-U"):
            g_opts.user = value
        elif(key == "-l"):
            g_opts.logFile = value
        else:
            exitWithError("Parameter input error: %s" % key)
            
def checkParameter():
    """
    Check parameter from command line
    """       
    # check if user exist and is the right user
    PlatformCommand.checkUser(g_opts.user)

    # check log file
    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE, g_opts.user, "")
    if (not os.path.isabs(g_opts.logFile)):
        GaussLog.exitWithError("Parameter input error, log path need absolute path.")
         
    if (g_opts.action == ACTION_IntegrityCheck):
        if(g_opts.sha256 == ""):
            exitWithError("Parameter input error, need '--sha256' parameter.")
    elif (g_opts.action == ACTION_GaussdbVersionCheck):
        if(g_opts.gaussdbversion == ""):
            exitWithError("Parameter input error, need '--gaussdbversion' parameter.")
    elif(g_opts.action == ACTION_QueryPerformanceCheck or g_opts.action == ACTION_ClusterServiceCheck or 
         g_opts.action == ACTION_DBConnectionCheck):
        if(g_opts.port == ""):
            exitWithError("Parameter input error, need '--port' parameter.")

def checkTmpDir():
    """
    Check tmp dir
    """
    tmpDir = DefaultValue.getTmpDirFromEnv()
    if(not os.path.exists(tmpDir)):
        raise Exception("The directory(%s) does not exist, please check it!" % tmpDir)
            
        
def doManage():
    """
    """
    if (g_opts.action == ACTION_IntegrityCheck):
        g_healthChecker.CheckIntegrity()
    elif (g_opts.action == ACTION_GaussdbVersionCheck):
        g_healthChecker.CheckGaussdbVersion()
    elif(g_opts.action == ACTION_DebugSwitchCheck):
        g_healthChecker.CheckDebugSwitch()
    elif(g_opts.action == ACTION_EnvironmentParameterCheck):
        g_healthChecker.CheckEnvironmentParameter()
    elif(g_opts.action == ACTION_OSVersionCheck):
        g_healthChecker.CheckOSVersion()  
    elif(g_opts.action == ACTION_OSKernelParameterCheck):
        g_healthChecker.CheckOSKernelParameter()
    elif(g_opts.action == ACTION_DirectoryPermission):
        g_healthChecker.CheckDirectoryPermissions()
    elif(g_opts.action == ACTION_OMMonitorCheck):
        g_healthChecker.CheckOMMonitor()
    elif(g_opts.action == ACTION_QueryPerformanceCheck):
        g_healthChecker.CheckQueryPerformance()
    elif(g_opts.action == ACTION_ClusterServiceCheck):
        g_healthChecker.CheckClusterService()
    elif(g_opts.action == ACTION_DBConnectionCheck):
        g_healthChecker.CheckDBConnection()
    elif(g_opts.action == ACTION_InstallDirUsageCheck):
        g_healthChecker.CheckInstallDirUsage()
    elif(g_opts.action == ACTION_DataDirUsageCheck):
        g_healthChecker.CheckDataDirUsage()
    elif(g_opts.action == ACTION_CursorNumCheck):
        g_healthChecker.CheckCursorNum()
    elif(g_opts.action == ACTION_LockNumCheck):
        g_healthChecker.CheckLockNum()
    elif(g_opts.action == ACTION_ConnectionNumCheck):
        g_healthChecker.CheckConnectionNum()
    elif(g_opts.action == ACTION_GaussdbParameterCheck):
        g_healthChecker.CheckGaussdbParameter()
    else:
        exitWithError("Invalid Action : %s" % g_opts.action)
        
if __name__ == '__main__':
    """
    main function
    """
    try:
        parseCommandLine()
        checkParameter()
        initGlobal() 
        checkTmpDir()
        doManage()
    
    except Exception, e:
        GaussLog.exitWithError("Error: %s" % str(e))
        
    g_logger.closeLog()
    sys.exit(0)                
                    
