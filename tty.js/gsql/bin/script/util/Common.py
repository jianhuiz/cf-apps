'''
Created on 2014-3-3

@author: 
'''

import commands
import hashlib
import os
import platform
import socket
import sys
from script.util.DbClusterInfo import *


class VersionInfo():
    """
    Info about current version
    """
    __PACKAGE_VERSION = ""
    __COMPATIBLE_VERSION = []
    
    @staticmethod
    def getPackageVersion():
        """
        Get version from version.cfg
        """
        if (VersionInfo.__PACKAGE_VERSION != ""):
            return VersionInfo.__PACKAGE_VERSION
        
        dirName = os.path.dirname(os.path.abspath(__file__))
        verionFile = os.path.join(dirName, "./../../", "version.cfg")
        verionFile = os.path.normpath(verionFile)
        if (not os.path.isfile(verionFile)):
            raise Exception("There is no version file[%s]!" % verionFile)
        
        strVersion = PlatformCommand.readFileLine(verionFile)
        infoList = strVersion.split("-")
        if (len(infoList) != 3):
            raise Exception("Get version failed!Version string: %s" % strVersion)
        
        VersionInfo.__PACKAGE_VERSION = infoList[2]
        
        return VersionInfo.__PACKAGE_VERSION
        
    @staticmethod
    def getCompatibleVersion():
        """
        Get compatible version for upgrade
        """
        if (len(VersionInfo.__COMPATIBLE_VERSION) != 0):
            return VersionInfo.__COMPATIBLE_VERSION
        
        # TODO: get this from a file
        curentVersion = VersionInfo.getPackageVersion()
        VersionInfo.__COMPATIBLE_VERSION = [curentVersion]
        
        return VersionInfo.__COMPATIBLE_VERSION
    
class DefaultValue():
    """
    """
    TIMEOUT_CLUSTER_START = 300
    TIMEOUT_CLUSTER_STOP = 300
    TIMEOUT_CLUSTER_FAILOVER = 300
    TIMEOUT_CLUSTER_SYNC = 1800
    TIMEOUT_CLUSTER_SWITCHRESET = 300
    
    DIRECTORY_MODE = 0750
    APP_DISK_SIZE = 100
    INSTANCE_DISK_SIZE = 200

    MPPRC_FILE_ENV = "MPPDB_ENV_SEPARATE_PATH"
    MPPDB_TMP_PATH_ENV = "PGHOST"
    TOOL_PATH_ENV = "GPHOME"

    TABLESPACE_VERSION_DIRECTORY = "PG_9.2_201412021"
    
    GAUSSDB_DIR = "/var/log/gaussdb"
    
    GURRENT_DIR_FILE = "."
    OM_MONITOR_DIR_FILE = "../cm/om_monitor"
    DEFAULT_LOG_FILE = "gaussdb.log"
    LOCAL_LOG_FILE = "gaussdb_local.log"
    PREINSTALL_LOG_FILE = "gaussdb_preinstall.log"
    DEPLOY_LOG_FILE = "gaussdb_deploy.log"
    REPLACE_LOG_FILE = "gaussdb_replace.log"
    UNINSTALL_LOG_FILE = "gaussdb_uninstall.log"
    OM_LOG_FILE = "gaussdb_om.log"
    UPGRADE_LOG_FILE = "gaussdb_upgrade.log"
    DILATAION_LOG_FILE = "gaussdb_dilatation.log"
    UNPREINSTALL_LOG_FILE = "gaussdb_unpreinstall.log"
    GSROACH_LOG_FILE = "gaussdb_roach.log"
    

    COO_CONNECTION_FILE = "gauss_connection.info"
    CLUSTER_LOCK_PID = "gauss_cluster_lock.pid"
    SCHEMA_COORDINATOR = "schema_coordinator.sql"
    SCHEMA_DATANODE = "schema_datanode.sql"
    
    CLUSTER_CONFIG_PATH = "/opt/huawei/wisequery/clusterconfig.xml"
    ALARM_COMPONENT_PATH = "/opt/huawei/snas/bin/snas_cm_cmd"
    CLUSTER_TOOL_PATH = "/opt/huawei/wisequery"

    FIREWALL_CONFIG_FILE = '/etc/sysconfig/SuSEfirewall2'

    @staticmethod
    def getOSKernelParameterList():
        """
        Get os kernel parameter list
        """
        KernelParameterList = {
                        'net.ipv4.tcp_max_tw_buckets':10000,
                        'net.ipv4.tcp_tw_reuse':1,
                        'net.ipv4.tcp_tw_recycle':1,
                        'net.ipv4.tcp_fin_timeout':20,
                        'net.ipv4.tcp_keepalive_time':30,
                        'net.ipv4.tcp_keepalive_probes':3,
                        'net.ipv4.tcp_keepalive_intvl':30,
                        'net.ipv4.tcp_retries2':5
                        }
        return KernelParameterList
    
    @staticmethod
    def getBinFilePath():
        """
        Check operator system version and install binary file version.
        """
        distname, version, id = platform.dist()
        bits, linkage = platform.architecture()
        dirName = os.path.dirname(os.path.abspath(__file__))
        binPath = os.path.join(dirName, "./../../", "Gauss200-OLAP-%s-%s%s-%s.bin" % (VersionInfo.getPackageVersion(), distname.upper(), version, bits))
        binPath = os.path.normpath(binPath)
        if(not os.path.isfile(binPath)):
            return ""
        
        return binPath
    
    @staticmethod
    def getSHA256FilePath():
        """
        Check operator system version and install binary file version.
        """
        distname, version, id = platform.dist()
        bits, linkage = platform.architecture()
        dirName = os.path.dirname(os.path.abspath(__file__))
        sha256Path = os.path.join(dirName, "./../../", "Gauss200-OLAP-%s-%s%s-%s.sha256" % (VersionInfo.getPackageVersion(), distname.upper(), version, bits))
        sha256Path = os.path.normpath(sha256Path)
        if(not os.path.isfile(sha256Path)):
            return ""
        
        return sha256Path
    
    @staticmethod
    def getInstallDir(user):
        """
        get the install dir for user
        """
        gaussHome = ""
        if(os.getgid() == 0):
            cmd = "su - %s -c 'echo $GAUSSHOME' 2>/dev/null" % user
        else:
            cmd = "echo $GAUSSHOME 2>/dev/null" 
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):  
            gaussHome = output.strip()
        return gaussHome
    
    @staticmethod
    def getTmpDir(user, xml_path):
        """
        Get the tmp dir for user
        """
        return dbClusterInfo.readClusterTmpMppdbPath(user, xml_path)
        
    @staticmethod
    def getTmpDirFromEnv():
        """
        Get the tmp dir from PGHOST
        """
        tmpDir = os.getenv(DefaultValue.MPPDB_TMP_PATH_ENV)
        if(tmpDir == ""):
            msg = "os env PGHOST is empty"
            sys.stderr.write("%s\n" % msg)
            sys.exit(1)
        return tmpDir
    
    @staticmethod
    def getTmpDirAppendMppdb(user):
        """
        """
        tmpDir = DefaultValue.getTmpDirFromEnv()
        forbidenTmpDir = "/tmp/%s" % user
        if(tmpDir == forbidenTmpDir):
            tmpDir = "/tmp/%s_mppdb" % user

        return tmpDir
    
    @staticmethod
    def getWarningFilePath(user):
        """
        Get the warning file path fro user
        """
        warningPath = DefaultValue.getTmpDirFromEnv()
        if(warningPath != ""):
            warningPath = DefaultValue.getTmpDirAppendMppdb(user)
        return "%s/warningMsg" % warningPath
   
    @staticmethod
    def getUserFromXml(xml_path):
        try:
            bin_path = dbClusterInfo.readClusterAppPath(xml_path)
            user = PlatformCommand.getPathOwner(bin_path)[0]
        except:
            user = ""
            
        return user

    @staticmethod
    def getEnvironmentParameterValue(environmentParameterName, user):
        mpprcFile = os.getenv('DefaultValue.MPPRC_FILE_ENV')
        if(mpprcFile != "" and mpprcFile != None):
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

    @staticmethod
    def getClusterToolPath():
        """
        get the value of cluster tool path. should get a value, if 
        the value is None or null, then there must be something wrong
        """
        mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(mpprcFile != "" and mpprcFile != None):
            etcProfile = mpprcFile
        else:
            etcProfile = "/etc/profile"

        cmd = "source %s;echo $%s 2>/dev/null" % (etcProfile, DefaultValue.TOOL_PATH_ENV)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("Get environment parameter value failed. Error:%s" % output)   
        
        clusterToolPath = output.split("\n")[0]
        if(clusterToolPath == None or clusterToolPath == ""):
            raise Exception("the value[%s] of environment parameter[%s] is invalid." % (clusterToolPath, DefaultValue.TOOL_PATH_ENV))

        return clusterToolPath


    @staticmethod
    def getUserLogDirWithUser(user): 
        log_path = ""
        try:
            log_path = DefaultValue.getEnvironmentParameterValue("GAUSSLOG", user)
        except:
            log_path = "%s/%s" % (DefaultValue.GAUSSDB_DIR, user)
        return log_path
        
    
    @staticmethod
    def getOMLogPath(logName, user = "", appPath = "", xml = ""):
        logPath = ""
        try:
            if(user != "" and xml != ""):
                logPath = "%s" % dbClusterInfo.readClusterLogPath(xml)
                path = "%s/%s/om/%s" % (logPath, user, logName)
            elif(user != ""):
                logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)
            elif(appPath != ""):
                user = PlatformCommand.getPathOwner(appPath)[0]
                if(user == ""):
                    user = "."
                if(user == "."):
                    logPath = DefaultValue.GAUSSDB_DIR
                else:
                    logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)
            elif(xml != ""):
                try:
                    appPath = dbClusterInfo.readClusterAppPath(xml)
                    user = PlatformCommand.getPathOwner(appPath)[0]
                except:
                    user = "." 
                if(user == ""):
                    user = "."
                if(user == "."):
                    logPath = DefaultValue.GAUSSDB_DIR
                else:
                    logPath = DefaultValue.getUserLogDirWithUser(user)
                path = "%s/om/%s" % (logPath, logName)  
            else:
                logPath = DefaultValue.GAUSSDB_DIR
                path = "%s/om/%s" % (logPath, logName)      
        except:
            logPath = DefaultValue.GAUSSDB_DIR
            path = "%s/om/%s" % (logPath, DefaultValue.DEFAULT_LOG_FILE)
            
        return os.path.abspath(path)
        
    @staticmethod
    def getBackupDir(subDir=""):
        """
        Get default backup directory for upgrade
        """
        
        bakDir = "%s/backup" % DefaultValue.getClusterToolPath()
        if (subDir != ""):
            bakDir = os.path.join(bakDir, subDir)
        
        return bakDir

    @staticmethod
    def getAppVersion(appPath=""):
        """
        Get the version of application
        """
        (user, group) = PlatformCommand.getPathOwner(appPath)
        if (user == "" or group == ""):
            return ""
        
        if(os.getgid() == 0):
            cmd = "su - %s -c 'echo $GAUSS_VERSION' 2>/dev/null" % user
        else:
            cmd = "echo $GAUSS_VERSION 2>/dev/null"
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return ""
        
        return output.strip()

class PlatformCommand():
    """
    Command for os
    """
    
    @staticmethod
    def getPathOwner(pathName):
        user = ""
        group = ""
        
        if (not os.path.exists(pathName)):
            return (user, group)
        
        cmd = "stat -c '%%U:%%G' %s" % pathName
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return (user, group)
        
        userInfo = output.split(":")
        if (len(userInfo) != 2):
            return (user, group)
        
        user = userInfo[0].strip()
        group = userInfo[1].strip()
    
        return (user, group)
    
    @staticmethod
    def checkOsVersion():
        """
        Check os version
        """
        distname, version, id = platform.dist()
        bits, linkage = platform.architecture()
        if(distname == "SuSE" and version == "11" and bits == ("64bit")):
            return True
        else:
            return False

    @staticmethod
    def findUnsupportedParameters(parameterList):
        """
        find unsupported config parameters, just ignore other invalid parameters
        if not find any unsupported config parameter, return [].
        """
        unsupportedArgs = ["support_extended_features"]

        inputedUnsupportedParameters = []
        for param in parameterList:
            keyValue = param.split("=")
            if (len(keyValue) != 2):
                continue
            if (keyValue[0].strip() in unsupportedArgs):
                inputedUnsupportedParameters.append(param)
                
        return inputedUnsupportedParameters

    @staticmethod
    def checkUser(user, strict = True):
        """
        Check if user exists and if is the right user
        """
        cmd = "id -gn '%s'" % user
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("User[%s] does not exist!Output: %s" % (user, output))
            
        if (not strict):
            return
        mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(mpprcFile != "" and mpprcFile != None):
            gaussHome = os.getenv("GAUSSHOME")
        else:
            if(os.getgid() == 0):
                cmd = "su - %s -c 'echo $GAUSSHOME' 2>/dev/null" % user
            else:
                cmd = "echo $GAUSSHOME 2>/dev/null"
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("Get environment parameter value failed. Error:%s" % output)
            gaussHome = output.split("\n")[0]
            
        if(gaussHome == ""):
            raise Exception("The install path of designated user (%s) does not exist, maybe the user is not right!" % user)

    @staticmethod
    def getFilesType(givenPath):
        """
        get the file and subdirectory type of given path
        """
        if(not os.path.exists(givenPath)):
            raise Exception("the path[%s] does not exist." % givenPath)

        tmpFile = "/tmp/fileList_%d" % os.getpid()
        cmd = "find %s | xargs file -F '::' > %s 2>/dev/null" % (givenPath, tmpFile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("get file type failed: %s" % output)

        fp = None
        resDict = {}
        fileNameTypeList = []
        try:
            fp = open(tmpFile)
            fileNameTypeList = fp.readlines()
            fp.close()
            os.remove(tmpFile)
            for oneItem in fileNameTypeList:
                res = oneItem.split("::")
                if(len(res) != 2):
                    continue
                else:
                    resDict[res[0]] = res[1]
            return resDict
        except Exception, e:
            if(fp != None): 
                fp.close()
            if(os.path.exists(tmpFile)):
                commands.getstatusoutput("rm -rf %s" % tmpFile)
            raise Exception("get file type failed: %s" % output)
    

    @staticmethod
    def checkPreInstallFlag(user):
        """
        check if have called preinstall.py script
        """
        cmd = "echo $GAUSS_ENV 2>/dev/null"
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            return False
        else:
            return True

    @staticmethod
    def getPIDofUser(user):
        """
        Get the pid of user
        """
        pidList = []
        
        cmd = "ps h U %s -o pid" % user
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return pidList
        
        pidList = output.split("\n")
        
        return pidList
    
    @staticmethod
    def KillProcess(pid):
        """
        Kill the process
        """
        cmd = "kill -9 %s >/dev/null 2>&1" % str(pid)
        status = os.system(cmd)
        if (status != 0):
            return False
        
        return True
        
    @staticmethod
    def checkDirWriteable(dirPath, user):
        """
        Check if target directory is writeable for user
        """
        testFile = os.path.join(dirPath, "touch.tst")
        cmd = "touch %s >/dev/null 2>&1" % testFile
        status = os.system(cmd)
        if (status != 0):
            return False
        
        cmd = "rm -f %s >/dev/null 2>&1" % testFile
        status = os.system(cmd)
        if (status != 0):
            return False
        
        return True
        
    @staticmethod
    def getFileSHA256(srcFile):
        if (not os.path.isfile(srcFile)):
            raise Exception("There is no file[%s]!" % srcFile)
        strSHA256 = ""
        cmd = "sha256sum %s | awk -F\" \" '{print $1}' " % srcFile
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return strSHA256
        strSHA256 = output.strip()
        
        return strSHA256
    
    @staticmethod
    def readFileLine(srcFile):
        """
        Read content of file
        """
        strRead = ""
        fp = None
        try:
            fp = open(srcFile, "r")
            strRead = fp.readline()
            strRead = strRead.strip()
            fp.close()
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Read file line failed!Error: %s." % str(e))
        
        return strRead
    
    @staticmethod
    def WriteInfoToFile(filePath, info, append=False):
        """
        Write info to the file
        """
        fp = None
        try:
            mode = "a" if append else "w"
            fp = open(filePath, mode)
            fp.write(info)
            fp.flush()
            fp.close()
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Write info to file failed!Error: %s." % str(e))
    @staticmethod    
    def cleanTmpFile(path, fp = None):
        """
        close and remove tmp file
        """
        if(fp): fp.close()
        if(os.path.exists(path)):
            os.remove(path)

    @staticmethod
    def distributeEncryptFiles(appPath, hostList):
        """
        distribute encrypt files server.key.cipher server.key.rand to remote host
        """
        binPath = "%s/bin" % appPath
        encryptFile1 = "%s/server.key.cipher" % binPath
        encryptFile2 = "%s/server.key.rand" % binPath
        if(not os.path.exists(encryptFile1) or not os.path.exists(encryptFile2)):
            raise Exception("encrypt files do not exist, please check it!")

        (user, group) = PlatformCommand.getPathOwner(appPath)
        if (user == "" or group == ""):
            raise Exception("Get user info failed!")

        for host in hostList:
            cmd = "scp %s %s %s:%s; ssh %s 'chown %s:%s %s %s;chmod 640 %s %s'" % (encryptFile1, encryptFile2, host, binPath, host, user, group, encryptFile1, encryptFile2, encryptFile1, encryptFile2)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("scp encrypt files to remote host failed!Error:%s" % output)    
    
    @staticmethod
    def getTopPathNotExist(topDirPath):
        tmpDir = topDirPath
        while True:
            #find the top path to be created
            (tmpDir, topDirName) = os.path.split(tmpDir)
            if (os.path.exists(tmpDir) or topDirName == ""):
                tmpDir = os.path.join(tmpDir, topDirName)
                break
        return tmpDir

       
class ClusterCommand():
    @staticmethod
    def getStartCmd(user, nodeId=0, timeout=DefaultValue.TIMEOUT_CLUSTER_START, datadir=""):
        """
        Start all cluster or a node
        """
        cmd = "cm_ctl start"
        if (nodeId > 0):
            cmd += " -n %d" % nodeId
        
        if (datadir != ""):
            cmd += " -D %s" % datadir
        
        if (timeout > 0):
            cmd += " -t %d" % timeout
        
        return cmd
     
    @staticmethod
    def getStopCmd(user, nodeId=0, stopMode="", timeout=DefaultValue.TIMEOUT_CLUSTER_STOP, datadir=""):
        """
        Stop all cluster or a node
        """
        cmd = "cm_ctl stop"
        if (nodeId > 0):
            cmd += " -n %d" % nodeId
            
        if (datadir != ""):
            cmd += " -D %s" % datadir
            
        if (stopMode != ""):
            cmd += " -m %s" % stopMode
        
        if (timeout > 0):
            cmd += " -t %d" % timeout
        
        
        return cmd
    
    @staticmethod
    def getSwitchOverCmd(user, nodeId, datadir):
        """
        Switch over standby instance
        """
        cmd = "cm_ctl switchover -n %d -D %s" % (nodeId, datadir)
        
        if (user != "") and (os.getgid() == 0):
            cmd = "su - %s -c '%s'" % (user, cmd)
 
        return cmd
    
    @staticmethod
    def getQuerySwitchOverCmd(user):
        """
        Query Switch over
        """
        cmd = "cm_ctl query -v -C -s"
        
        if (user != "") and (os.getgid() == 0):
            cmd = "su - %s -c '%s'" % (user, cmd)
 
        return cmd

    @staticmethod
    def getResetSwitchOverCmd(user, timeout=DefaultValue.TIMEOUT_CLUSTER_SWITCHRESET):
        """
        Reset Switch over
        """
        cmd = "cm_ctl switchover -a -t %d" % timeout
        
        if (user != "") and (os.getgid() == 0):
            cmd = "su - %s -c '%s'" % (user, cmd)
 
        return cmd

    @staticmethod
    def getRedisCmd(user, port, jobs = 1, timeout = None, enableVacuum = "", enableFast = "", host = "localhost", database="postgres"):
        if (timeout is None):
            cmd = "gs_redis -u %s -p %s -h %s -d %s -j %d -r %s %s" % (user, str(port), host, database, jobs, enableVacuum, enableFast)
        else:
            cmd = "gs_redis -u %s -p %s -t %d -h %s -d %s -j %d -r %s %s" % (user, str(port), timeout, host, database, jobs, enableVacuum, enableFast)
        return cmd
        
    @staticmethod
    def getQueryStatusCmd(user, nodeId=0, outFile="", showDetail=True):
        """
        Query status of cluster or node
        """
        cmd = "cm_ctl query"
        if (nodeId > 0):
            cmd += " -n %d" % nodeId
            
        if (showDetail):
            cmd += " -v" 
            
        if (outFile != ""):
            cmd += " > %s" % outFile
            
        return cmd
    
    @staticmethod
    def execSQLCommand(sql, user, host, port, database="postgres"):
        """
        Execute sql command
        """
        sqlFile = os.path.join(DefaultValue.getTmpDirFromEnv(), "gaussdb_query.sql")
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
            
        cmd = "echo \"%s\" > %s && chmod 640 %s" % (sql, sqlFile, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            if(os.path.isfile(sqlFile)):
                os.remove(sqlFile)
            return (status, output)
        if(os.getgid() == 0):
            cmd = "su - %s -c \"gsql -h %s -p %s -d postgres -f %s -t -A -X\" 2>/dev/null" % (user, host, port, sqlFile)
        else:
            cmd = "gsql -h %s -p %s -d postgres -f %s -t -A -X 2>/dev/null" % (host, port, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            if(os.path.isfile(sqlFile)):
                os.remove(sqlFile)
            return (status, output)
        
        rowList = output.split("\n")

        #remove local sqlFile
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
        
        return (0, "\n".join(rowList[:-1]))
    
    @staticmethod
    def remoteSQLCommand(sql, user, host, port, ignoreError = True, database="postgres"):
        """
        Execute sql command on remote host
        """
        sqlFile = os.path.join(DefaultValue.getTmpDirFromEnv(), "gaussdb_remote_query.sql")
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
            
        cmd = "echo \"%s\" > %s && chmod 640 %s" % (sql, sqlFile, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return (status, output)

        localHost = socket.gethostname()
        if(str(localHost) != str(host)):
            cmd = "ssh %s -o BatchMode=yes '(if [ -f %s ];then rm -rf %s;fi)'" % (host, sqlFile, sqlFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                return (status, output)
            if(os.getgid() == 0):
                cmd = "su - %s -c 'scp %s %s:%s'" % (user, sqlFile, host, DefaultValue.getTmpDirFromEnv())
            else:
                cmd = "scp %s %s:%s" % (sqlFile, host, DefaultValue.getTmpDirFromEnv())
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                return (status, output)

        mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(os.getgid() == 0):
            cmd = "ssh %s -o BatchMode=yes 'su - %s -c \"" % (host, user)
            if(mpprcFile != "" and mpprcFile != None):
                cmd += "source %s;" % mpprcFile
            cmd += "gsql -h localhost -p %s -d postgres -f %s -t -A -X\"'" % (port, sqlFile)
            if(ignoreError):
                cmd += " 2>/dev/null"
        else:
            cmd = "ssh %s -o BatchMode=yes '" % host
            if(mpprcFile != "" and mpprcFile != None):
                cmd += "source %s;" % mpprcFile
            cmd += "gsql -h localhost -p %s -d postgres -f %s -t -A -X'" % (port, sqlFile)
            if(ignoreError):
                cmd += " 2>/dev/null"
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            return (status, output)
        
        rowList = output.split("\n")

        #remove local sqlFile
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
        #remove remote sqlFile
        if(str(localHost) != str(host)):
            cmd = "ssh %s -o BatchMode=yes '" % host
            cmd += "(if [ -f %s ]; then rm %s; fi)'" % (sqlFile, sqlFile)
            if(ignoreError):
                cmd += " 2>/dev/null"
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                return (status, output)
            
        return (0, "\n".join(rowList[:-1]))
    
    @staticmethod
    def SaveCooConnections(connList, user, group, append = False):
        """
        Save coordinator ip and port to file
        """
        fp = None
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            coConnFile = "%s/%s.%s" % (tmpDir, DefaultValue.COO_CONNECTION_FILE, user)
            if(append):
                fp = open(coConnFile, "a")
            else:
                fp = open(coConnFile, "w")
            for conn in connList:
                fp.write("%s:%s\n" % (conn[0], conn[1]))
            fp.flush()
            fp.close()
            cmd = "chown %s:%s %s;chmod 640 %s" % (user, group, coConnFile, coConnFile)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("Change %s owner failed!Error: %s." % (coConnFile, output))
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Save coordinator connections failed!Error: %s." % str(e))
        
    @staticmethod
    def readCooConnections(user):
        """
        Read coordinator ip and port from file
        """
        fp = None
        connList = []
        try:
            tmpDir = DefaultValue.getTmpDirFromEnv()
            coConnFile = "%s/%s.%s" % (tmpDir, DefaultValue.COO_CONNECTION_FILE, user)
            fp = open(coConnFile, "r")
            for line in fp.readlines():
                line = line.strip()
                conn = line.split(":")
                if (len(conn) != 2):
                    continue
                connList.append([conn[0], int(conn[1])])
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Read coordinator connections failed!Error: %s." % str(e))
        
        return connList
    @staticmethod
    def getCooConnections(clusterInfo):
        """
        Get coordinator ip and port from clustreInfo
        """
        try:
            cooInstList = []
            for dbNode in clusterInfo.dbNodes:
                cooInstList += dbNode.coordinators
            connInfoList = []
            for cooInst in cooInstList:
                for ip in cooInst.listenIps:
                    connInfoList.append([ip, cooInst.port])
        except Exception, e:
            raise Exception("Read coordinator connections failed!Error: %s." % str(e))
        return connInfoList
