'''
Created on 2014-2-17

@author: 
'''

import commands
import getopt
import socket
import sys
import os
import statvfs

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.Common import *

TIME_OUT = 2
#############################################################################
# Global variables
#############################################################################
g_logger = None
g_clusterUser = ""

class CheckNodeEnv():
    """
    Init all instance on local node
    """
    def __init__(self, cooParams, dataParams, instIds = []):
        self.__cooGucParams = cooParams[:]
        self.__dataGucParams = dataParams[:]
        self.__instanceIds = instIds[:] # if is empty, check all instances
        self.__clusterInfo = None
        self.__dbNodeInfo = None
        self.__diskSizeInfo = {}
        self.__pgsqlFiles = []
        self.__user = ""
        self.__group = ""

    def run(self):
        """
        Init instance on local node
        """
        self.__checkParameters()
        self.__readConfigInfo()
        self.__getUserInfo()
        self.__checkGaussLogDir()
        self.__checkPgsqlDir()
        self.__checkNodeConfig()
        self.__setManualStart()
        self.__setCron()

    def __checkParameters(self):
        """
        Check parameters for instance config
        """
        g_logger.log("Checking parameters for config coodinator and datanode...")
        for param in self.__cooGucParams:
            if (self.__checkconfigParams(param.strip()) != 0):
                g_logger.logExit("Parameter input error: %s." % param)

        for param in self.__dataGucParams:
            if (self.__checkconfigParams(param.strip()) != 0):
                g_logger.logExit("Parameter input error: %s." % param)

    def __checkconfigParams(self, param):
        """
        Check parameter for postgresql.conf
            port : this is calculated automatically
        """
        configInvalidArgs = ["port"]

        argList = param.split("=")
        for arg in configInvalidArgs:
            if (arg in argList):
                return 1

        return 0

    def __readConfigInfo(self):
        """
        Read config from static config file
        """
        try:
            self.__clusterInfo = dbClusterInfo()
            self.__clusterInfo.initFromStaticConfig(g_clusterUser)
            hostName = socket.gethostname()
            self.__dbNodeInfo = self.__clusterInfo.getDbNodeByName(hostName)
            if (self.__dbNodeInfo is None):
                g_logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            g_logger.logExit(str(e))

        g_logger.debug("Instance info on local node:\n%s" % str(self.__dbNodeInfo))

    def __getUserInfo(self):
        """
        Get user and group
        """
        g_logger.log("Getting user and group for application...")
        cmd = "stat -c '%%U:%%G' %s" % self.__clusterInfo.appPath
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Get user info failed!Error :%s" % output)

        userInfo = output.split(":")
        if (len(userInfo) != 2):
            g_logger.logExit("Get user info failed!Error :%s" % output)

        self.__user = userInfo[0]
        self.__group = userInfo[1]

    def __checkGaussLogDir(self):
        """
        Check GaussDB Log directory
        """
        #check user base log dir
        user_dir = DefaultValue.getUserLogDirWithUser(self.__user)
        g_logger.log("Checking gaussdb log directory[%s]..." % user_dir)
        if(not os.path.exists(user_dir)):
            g_logger.logExit("user base log directory[%s] does not exist." % user_dir)

        ##make cm log dir
        user_cm_dir = os.path.join(user_dir, "cm")
        self.__makeDirForDBUser(user_cm_dir, "user_cm_dir")

        ##make cm_agent log dir
        user_cm_cm_agent_dir = os.path.join(user_cm_dir, "cm_agent")
        self.__makeDirForDBUser(user_cm_cm_agent_dir, "user_cm_cm_agent_dir")

        ##make cm_server log dir
        user_cm_cm_server_dir = os.path.join(user_cm_dir, "cm_server")
        self.__makeDirForDBUser(user_cm_cm_server_dir, "user_cm_cm_server_dir")

        ##make om_monitor log dir
        user_cm_om_monitor_dir = os.path.join(user_cm_dir, "om_monitor")
        self.__makeDirForDBUser(user_cm_om_monitor_dir, "user_cm_om_monitor_dir")

        ##make pg_log dir and pg_audit dir
        user_pg_log_dir = os.path.join(user_dir, "pg_log")
        self.__makeDirForDBUser(user_pg_log_dir, "user_pg_log_dir")

        user_pg_audit_dir = os.path.join(user_dir, "pg_audit")
        self.__makeDirForDBUser(user_pg_audit_dir, "user_pg_audit_dir")

        ##make bin log dir
        user_bin_dir = os.path.join(user_dir, "bin")
        self.__makeDirForDBUser(user_bin_dir, "user_bin_dir")

        ##make dn cn gtm dir in pg_log and pg_audit
        ##make gtm dir
        for inst in self.__dbNodeInfo.gtms:
            user_pg_log_gtm_dir = os.path.join(user_pg_log_dir, "gtm")
            self.__makeDirForDBUser(user_pg_log_gtm_dir, "user_pg_log_gtm_dir")

        ##make dn cn dir
        for inst in self.__dbNodeInfo.coordinators:
            log_dir_name = "cn_%d" % (inst.instanceId)
            log_dir = os.path.join(user_pg_log_dir, log_dir_name)
            audit_dir = os.path.join(user_pg_audit_dir, log_dir_name)
            self.__makeDirForDBUser(log_dir, "user_pg_log_%s_dir" % log_dir_name)
            self.__makeDirForDBUser(audit_dir, "user_pg_audit_%s_dir" % log_dir_name)
            self.__cleanLogDir(audit_dir, "user_pg_audit_%s_dir" % log_dir_name)

        for inst in self.__dbNodeInfo.datanodes:
            log_dir_name = "dn_%d" % (inst.instanceId)
            log_dir = os.path.join(user_pg_log_dir, log_dir_name)
            audit_dir = os.path.join(user_pg_audit_dir, log_dir_name)
            self.__makeDirForDBUser(log_dir, "user_pg_log_%s_dir" % log_dir_name)
            self.__makeDirForDBUser(audit_dir, "user_pg_audit_%s_dir" % log_dir_name)
            self.__cleanLogDir(audit_dir, "user_pg_audit_%s_dir" % log_dir_name)

        ##fix log permission
        logPathFileTypeDict = {}
        try:
            logPathFileTypeDict = PlatformCommand.getFilesType(user_dir)
        except Exception, e:
            g_logger.logExit("get file type of log path failed: %s" % str(e))
        for key in logPathFileTypeDict:
            if(not os.path.exists(key)):
                g_logger.debug("[%s] does not exist, skip it." % key)
                continue
            if(os.path.islink(key)):
                self.logger.debug("[%s] is a link file, skip it." % key)
                continue
            if(logPathFileTypeDict[key].find("executable") >= 0 or
                      logPathFileTypeDict[key].find("directory") >= 0):
                cmd = "chmod 750 %s" % key
            else:
                cmd = "chmod 640 %s" % key
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("chmod of %s failed. cmd: %s output: %s" % (key, cmd, output))


    def __makeDirForDBUser(self, path, desc, mode = "750"):
        """
        """
        g_logger.debug("Making %s directory[%s] for DB user..." % (desc, path))
        cmd = "mkdir -p %s" % path
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Make %s directory[%s] failed!Error: %s" % (desc, path, output))

        cmd = "chmod -R %s %s" % (mode, path)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change mode[%s] of %s directory[%s] failed!Error: %s" % (mode, desc, path, output))

        cmd = "chown -R %s:%s %s" % (self.__user, self.__group, path)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change owner of %s directory[%s] failed!Error: %s" % (desc, path, output))

        if (not PlatformCommand.checkDirWriteable(path, self.__user)):
            g_logger.logExit("This directory[%s] is not writeable for database administrator." % path)

    def __cleanLogDir(self, path, desc):
        g_logger.debug("Clean %s directory[%s]..." % (desc, path))
        cmd = "rm -rf %s/*" % path
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("clean %s directory[%s] failed!Error: %s" % (desc, path, output))

    def __checkPgsqlDir(self):
        """
        Check pgsql directory
        """
        tmpDir = DefaultValue.getTmpDirFromEnv()
        g_logger.log("Checking directory[%s]..." % tmpDir)
        if(not os.path.exists(tmpDir)):
            g_logger.logExit("temp directory[%s] does not exist, please create it first." % tmpDir)

        self.__pgsqlFiles = os.listdir(tmpDir)

        cmd = "chown %s:%s %s" % (self.__user, self.__group, tmpDir)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (tmpDir, output))

        cmd = "chmod 700 %s" % tmpDir
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change mode of directory[%s] failed!Error: %s" % (tmpDir, output))

        if (not PlatformCommand.checkDirWriteable(tmpDir, self.__user)):
            g_logger.logExit("Temp directory[%s] is not writeable for database administrator." % tmpDir)

    def __checkNodeConfig(self):
        """
        Check instances conifg on local node
        """
        g_logger.log("Check cm datadir")
        self.__checkDataDir(self.__dbNodeInfo.cmDataDir, False)

        g_logger.log("Check cm agent config")
        for cmaInst in self.__dbNodeInfo.cmagents:
            if (len(self.__instanceIds) != 0 and cmaInst.instanceId not in self.__instanceIds):
                continue
            self.__checkDataDir(cmaInst.datadir)

        g_logger.log("Check cm server config")
        for cmsInst in self.__dbNodeInfo.cmservers:
            if (len(self.__instanceIds) != 0 and cmsInst.instanceId not in self.__instanceIds):
                continue
            self.__checkPort(cmsInst.port)
            self.__checkPort(cmsInst.haPort)
            self.__checkDataDir(cmsInst.datadir)

        g_logger.log("Check gtm config...")
        for gtmInst in self.__dbNodeInfo.gtms:
            if (len(self.__instanceIds) != 0 and gtmInst.instanceId not in self.__instanceIds):
                continue
            self.__checkPort(gtmInst.port)
            self.__checkPort(gtmInst.haPort)
            self.__checkDataDir(gtmInst.datadir)

        g_logger.log("Check gtm proxy config...")
        for proxyInst in self.__dbNodeInfo.gtmProxys:
            if (len(self.__instanceIds) != 0 and proxyInst.instanceId not in self.__instanceIds):
                continue
            self.__checkPort(proxyInst.port)

        g_logger.log("Check coordinator config...")
        for cooInst in self.__dbNodeInfo.coordinators:
            if (len(self.__instanceIds) != 0 and cooInst.instanceId not in self.__instanceIds):
                continue
            self.__checkPort(cooInst.port)
            self.__checkPort(cooInst.haPort)
            self.__checkDataDir(cooInst.datadir)

        g_logger.log("Check datanode config...")
        for dnInst in self.__dbNodeInfo.datanodes:
            if (len(self.__instanceIds) != 0 and dnInst.instanceId not in self.__instanceIds):
                continue
            self.__checkPort(dnInst.port)
            self.__checkPort(dnInst.haPort)
            self.__checkDataDir(dnInst.datadir)

    def __checkDataDir(self, datadir, checkEmpty = True, checkSize = True):
        """
        Check if directory exists and disk size lefted
        """
        g_logger.log("Checking directory[%s]..." % datadir)

        # Check and create directory
        ownerPath = datadir
        if(os.path.exists(datadir)):
            if (checkEmpty):
                fileList = os.listdir(datadir)
                if(len(fileList) != 0):
                    g_logger.logExit("Data directory[%s] of instance should be empty." % datadir)
        else:
            while True:
                (ownerPath, dirName) = os.path.split(ownerPath)
                if (os.path.exists(ownerPath) or dirName == ""):
                    ownerPath = os.path.join(ownerPath, dirName)
                    os.makedirs(datadir, DefaultValue.DIRECTORY_MODE)
                    break

        cmd = "chown -R %s:%s %s" % (self.__user, self.__group, ownerPath)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (datadir, output))

        # Check if data directory is writeable
        if (not PlatformCommand.checkDirWriteable(datadir, self.__user)):
            g_logger.logExit("Data directory[%s] is not writeable for database administrator." % datadir)

        if (checkSize):
            self.__checkDataDirSize(datadir, DefaultValue.INSTANCE_DISK_SIZE)

    def __checkDataDirSize(self, datadir, needSize):
        """
        Check the size of directory
        """
        # The file system of directory
        dfCmd = "df -h '%s' | head -2 |tail -1 | awk -F\" \" '{print $1}'" % datadir
        status, output = commands.getstatusoutput(dfCmd)
        if (status != 0):
            g_logger.logExit("Get the file system of directory failed!Error: %s" % output)

        fileSysName = str(output)
        diskSize = self.__diskSizeInfo.get(fileSysName)
        if (diskSize is None):
            vfs = os.statvfs(datadir)
            diskSize = vfs[statvfs.F_BAVAIL] * vfs[statvfs.F_BSIZE] / (1024 * 1024)
            self.__diskSizeInfo[fileSysName] = diskSize

        # 200M for a instance
        if (diskSize < needSize):
            g_logger.logExit("The available size of file system[%s] is not enough for the instances on it. Each instance needs 200M!" % fileSysName)

        self.__diskSizeInfo[fileSysName] -= needSize

    def __checkPort(self, port):
        """
        Check if port is used
        """
        if(port < 0 or port > 65535):
            g_logger.logExit("illegal number of port[%d]." % port)
        if(port >=0 and port <= 1023):
            g_logger.logExit("system reserved port[%d]." % port)
        self.__checkRandomPortRange(port)

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
    def __checkRandomPortRange(self, port):
        """
        Check if port is in the range of random port
        """
        cmd = "cat /proc/sys/net/ipv4/ip_local_port_range"
        g_logger.debug("Get random port range cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("Get the range of random port failed!Error:%s" % str(output))
            return
        res = output.split()
        if(len(res) != 2):
            g_logger.debug("The range of random port is invalid!Error:%s" % str(output))
            return
        minPort = int(res[0])
        maxPort = int(res[1])
        if(port >= minPort and port <= maxPort):
            g_logger.debug("Warning: Current instance port is in the range of random port(%d - %d)!" % (minPort, maxPort))

    def __setManualStart(self):
        """
        Set manual start
        """
        g_logger.log("Set manual start...")
        cmd = "touch %s/bin/cluster_manual_start;chmod 640 %s/bin/cluster_manual_start" % (self.__clusterInfo.appPath, self.__clusterInfo.appPath)
        g_logger.debug("Set manual cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("Set manual start failed!Error:%s" % str(output))

    def __setCron(self):
        """
        Set linux cron
        """
        g_logger.log("Set cron...")
        log_path = DefaultValue.getOMLogPath(DefaultValue.OM_MONITOR_DIR_FILE, self.__user, self.__clusterInfo.appPath, "")
        cronFile = "%s/gauss_cron_%d" % (DefaultValue.getTmpDirFromEnv(), os.getpid())
        setCronCmd = ""
        
        cmd = "crontab -l"
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            setCronCmd += "crontab -l > %s&& " % cronFile
            setCronCmd += "sed -i '/\\/bin\\/om_monitor/d' %s; " % cronFile
        elif(status != 256):#status==256 means this user has no cron
            g_logger.logExit("check user cron failed:%s" % output)
        
        mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(mpprcFile != "" and mpprcFile != None):
            setCronCmd += "echo '*/1 * * * * source %s;" % mpprcFile
        else:
            setCronCmd += "echo '*/1 * * * * source /etc/profile;source ~/.bashrc;"
            
        setCronCmd += "nohup %s/bin/om_monitor -L %s >>/dev/null 2>&1 &' >> %s&&" % (self.__clusterInfo.appPath, log_path, cronFile)
        setCronCmd += "crontab %s&&" % cronFile
        setCronCmd += "rm -f %s" % cronFile

        g_logger.debug("Set cron cmd: %s" % setCronCmd)
        (status, output) = commands.getstatusoutput(setCronCmd)
        if(status != 0):
            g_logger.logExit("Set cron failed!Error:%s" % str(output))


def usage():
    """
Usage:
    python CheckConfig.py -h | --help
    python CheckConfig.py -U user [-i instId [...]] [-C "PARAMETER=VALUE" [...]] [-D "PARAMETER=VALUE" [...]] [-l logfile]
    """

    print usage.__doc__

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "U:C:D:i:l:h", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

    global g_clusterUser
    logFile = ""
    cooParams = []
    dataParams = []
    instanceIds = []

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-C"):
            cooParams.append(value)
        elif (key == "-D"):
            dataParams.append(value)
        elif (key == "-l"):
            logFile = os.path.abspath(value)
        elif(key == "-i"):
            if (value.isdigit()):
                instanceIds.append(int(value))
            else:
                GaussLog.exitWithError("Parameter invalid. -i %s is not digit." % value)

    # check if user exist and is the right user
    PlatformCommand.checkUser(g_clusterUser)

    #check log dir
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "", "")

    #Init logger
    global g_logger
    g_logger = GaussLog(logFile, "CheckConfig")
    try:
        checker = CheckNodeEnv(cooParams, dataParams, instanceIds)
        checker.run()

        g_logger.log("Check config on node[%s] successfully!" % socket.gethostname())
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.logExit(str(e))

if __name__ == '__main__':
    main()
