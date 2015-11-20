import getopt
import shutil
import sys
import os
import socket
import commands
import statvfs
import platform
import signal
import time

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import *
from script.util.OMCommand import OMCommand, ParallelCommand
from script.util.DbClusterInfo import *

ACTION_PREPARE_PATH = "prepare_path"
ACTION_CHECK_OS_VERSION = "check_os_Version"
ACTION_CREATE_OS_USER = "create_os_user"
ACTION_CREATE_CLUSTER_PATHS = "create_cluster_paths"
ACTION_SET_OS_PARAMETER = "set_os_parameter"
ACTION_SET_FINISH_FLAG = "set_finish_flag"
ACTION_SET_USER_ENV = "set_user_env"
ACTION_SET_TOOL_ENV = "set_tool_env"
ACTION_PREPARE_USER_CRON_SERVICE = "prepare_user_cron_service"
ACTION_PREPARE_USER_SSHD_SERVICE = "prepare_user_sshd_service"
ACTION_SET_WARNING_ENV = "set_warning_env"
ACTION_SET_FIRE_WALL_PORT = "set_fire_wall_port"
ACTION_SET_CGROUP = "set_cgroup"
ACTION_SET_SCTP = "set_sctp"

g_logger = None
g_opts = None
g_clusterInfo = None
g_nodeInfo = None
diskSizeInfo = {}
envConfig = {}
cooPortList = []
backPortList = []
newFrontPortTcpList = []
newFrontPortUdpList = []
newBackPortList = []

SYSLOGNG_CONFIG_FILE = '/etc/syslog-ng/syslog-ng.conf'
SYSLOGNG_CONFIG_FILE_SERVER = '/etc/sysconfig/syslog'

class CmdOptions():
    def __init__(self):
        self.action = ""
        self.userInfo = ""
        self.user = ""
        self.group = ""
        self.configfile = ""
        self.preparePath = ""
        self.checkEmpty = False
        self.envParams = []
        self.warningIp = ""
        self.warningNode = ""
        self.warningType = 1
        self.logFile = ""
        self.mpprcFile = ""
        self.clusterToolPath = ""

def initGlobals():
    """
    init global variables
    """
    global g_logger
    global g_clusterInfo
    global g_nodeInfo
    
    g_logger = GaussLog(g_opts.logFile, g_opts.action)
    if(g_opts.configfile != ""):
        g_clusterInfo = dbClusterInfo()
        g_clusterInfo.initFromXml(g_opts.configfile)
        

def initNodeInfo():
    """
    function:
      init node info
    precondition:
      g_clusterInfo has been initialized
    """
    global g_nodeInfo
    
    hostName = socket.gethostname()
    g_nodeInfo = g_clusterInfo.getDbNodeByName(hostName)
    if (g_nodeInfo is None):
        g_logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)

    
def usage():
    """
Usage:
  python PreInstallUtility.py -t action -u user -T warning_type [-g group] [-X xmlfile] [-P path] [-Q clusterToolPath] [-e "envpara=value" [...]] [-w warningserverip] [-h nodename] [-s mpprc_file] [--check_empty] [-l log]
Common options:
  -t                                the type of action
  -u                                the os user of cluster
  -g                                the os user's group of cluster
  -X                                the xml file path
  -P                                the path to be check
  -Q                                the path of cluster tool
  -e "envpara=value"                the os user environment variable
  --check_empty                     check path empty
  -T                                warningtype=1 will use N9000/FI format, warningtype=2 will use ICBC format
  -w                                the ip of warning server
  -s                                the path of mpp environment file
  -h                                the nodename of warning server
  -l                                the path of log file
  --help                            show this help, then exit
    """
    print usage.__doc__

def parseCommandLine():
    """
    Parse command line and save to global variables
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:u:g:X:P:Q:e:T:w:s:h:l:", ["check_empty", "help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

    global g_opts
    g_opts = CmdOptions()
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-u"):
            g_opts.user = value
        elif (key == "-g"):
            g_opts.group = value
        elif (key == "-X"):
            g_opts.configfile = value
        elif (key == "-P"):
            g_opts.preparePath = value
        elif (key == "-Q"):
            g_opts.clusterToolPath = value
        elif (key == "-e"):
            g_opts.envParams.append(value)
        elif (key == "-T"):
            g_opts.warningType = value
        elif (key == "-w"):
            g_opts.warningIp = value
        elif (key == "-h"):
            g_opts.warningNode = value
        elif (key == "--check_empty"):
            g_opts.checkEmpty = True
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        elif (key == "-s"):
            g_opts.mpprcFile = value
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

def checkParameter():
    """
    check parameter for different ation
    """
    if (g_opts.user == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")
    try:
        if(g_opts.action == ACTION_PREPARE_PATH 
            or g_opts.action == ACTION_CREATE_CLUSTER_PATHS
            or g_opts.action == ACTION_SET_FINISH_FLAG
            or g_opts.action == ACTION_SET_USER_ENV):
            PlatformCommand.checkUser(g_opts.user, False)
    except Exception, e:
        GaussLog.exitWithError(str(e))

    if (g_opts.action == ""):
        GaussLog.exitWithError("Parameter input error, need '-t' parameter.")
    if (g_opts.action == ACTION_PREPARE_PATH):
        checkPreparePathParameter()
    elif (g_opts.action == ACTION_CHECK_OS_VERSION):
        pass
    elif (g_opts.action == ACTION_CREATE_OS_USER):
        checkCreateOSUserParameter()
    elif (g_opts.action == ACTION_CREATE_CLUSTER_PATHS):
        checkCreateClusterPathsParameter()
    elif (g_opts.action == ACTION_SET_OS_PARAMETER):
        pass
    elif(g_opts.action == ACTION_SET_FINISH_FLAG):
        pass
    elif(g_opts.action == ACTION_SET_USER_ENV):
        pass
    elif(g_opts.action == ACTION_SET_TOOL_ENV):
        checkSetToolEnvParameter()
    elif(g_opts.action == ACTION_SET_WARNING_ENV):
        checkSetWarningEnvParameter()
    elif(g_opts.action == ACTION_SET_CGROUP):
        checkSetCgroupParameter()
    elif(g_opts.action == ACTION_SET_SCTP):
        pass
    elif(g_opts.action == ACTION_PREPARE_USER_CRON_SERVICE):
        pass
    elif(g_opts.action == ACTION_PREPARE_USER_SSHD_SERVICE):
        pass
    elif(g_opts.action == ACTION_SET_FIRE_WALL_PORT):
        checkSetFireWallPortParameter()
    else:
        GaussLog.exitWithError("Invalid Action : %s" % g_opts.action)

    if(g_opts.mpprcFile != ""):
        if (not os.path.isabs(g_opts.mpprcFile)):
            GaussLog.exitWithError("Parameter input error, mpprc file need absolute path.") 
        #1.set tool env is the first time we use this mpprc file, so we can check and create it.
        #2.in other scene, the mpprc file should have exist, so we just check its exists
        if(g_opts.action == ACTION_SET_TOOL_ENV):
            prepareMpprcFile()
        else:
            if (not os.path.exists(g_opts.mpprcFile)):
                GaussLog.exitWithError("mpprc file does not exist: %s" % g_opts.mpprcFile)

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.user, "")


def prepareMpprcFile():
    """
    """
    mpprcFilePath, mpprcFileName = os.path.split(g_opts.mpprcFile)
    ownerPath = g_opts.mpprcFile
    if(not os.path.exists(g_opts.mpprcFile)):
        while True:
            #find the top path to be created
            (ownerPath, dirName) = os.path.split(ownerPath)
            if (os.path.exists(ownerPath) or dirName == ""):
                ownerPath = os.path.join(ownerPath, dirName)
                break

    #for internal useage, we should set mpprc file permission to 644 here, and change to 640 later.
    cmd = "mkdir -p %s;(if [ ! -f %s ];then touch %s;fi);chmod -R 750 %s;chmod 644 %s" % (mpprcFilePath,
       g_opts.mpprcFile, g_opts.mpprcFile, ownerPath, g_opts.mpprcFile)
    #if given group info in cmdline, we will change the mpprc file owner, otherwise, will not change the mpprc file owner.
    if(g_opts.group != ""):
        cmd += ";chown -R %s:%s %s" % (g_opts.user, g_opts.group, ownerPath)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        GaussLog.exitWithError("prepare mpprc file failed.cmd:%s output:%s" % (cmd, output))


def checkPreparePathParameter():
    """
    check parameter for path
    """
    if(g_opts.preparePath == ""):
        GaussLog.exitWithError("Parameter input error, need '-P' parameter.")
    if(not os.path.isabs(g_opts.preparePath)):
        GaussLog.exitWithError("Parameter input error, %s is not absolute path." % g_opts.preparePath)

    if (g_opts.group == ""):
        GaussLog.exitWithError("Parameter input error, need '-g' parameter.")

def checkCreateOSUserParameter():
    """
    check patameter for create os user
    """
    if (g_opts.group == ""):
        GaussLog.exitWithError("Parameter input error, need '-g' parameter.")

def checkCreateClusterPathsParameter():
    """
    """
    if (g_opts.group == ""):
        GaussLog.exitWithError("Parameter input error, need '-g' parameter.")
        
    if (g_opts.configfile == ""):
        GaussLog.exitWithError("Parameter input error, need '-X' parameter.")
    if (not os.path.exists(g_opts.configfile)):
        GaussLog.exitWithError("Config file does not exist: %s" % g_opts.configfile)   
    if (not os.path.isabs(g_opts.configfile)):
        GaussLog.exitWithError("Parameter input error, configure file need absolute path.")

def checkSetToolEnvParameter():
    """
    """
    if(g_opts.clusterToolPath == ""):
        GaussLog.exitWithError("Parameter input error, need '-Q' parameter.")

def checkSetCgroupParameter():
    """
    """
    if(g_opts.clusterToolPath == ""):
        GaussLog.exitWithError("Parameter input error, need '-Q' parameter.")

def checkSetWarningEnvParameter():
    """
    """    
    if(g_opts.warningType == ""):
        GaussLog.exitWithError("Parameter input error, need '-T' parameter.")      
    g_opts.warningType = int(g_opts.warningType)
    if(g_opts.warningType == 2 and g_opts.warningIp == ""):
        GaussLog.exitWithError("Parameter input error, need '-w' parameter.")

def checkSetFireWallPortParameter():
    """
    """
    if (g_opts.configfile == ""):
        GaussLog.exitWithError("Parameter input error, need '-X' parameter.")
    if (not os.path.exists(g_opts.configfile)):
        GaussLog.exitWithError("Config file does not exist: %s" % g_opts.configfile)   
    if (not os.path.isabs(g_opts.configfile)):
        GaussLog.exitWithError("Parameter input error, configure file need absolute path.")

def checkOSVersion():
    """
    function:
      check if os version is supported
    """
    g_logger.log("begin check os version...")
    if(not PlatformCommand.checkOsVersion()):
        g_logger.logExit("The OS is not SuSE11 64bit!")
    g_logger.log("check os version finished.")

def prepareGivenPath(onePath, checkEmpty = True, checkSize = True):
    """
    function:
      make sure the path exist and user has private to access this path
    precondition:
      1.checkEmpty is True or False
      2.checkSize is True or False
      3.user and group has been initialized
      4.path list has been initialized
      5.path in path list is absolute path
    postcondition:
      1.
    input:
      1.path list
      2.checkEmpty
      3.checkSize
      4.path owner
    output:
      paths in os
    hiden info:na
    ppp:
    for each path in the path list
        save the path
        if path exist
            if need check empty
                check empty
        else
            find the top path to be created
        create the path
        chown owner
        check permission
        check path size
    """
    g_logger.debug("begin prepare path [%s]" % onePath)
    ownerPath = onePath
    if(os.path.exists(onePath)):
        if(checkEmpty):
            fileList = os.listdir(onePath)
            if(len(fileList) != 0):
                g_logger.logExit("[%s] should be empty." % onePath)
    else:
        while True:
            #find the top path to be created
            (ownerPath, dirName) = os.path.split(ownerPath)
            if (os.path.exists(ownerPath) or dirName == ""):
                ownerPath = os.path.join(ownerPath, dirName)
                break
        #create the given path
        g_logger.debug("path [%s] does not exist, create it." % onePath)
        os.makedirs(onePath, DefaultValue.DIRECTORY_MODE)
        
    #if the path already exist, just change the top path mode, else change mode with -R
    ##do not change the file mode in path if exist
    #found error: given path is /a/b/c, script path is /a/b/c/d, then change mode with -R 
    #will cause an error
    if(ownerPath != onePath):
        cmd = "chown -R %s:%s %s" % (g_opts.user, g_opts.group, ownerPath)
    else:
        cmd = "chown %s:%s %s" % (g_opts.user, g_opts.group, ownerPath)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("change path owner cmd:%s" % cmd)
        g_logger.logExit("Change owner of directory[%s] failed!Error: %s" % (onePath, output))
    #check permission
    if(g_opts.action == ACTION_PREPARE_PATH):
        #for tool path, we only need check enter permission
        if (not checkPermission(g_opts.user, onePath, True)):
            g_logger.logExit("[%s] is not enterable for user [%s]." % (onePath, g_opts.user))
    else:
        if (not checkPermission(g_opts.user, onePath)):
            g_logger.logExit("[%s] is not writeable for user [%s]." % (onePath, g_opts.user))
    #check path size
    if (checkSize):
        checkDirSize(onePath, DefaultValue.INSTANCE_DISK_SIZE)
    
    g_logger.debug("prepare path finished.")
    

def checkPermission(username, originalPath, check_enter_only = False):
    """
    function:
      check if given user has operation permission for given path
    precondition:
      1.user should be exist
      2.originalPath should be an absolute path
      3.caller should has root privilege
    postcondition:
      1.return True or False
    """
    cmd = "su - %s -c 'cd %s'" % (username, originalPath)
    status = os.system(cmd)
    if(status != 0):
        return False

    if(check_enter_only == True):
        return True
    
    testFile = os.path.join(originalPath, "touch.tst")
    cmd = "su - %s -c 'touch %s' >/dev/null 2>&1" % (username, testFile)
    status = os.system(cmd)
    if (status != 0):
        return False

    cmd = "su - %s -c 'echo aaa > %s' >/dev/null 2>&1" % (username, testFile)
    status = os.system(cmd)
    if (status != 0):
        cmd = "rm -f %s >/dev/null 2>&1" % testFile
        status = os.system(cmd)
        return False
    
    cmd = "rm -f %s >/dev/null 2>&1" % testFile
    status = os.system(cmd)
    if (status != 0):
        return False
    
    return True

def checkDirSize(path, needSize):
    """
    Check the size of directory
    """
    # The file system of directory
    dfCmd = "df -h '%s' | head -2 |tail -1 | awk -F\" \" '{print $1}'" % path
    status, output = commands.getstatusoutput(dfCmd)
    if (status != 0):
        g_logger.logExit("Get the file system of directory failed!Error: %s" % output)

    fileSysName = str(output)
    diskSize = diskSizeInfo.get(fileSysName)
    if (diskSize is None):
        vfs = os.statvfs(path)
        diskSize = vfs[statvfs.F_BAVAIL] * vfs[statvfs.F_BSIZE] / (1024 * 1024)
        diskSizeInfo[fileSysName] = diskSize

    # 200M for a instance
    if (diskSize < needSize):
        g_logger.logExit("The available size of file system[%s] is not enough for the instances[%s] on it. Each instance needs 200M!" % (fileSysName, path))

    diskSizeInfo[fileSysName] -= needSize

def createOSUser():
    """
    """
    g_logger.debug("begin create os user on local host...")
    # Check if group exists
    cmd = "cat /etc/group | awk -F [:] '{print $1}' | grep '^%s$'" % g_opts.group
    g_logger.debug("check group exists cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("check group exists cmd:%s" % cmd)
        g_logger.logExit("Check group failed!Error: %s" % output)
    
    if (output != g_opts.group):
        g_logger.logExit("Group[%s] does not exist!" % g_opts.group)
    
    cmd = "id -gn '%s'" % g_opts.user
    (status, output) = commands.getstatusoutput(cmd)
    if (status == 0):
        g_logger.debug("User[%s] exists!" % g_opts.user)
        if (output != g_opts.group):
            g_logger.logExit("User not in the group[%s]." % g_opts.group)
        else:
            g_logger.debug("User[%s] in the group[%s]." % (g_opts.user, g_opts.group))
            cmd = "(if [ -f /tmp/temp.%s ];then rm -f /tmp/temp.%s;fi)" % (g_opts.user, g_opts.user)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("Delete tmp file failed!Error:%s" % output)
            return
    else:
        g_logger.debug("Create os user[%s:%s]..." % (g_opts.user, g_opts.group))
        cmd = "useradd -m -g %s %s" % (g_opts.group, g_opts.user)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("create os user cmd:%s" % cmd)
            g_logger.logExit("Create os user failed!Error:%s" % output)
            
        g_logger.debug("Change user password...")
        try:
            fp = open("/tmp/temp.%s" % g_opts.user, "r")
            password = fp.read()
            fp.close()
        except Exception, e:
            g_logger.logExit("get user's passwd failed:%s" % str(e))
            
        cmd = "(if [ -f /tmp/temp.%s ];then rm -f /tmp/temp.%s;fi)" % (g_opts.user, g_opts.user)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("Delete tmp file failed!Error:%s" % output)
        cmd = "echo %s | passwd %s --stdin" % (password.strip(), g_opts.user)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("change passwd for %s failed:%s" % (g_opts.user, output))

    g_logger.debug("create os user on local host finished.")
    
def createClusterPaths():
    """
    function:
      create all paths for cluster
        install path
        tmp path
        data path
        log path
    precondition:
      1.g_clusterInfo has been initialized
    postcondition:
      1.all path exist and have proper authority
    input:NA
    output:na
    hiden info:
      current info of each path
    """
    g_logger.debug("create paths for cluster...")
    if(checkFinishFlag()):
        needCheckEmpty = False
    else:
        needCheckEmpty = True
    
    initNodeInfo()
    prepareGaussLogPath()
    prepareInstallPath(needCheckEmpty)
    prepareTmpPath(needCheckEmpty)
    prepareDataPath(needCheckEmpty)
    
    g_logger.debug("create paths for cluster finished.")

def prepareGaussLogPath():
    """
    """
    g_logger.debug("create log path...")
    gaussdb_dir = g_clusterInfo.logPath
    g_logger.debug("Checking gaussdb directory[%s]..." % gaussdb_dir)
    if(not os.path.exists(gaussdb_dir)):
        os.makedirs(gaussdb_dir, DefaultValue.DIRECTORY_MODE)

    #change gaussdb dir mode
    cmd = "chmod 750 %s&&chown %s:%s %s" % (gaussdb_dir, g_opts.user, g_opts.group, gaussdb_dir)
    g_logger.debug("change directory mode cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("change directory mode cmd:%s" % cmd)
        g_logger.logExit("change mode of directory[%s] failed!Error: %s" % (gaussdb_dir, output))

    #make user log dir
    user_dir = "%s/%s" % (g_clusterInfo.logPath, g_opts.user)
    prepareGivenPath(user_dir, False)

    #change user log dir mode
    userLogPathFileTypeDict = {}
    try:
        userLogPathFileTypeDict = PlatformCommand.getFilesType(user_dir)
    except Exception, e:
        g_logger.logExit("get file type of user log path failed: %s" % str(e))
    for key in userLogPathFileTypeDict:
        if(not os.path.exists(key)):
            g_logger.debug("%s does not exists, skip it." % key)
            continue
        if(os.path.islink(key)):
            #modify link file permission will modify the real file permission,so
            #we should skip it.
            self.logger.debug("[%s] is a link file, skip it." % key)
            continue
        if(userLogPathFileTypeDict[key].find("executable") >= 0 or
                  userLogPathFileTypeDict[key].find("directory") >= 0):
            cmd = "chmod 750 %s" % key
        else:
            cmd = "chmod 640 %s" % key
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("change mode of %s failed. cmd: %s output: %s" % (key, cmd, output))

    #change user log dir owner
    cmd = "chown -R %s:%s %s" % (g_opts.user, g_opts.group, user_dir)
    g_logger.debug("change directory owner cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("change directory owner cmd:%s" % cmd)
        g_logger.logExit("change owner of directory[%s] failed!Error: %s" % (user_dir, output)) 

    g_logger.debug("create log path finished.")
    

def prepareTmpPath(needCheckEmpty):
    """
    """
    g_logger.debug("create tmp path...")
    tmpDir = DefaultValue.getTmpDir(g_opts.user, g_opts.configfile)
    prepareGivenPath(tmpDir, needCheckEmpty)
    g_logger.debug("create tmp path finished.")
    

def prepareDataPath(needCheckEmpty):
    """
    """
    g_logger.debug("create data path...")
    
    g_logger.debug("Check cm datadir")
    prepareGivenPath(g_nodeInfo.cmDataDir, False)

    g_logger.debug("Check cm agent config")
    for cmaInst in g_nodeInfo.cmagents:
        prepareGivenPath(cmaInst.datadir, needCheckEmpty)

    g_logger.debug("Check cm server config")
    for cmsInst in g_nodeInfo.cmservers:
        prepareGivenPath(cmsInst.datadir, needCheckEmpty)

    g_logger.debug("Check gtm config...")
    for gtmInst in g_nodeInfo.gtms:
        prepareGivenPath(gtmInst.datadir, needCheckEmpty)

    g_logger.debug("Check coordinator config...")
    for cooInst in g_nodeInfo.coordinators:
        prepareGivenPath(cooInst.datadir, needCheckEmpty)

    g_logger.debug("Check datanode config...")
    for dnInst in g_nodeInfo.datanodes:
        prepareGivenPath(dnInst.datadir, needCheckEmpty)
        
    g_logger.debug("create data path finished.")

def prepareInstallPath(needCheckEmpty):
    """
    """
    g_logger.debug("create install path...")
    installPath = g_clusterInfo.appPath
    prepareGivenPath(installPath, needCheckEmpty)
    g_logger.debug("create install path finished.")

    
def setOSParameter():
    """
    """
    g_logger.debug("Set os kernel parameter...")
    
    kernelParameterFile = "/etc/sysctl.conf"
    kernelParameterList = DefaultValue.getOSKernelParameterList()
    g_logger.debug("os kernel parameters to be set:\n%s" % kernelParameterList)

    #clean old kernel parameter
    for key in kernelParameterList:
        cmd = "sed -i '/^\\s*%s *=.*$/d' %s" % (key, kernelParameterFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("clean kernel parameter cmd:%s" % cmd)
            g_logger.logExit("Clean old kernel parameter failed!Output:%s" % output)

    #set new kernel parameter
    for key in kernelParameterList:
        cmd = "echo %s = %s  >> %s 2>/dev/null" % (key, kernelParameterList[key], kernelParameterFile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("set kernel parameter cmd:%s" % cmd)
            g_logger.logExit("Set os kernel parameter failed!Error:%s" % str(e))

    #enforce the kernel parameter
    cmd = "sysctl -p"
    (status, output) = commands.getstatusoutput(cmd)
    for key in kernelParameterList:
        if key not in output:
            g_logger.logExit("Enforce os kernel parameter failed!Error:%s" % output)

    g_logger.debug("set os fd num...")
    #set fd num
    cmd = """sed -i '/^.* soft *nofile .*$/d' /etc/security/limits.conf &&
           sed -i '/^.* hard *nofile .*$/d' /etc/security/limits.conf &&
           echo "*       soft    nofile  1000000" >> /etc/security/limits.conf &&
           echo "*       hard    nofile  1000000" >> /etc/security/limits.conf"""
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("set fd cmd:%s" % cmd)
        g_logger.logExit("Set file handle number failed!Output:%s" % output)

    g_logger.debug("Set os kernel parameter finished.")


def prepareUserCronService():
    """
    1.set cron bin permission 
    2.check and make sure user have pemission to use cron
    3.restart cron service
    """
    g_logger.debug("Prepare user cron service...")

    ##1.set crontab file permission
    crontabFile = "/usr/bin/crontab"
    if(not os.path.isfile(crontabFile)):
        g_logger.logExit("cron bin file [%s] does not exist." % crontabFile)
    #attention:crontab file permission should be 755
    cmd = "chown root:root %s && chmod 755 %s && chmod u+s %s" % (crontabFile, crontabFile, crontabFile)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("set permission of [%s] cmd:%s" % (crontabFile, cmd))
        g_logger.logExit("set permission of [%s] failed!Error:%s" % str(output))

    ##2.make sure user have permission to use cron
    cron_allow_file = "/etc/cron.allow"
    if(not os.path.isfile(cron_allow_file)):
        cmd = "touch %s" % cron_allow_file
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("create cron allow file cmd:%s" % cmd)
            g_logger.logExit("create cron allow file failed!Error:%s" % str(output))

    cmd = "chmod 600 %s&& chown root:root %s" % (cron_allow_file, cron_allow_file)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("set cron file owner and permission cmd:%s" % cmd)
        g_logger.logExit("set cron file owner and permission failed!Error:%s" % str(output))
        
    cmd = "sed -i '/^\\s*%s\\s*$/d' %s &&" % (g_opts.user, cron_allow_file)
    cmd += "echo %s >> %s 2>/dev/null" % (g_opts.user, cron_allow_file)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("add user cron permission cmd:%s" % cmd)
        g_logger.logExit("add user cron permission failed!Error:%s" % str(output))

    ##3.restart cron service
    distname, version, id = platform.dist()
    if ("REDHAT" in distname.upper()):
        cronBinName = "crond"
    else:
        cronBinName = "cron"
    cmd = "service %s restart" % cronBinName
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("restart cron service cmd:%s" % cmd)
        g_logger.logExit("Restart cron service failed!Error:%s" % str(output))
        
    g_logger.debug("Prepare user cron service finished.")

def prepareUserSshdService():
    """
    set MaxStartups to 1000.
    """
    g_logger.debug("Prepare user sshd service...")
    sshd_config_file = "/etc/ssh/sshd_config"
    paramName = "MaxStartups"

    #1.change the MaxStartups
    cmd = "sed -i '/^.*%s .*$/d' %s &&" % (paramName, sshd_config_file)
    cmd += "echo '%s 1000' >> %s 2>/dev/null" % (paramName, sshd_config_file)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("change user max startup number cmd:%s" % cmd)
        g_logger.logExit("change user max startup number failed!Error:%s" % str(output))

    #2.restart the sshd service
    cmd = "/etc/init.d/sshd restart"
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("restart sshd service cmd:%s" % cmd)
        g_logger.logExit("Restart sshd service failed!Error:%s" % str(output))
        
    g_logger.debug("Prepare user sshd service finished.")


def setFinishFlag():
    """
    function:
      set env show that do pre install succeed
    precondition:
      1.user has been created
    postcondition:
      1.the value of GAUSS_ENV is 1
    input:NA
    output:user's env GAUSS_ENV
    hiden:
      the evn name and value to be set
    ppp:
    if user bashrc file does not exist
        create it
    clean GAUSS_ENV in user bashrc file
    set GAUSS_ENV in user bashrc file
    """
    g_logger.debug("Set finish flag...")
    if(g_opts.mpprcFile != ""):
        #have check its exists when check parameters, so it should exist here
        userProfile = g_opts.mpprcFile
    else:
        #check if user home exist
        cmd = "su - %s -c \"echo ~\" 2>/dev/null" % g_opts.user
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("check home exists cmd:%s" % cmd)
            g_logger.logExit("Can not get user home.")

        #check if user profile exist
        userProfile = "/home/%s/.bashrc" % g_opts.user
        if (not os.path.exists(userProfile)):
            g_logger.debug("User profile does not exist! Create: %s" % userProfile)
            cmd = "su - %s -c 'touch %s'" % (g_opts.user, userProfile)
            status, output = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.debug("create user profile cmd: %s" % cmd)
                g_logger.logExit("Create user profile failed!Error:%s" % output)

    #clean GAUSS_ENV in user bashrc file
    cmd = "sed -i '/^\\s*export\\s*GAUSS_ENV=.*$/d' %s" % userProfile
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("clean GAUSS_ENV cmd: %s" % cmd)
        g_logger.logExit("Clean GAUSS_ENV in user environment variables failed!Output:%s" % output)

    g_logger.debug("clean old flag finished, begin set new flag...")
    #set GAUSS_ENV in user bashrc file
    fp = None
    try:
        fp = open(userProfile, "a") 
        fp.write("export GAUSS_ENV=1")
        fp.write(os.linesep)
        fp.flush()
        fp.close()
    except Exception, e:
        if(fp):fp.close()
        g_logger.logExit("Set GAUSS_ENV in user environment failed!Error:%s" % str(e))
    
    g_logger.debug("Set finish flag finished.")

def checkFinishFlag():
    """
    return True means have execed preinstall script
    return False means have not execed preinstall script
    """
    if(g_opts.mpprcFile != ""):
        cmd = "su - root -c 'source %s;echo $GAUSS_ENV' 2>/dev/null" % g_opts.mpprcFile
    else:
        cmd = "su - %s -c 'echo $GAUSS_ENV' 2>/dev/null" % g_opts.user
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("check GAUSS_ENV failed.\ncmd: %s\noutput:%s\n" % (cmd, output))
        return False
        
    if(output.strip() == str(1)):
        g_logger.debug("check GAUSS_ENV succeed.")
        return True
    else:
        g_logger.debug("GAUSS_ENV value(%s) is invalid." % (output.strip()))
        return False

def setUserProfile(user, userEnvConfig):
    """
    function:
      set env into user's .bashrc file
    precondition:
      1.env list are valid
      2.user exist
    input:
      1.env list
      2.use name
    postcondition:na
    output:na
    hiden:
      the file to be set into
    """
    g_logger.debug("begin set user profile...")
    if(g_opts.mpprcFile != ""):
        #have check its exists when check parameters, so it should exist here
        userProfile = g_opts.mpprcFile
    else:
        #check if user home exist
        cmd = "su - %s -c \"echo ~\" 2>/dev/null" % user
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("Can not get user home:%s" % output)

        #check if user profile exist
        userProfile = "/home/%s/.bashrc" % user 
        if (not os.path.exists(userProfile)):
            g_logger.debug("User profile does not exist! Create: %s" % userProfile)
            cmd = "su - %s -c 'touch %s'" % (user, userProfile)
            status, output = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Create user profile failed!Error:%s" % output)

    #clean ENV in user bashrc file
    g_logger.debug("user profile exist, begin clean old env...")
    for env in userEnvConfig:
        cmd = "sed -i '/^\\s*export\\s*%s=.*$/d' %s" % (env, userProfile)
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("clean env cmd:%s" % cmd)
            g_logger.logExit("Clean %s in user profile failed!Output:%s" % (env,output))
        g_logger.debug("Clean %s in user profile" % env)

    
    #set ENV in user bashrc file
    g_logger.debug("clean old env finished, begin set new env...")
    for env in userEnvConfig:
        cmd = "echo 'export %s=%s' >> %s 2>/dev/null" % (env, userEnvConfig[env], userProfile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("set env cmd:%s" % cmd)
            g_logger.logExit("Set %s in user profile failed!Output:%s" % (env,output))

    g_logger.debug("set user profile finished.")

def setOSProfile(OSEnvConfig):
    """
    set env into /etc/profile
    """
    g_logger.debug("begin set os profile...")
    if(g_opts.mpprcFile != ""):
        #have check its exists when check parameters, so it should exist here
        userProfile = g_opts.mpprcFile
    else:
        #check if os profile exist
        userProfile = "/etc/profile"
        if (not os.path.exists(userProfile)):
            g_logger.debug("Profile does not exist! Create: %s" % userProfile)
            cmd = "touch %s" % userProfile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Create profile failed!Error:%s" % output)

    #clean ENV in os profile
    g_logger.debug("os profile exist, begin clean old env...")
    for env in OSEnvConfig:
        cmd = "sed -i '/^\\s*export\\s*%s=.*$/d' %s" % (env, userProfile)
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("clean env cmd:%s" % cmd)
            g_logger.logExit("Clean %s in OS profile failed!Output:%s" % (env,output))
        g_logger.debug("Clean %s in OS profile" % env)

    #set ENV in os profile
    g_logger.debug("clean old env finished, begin set new env...")
    for env in OSEnvConfig:
        cmd = "echo 'export %s=%s' >> %s 2>/dev/null" % (env, OSEnvConfig[env], userProfile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.debug("set env cmd:%s" % cmd)
            g_logger.logExit("Set %s in OS profile failed!Output:%s" % (env,output))

    g_logger.debug("set os profile finished.")

    
def setDBUerProfile():
    """
    set database user's env into user's .bashrc file.
    env list are provided by user
    """
    g_logger.debug("begin set db user profile...")
    #check if need to set env parameter
    if(len(g_opts.envParams) == 0):
        g_logger.debug("no env need to set, just return.")
        return
        
    #parse env user inputed
    for param in g_opts.envParams:
        keyValue = param.split("=")
        if (len(keyValue) != 2):
            g_logger.logExit("Parameter input error:%s" % param)
        envConfig[keyValue[0].strip()] = keyValue[1].strip()
        
    #set env into user's profile
    setUserProfile(g_opts.user, envConfig)
    
    g_logger.debug("set db user profile finished.")

def setToolEnv():
    """
    """
    g_logger.debug("begin set tool env...")
    if(g_opts.mpprcFile != ""):
        #have check its exists when check parameters, so it should exist here
        userProfile = g_opts.mpprcFile
    else:
        #check if os profile exist
        userProfile = "/etc/profile"
        if (not os.path.exists(userProfile)):
            g_logger.debug("Profile does not exist! Create: %s" % userProfile)
            cmd = "touch %s" % userProfile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                g_logger.logExit("Create profile failed!Error:%s" % output)

    #clean ENV in os profile
    g_logger.debug("os profile exist, begin clean old tool env...")
    #clean MPPRC FILE PATH
    if(g_opts.mpprcFile != ""):
        cmd = "sed -i '/^\\s*export\\s*%s=.*$/d' %s" % (DefaultValue.MPPRC_FILE_ENV, userProfile)
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Clean mpprc file path in user environment failed!Output:%s" % output)
        g_logger.debug("Clean mpprc file path in user environment variables")
    #clean GPHOME
    cmd = "sed -i '/^\\s*export\\s*GPHOME=.*$/d' %s" % userProfile
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("Clean GPHOME in user environment failed!Output:%s" % output)
    g_logger.debug("Clean GPHOME in user environment variables")
    #clean LD_LIBRARY_PATH
    cmd = "sed -i '/^\\s*export\\s*LD_LIBRARY_PATH=\\$GPHOME\\/lib:\\$LD_LIBRARY_PATH$/d' %s" % userProfile
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("Clean LD_LIBRARY_PATH in user environment failed!Output:%s" % output)
    g_logger.debug("Clean LD_LIBRARY_PATH in user environment variables")
    #clean PATH
    cmd = "sed -i '/^\\s*export\\s*PATH=\\$GPHOME\\/pssh-2.3.1\\/bin:\\$GPHOME\\/sctp:\\$PATH$/d' %s" % userProfile
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("Clean PATH in user environment variables failed!Output:%s" % output)      
    g_logger.debug("Clean PATH in user environment variables") 
    #clean PYTHONPATH
    cmd = "sed -i '/^\\s*export\\s*PYTHONPATH=\\$GPHOME\\/lib/d' %s" % userProfile
    status, output = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("Clean PYTHONPATH in user environment failed!Output:%s" % output)
    g_logger.debug("Clean PYTHONPATH in user environment variables")

    #set ENV in os profile
    g_logger.debug("clean old tool env finished, begin set new tool env...")
    #set env in user profile
    fp = None
    try:
        fp = open(userProfile, "a") 
        if(g_opts.mpprcFile != ""):
            fp.write("export %s=%s" % (DefaultValue.MPPRC_FILE_ENV, g_opts.mpprcFile))
            fp.write(os.linesep)
        fp.write("export GPHOME=%s" % g_opts.clusterToolPath)
        fp.write(os.linesep)
        fp.write("export PATH=$GPHOME/pssh-2.3.1/bin:$GPHOME/sctp:$PATH")
        fp.write(os.linesep)
        fp.write("export LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH")
        fp.write(os.linesep)
        fp.write("export PYTHONPATH=$GPHOME/lib")
        fp.write(os.linesep)
        fp.flush()
        fp.close()
    except Exception, e:
        if(fp):fp.close()
        g_logger.logExit("Set tool env in user environment failed!Error:%s" % str(e))
    g_logger.debug("set tool env finished.")

def cleanWarningEnv():
    """
    """
    g_logger.debug("begin clean syslog env...")

    #clean client syslog-ng configure
    cmd = "(if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE
    cmd += "sed -i -e '/^filter f_gaussdb.*$/d' %s " %  SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^destination d_gaussdb.*$/d' %s " % SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^log { source(src); filter(f_gaussdb); destination(d_gaussdb); };$/d' %s;fi;) " % SYSLOGNG_CONFIG_FILE
    g_logger.debug("clean client syslog cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("clean client syslog failed:Output:%s" % output)

    #clean server syslog-ng configure
    cmd = "(if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE
    cmd += "sed -i -e '/^template t_gaussdb.*$/d' %s " % SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^source s_gaussdb.*$/d' %s " % SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^filter f_gaussdb.*$/d' %s " %  SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^destination d_gaussdb.*$/d' %s " % SYSLOGNG_CONFIG_FILE
    cmd += "-e '/^log { source(s_gaussdb); filter(f_gaussdb); destination(d_gaussdb); };$/d' %s;fi; " % SYSLOGNG_CONFIG_FILE
    cmd += "if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE_SERVER
    cmd += "sed -i -e '/^SYSLOGD_OPTIONS=\\\"-r -m 0\\\"/d' %s " % SYSLOGNG_CONFIG_FILE_SERVER
    cmd += "-e '/^KLOGD_OPTIONS=\\\"-x\\\"/d' %s; " % SYSLOGNG_CONFIG_FILE_SERVER
    cmd += " service syslog restart; fi) "
    g_logger.debug("clean server syslog cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("clean server syslog failed:Output:%s" % output)

    g_logger.debug("clean syslog env finished.")

def setClientWarningEnv():
    """
    """
    g_logger.debug("begin set client warning env...")

    #set client syslog-ng configure
    client_filter = "filter f_gaussdb    { level(err,  crit) and match('MPPDB'); };"
    client_destination = "destination d_gaussdb { udp(\\\"%s\\\"  port(514) ); };" % g_opts.warningIp
    client_log = "log { source(src); filter(f_gaussdb); destination(d_gaussdb); };"
    
    client_syslog_cmd = "(if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE
    client_syslog_cmd += "echo \"%s\" >> %s 2>/dev/null;" % (client_filter, SYSLOGNG_CONFIG_FILE)
    client_syslog_cmd += "echo \"%s\" >> %s 2>/dev/null;" % (client_destination, SYSLOGNG_CONFIG_FILE)
    client_syslog_cmd += "echo \"%s\" >> %s 2>/dev/null; fi)" % (client_log, SYSLOGNG_CONFIG_FILE)
    g_logger.debug("set syslog-ng client config cmd: " + client_syslog_cmd)
    (status, output) = commands.getstatusoutput(client_syslog_cmd)
    if(status != 0):
        g_logger.logExit("set client syslog env failed:Output:%s" % output)

    client_syslog_restart = "service syslog restart"
    g_logger.debug("restart client syslog service: " + client_syslog_restart)
    (status, output) = commands.getstatusoutput(client_syslog_restart)
    if(status != 0):
        g_logger.logExit("restart client syslog service failed:Output:%s" % output)

    g_logger.debug("set client warning env finished.")

def setServerWarningEnv():
    """
    """
    g_logger.debug("begin set server warning env...")
    #set server syslog-ng configure
    server_template = "template t_gaussdb {template(\"$DATE $SOURCEIP $MSGONLY\\n\"); template_escape(no); };"
    server_source = "source s_gaussdb{ udp(); };"
    server_filter = "filter f_gaussdb    { level(err,  crit) and match('MPPDB'); };"
    server_destination = "destination d_gaussdb { file(\"/var/log/syslog_MPPDB\", template(t_gaussdb)); };"
    server_log = "log { source(s_gaussdb); filter(f_gaussdb); destination(d_gaussdb); };"
    server_syslog_cmd = "(if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE
    server_syslog_cmd += "echo '%s' >> %s 2>/dev/null;" % (server_template, SYSLOGNG_CONFIG_FILE)
    server_syslog_cmd += "echo '%s' >> %s 2>/dev/null;" % (server_source, SYSLOGNG_CONFIG_FILE)
    server_syslog_cmd += "echo \"%s\" >> %s 2>/dev/null;" % (server_filter, SYSLOGNG_CONFIG_FILE)
    server_syslog_cmd += "echo '%s' >> %s 2>/dev/null;" % (server_destination, SYSLOGNG_CONFIG_FILE)
    server_syslog_cmd += "echo '%s' >> %s 2>/dev/null; fi)" % (server_log, SYSLOGNG_CONFIG_FILE)

    #set server sysconfig configure
    server_sysconfig_syslogd = "SYSLOGD_OPTIONS=\"-r -m 0\""
    server_sysconfig_klogd = "KLOGD_OPTIONS=\"-x\""
    server_sysconfig_cmd = "(if [ -s %s ]; then " % SYSLOGNG_CONFIG_FILE_SERVER
    server_sysconfig_cmd += "echo '%s' >> %s 2>/dev/null;" % (server_sysconfig_syslogd, SYSLOGNG_CONFIG_FILE_SERVER)
    server_sysconfig_cmd += "echo '%s' >> %s 2>/dev/null; fi)" % (server_sysconfig_klogd, SYSLOGNG_CONFIG_FILE_SERVER)

    g_logger.debug("set syslog-ng server config cmd: " + server_syslog_cmd)
    (status, output) = commands.getstatusoutput(server_syslog_cmd)
    if(status != 0):
        g_logger.logExit("set server syslog env failed:Output:%s" % output)
    g_logger.debug("set sysconfig server config cmd: " + server_sysconfig_cmd)
    (status, output) = commands.getstatusoutput(server_sysconfig_cmd)
    if(status != 0):
        g_logger.logExit("set server sysconfig env failed:Output:%s" % output)

    server_syslog_restart = "service syslog restart"
    g_logger.debug("restart server syslog service: " + server_syslog_restart)
    (status, output) = commands.getstatusoutput(server_syslog_restart)
    if(status != 0):
        g_logger.logExit("restart client syslog service failed:Output:%s" % output)
    
    g_logger.debug("set server warning env finished.")

def setWarningEnv():
    """
    """
    g_logger.debug("begin set warning env...") 
    #set warning env
    warningEnv = {}
    warningEnv['GAUSS_WARNING_TYPE'] = '%d' % g_opts.warningType
    setUserProfile(g_opts.user, warningEnv)
    #set warning syslog configure
    if(g_opts.warningType == 2):
        cleanWarningEnv()
        setClientWarningEnv()
        hostName = socket.gethostname()
        if(g_opts.warningNode == hostName):
            setServerWarningEnv()
    g_logger.debug("set warning env finished.")

def getClusterPort():
    """
    get cluster port information
    """
    global cooPortList
    global backPortList
    try:
        for cooInst in g_nodeInfo.coordinators:
            if (cooInst.port > 0):
                cooPortList.append(cooInst.port)
                backPortList.append(cooInst.haPort)
        for cmsInst in g_nodeInfo.cmservers:
            if (cmsInst.port > 0):
                backPortList.append(cmsInst.port)
                backPortList.append(cmsInst.haPort)
        for gtmInst in g_nodeInfo.gtms:
            if (gtmInst.port > 0):
                backPortList.append(gtmInst.port)
                backPortList.append(gtmInst.haPort)
        for dnInst in g_nodeInfo.datanodes:
            if (dnInst.port > 0):
                backPortList.append(dnInst.port)
                backPortList.append(dnInst.haPort)
    except Exception, e:
        g_logger.logExit(str(e))

    g_logger.debug("Coodinator port %s, back port %s " % (cooPortList, backPortList))

def getClusterIp(clusterInfo):
    """
    get cluster ip information
    """
    oldHostIps = []
    newHostIps = []
    try:
        for dbNode in clusterInfo.dbNodes:
            if dbNode.backIps is not None:
                oldHostIps.append(dbNode.backIps)
            if dbNode.sshIps is not None:
                oldHostIps.append(dbNode.sshIps)
        for i in range(len(oldHostIps)):
            for oldHostIp in oldHostIps[i]:
                if oldHostIp not in newHostIps:
                    newHostIps.append(oldHostIp)
    except Exception, e:
        g_logger.logExit(str(e))

    g_logger.debug("Cluster Ips: %s " % newHostIps)
    return newHostIps

def buildIpPortList():
    """
    build ip-port string list
    """
    global newFrontPortTcpList
    global newFrontPortUdpList
    global newBackPortList
    try:
        hostIps = getClusterIp(g_clusterInfo)
        newFrontPortTcpList = cooPortList
        newFrontPortUdpList = cooPortList
        for ip in hostIps:
            for port in backPortList:
                ipTcpPort = "%s,tcp,%s" % (ip, port)
                ipUdpPort = "%s,udp,%s" % (ip, port)
                newBackPortList.append(ipTcpPort)
                newBackPortList.append(ipUdpPort)
    except Exception, e:
        g_logger.logExit(str(e))

    g_logger.debug("Coo tcp port list before check %s " % newFrontPortTcpList )
    g_logger.debug("Coo tcp port list before check %s " % newFrontPortUdpList )
    g_logger.debug("Back port list before check %s " % newBackPortList )


def cleanIpPortList():
    """
    clean ip port list if already exists
    """
    try:
        cmd = "awk -F '\"' '/^FW_SERVICES_EXT_TCP.*$/ {print $2}' %s" % DefaultValue.FIREWALL_CONFIG_FILE
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("get /etc/sysconfig/SuSEfirewall2 [FW_SERVICES_EXT_TCP] failed!Error: %s" % output)
        existFrontPortTcpList = output.split(' ')
        if (len(existFrontPortTcpList)):
            for existPort in existFrontPortTcpList:
                for newPort in newFrontPortTcpList:
                    if ( str(existPort) == str(newPort)):
                        newFrontPortTcpList.remove(newPort)
                        continue

        cmd = "awk -F '\"' '/^FW_SERVICES_EXT_UDP.*$/ {print $2}' %s" % DefaultValue.FIREWALL_CONFIG_FILE
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("get /etc/sysconfig/SuSEfirewall2 [FW_SERVICES_EXT_UDP] failed!Error: %s" % output)
        existFrontPortUdpList = output.split(' ')
        if (len(existFrontPortUdpList)):           
            for existPort in existFrontPortUdpList:
                for newPort in newFrontPortUdpList:
                    if ( str(existPort) == str(newPort)):
                        newFrontPortUdpList.remove(newPort)
                        continue

        cmd = "awk -F '\"' '/^FW_SERVICES_ACCEPT_EXT.*$/ {print $2}' %s" % DefaultValue.FIREWALL_CONFIG_FILE
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("get /etc/sysconfig/SuSEfirewall2 [FW_SERVICES_ACCEPT_EXT] failed!Error: %s" % output)
        existBackPortList = output.split(' ')
        if (len(existBackPortList)):
            for existIpPort in existBackPortList:
                for newIpPort in newBackPortList:
                    if ( existIpPort == newIpPort):
                        newBackPortList.remove(newIpPort)
                        continue
    except Exception, e:
        g_logger.logExit(str(e))
    g_logger.debug("Coo tcp port list after check %s " % newFrontPortTcpList )
    g_logger.debug("Coo tcp port list after check %s " % newFrontPortUdpList )
    g_logger.debug("Back port list after check %s " % newBackPortList )


def setIpPortList():
    """
    """
    try:
        #config CM GTM DN firewall port 
        backIpPort = ""
        for newIpPort in newBackPortList:
            backIpPort += newIpPort
            backIpPort += " "
        cmd = "sed -i -e 's/^FW_SERVICES_ACCEPT_EXT=\\\"/FW_SERVICES_ACCEPT_EXT=\\\"%s/g' %s " % (backIpPort, DefaultValue.FIREWALL_CONFIG_FILE)
        g_logger.debug("config CM GTM DN firewall cmd: " + cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("config CM GTM DN firewall port failed!Error:%s" % output)

        #config CN firewall port
        frontTcpPort = ""
        frontUdpPort = ""
        for newFrontPortTcp in newFrontPortTcpList:
            frontTcpPort += str(newFrontPortTcp)
            frontTcpPort += " "
        for newFrontPortUdp in newFrontPortUdpList:
            frontUdpPort += str(newFrontPortUdp)
            frontUdpPort += " "
        cmd =  "sed -i -e 's/^FW_SERVICES_EXT_TCP=\\\"/FW_SERVICES_EXT_TCP=\\\"%s/g' %s; " % (frontTcpPort, DefaultValue.FIREWALL_CONFIG_FILE)
        cmd += "sed -i -e 's/^FW_SERVICES_EXT_UDP=\\\"/FW_SERVICES_EXT_UDP=\\\"%s/g' %s; " % (frontUdpPort, DefaultValue.FIREWALL_CONFIG_FILE)
        cmd += "SuSEfirewall2 stop; SuSEfirewall2 start "
        g_logger.debug("config CN firewall cmd: " + cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("config CN firewall port failed!Error:%s" % output)
            
    except Exception, e:
        g_logger.logExit("config user : " + g_opts.user + " firewall failed, " + str(e))


def setFireWallPort():
    """
    """
    g_logger.debug("begin set fire wall port...")
    if (not os.path.exists(DefaultValue.FIREWALL_CONFIG_FILE)):
        g_logger.logExit("[%s] does not exists" % DefaultValue.FIREWALL_CONFIG_FILE)
    initNodeInfo()
    getClusterPort()
    buildIpPortList()
    cleanIpPortList()
    setIpPortList()
    g_logger.debug("set fire wall port finished.")

def setCgroup():
    """
    """
    g_logger.debug("begin set Cgroup...")
    #create temp directory for libcgroup etc
    cgroup_etc_dir = "%s/%s/etc" % (g_opts.clusterToolPath, g_opts.user)
    dirName = os.path.dirname(os.path.abspath(__file__))
    libcgroup_dir = os.path.join(dirName, "../../libcgroup/lib/libcgroup.so")
    cgroup_exe_dir = os.path.join(dirName, "../../libcgroup/bin/gs_cgroup")
    cmd = "rm -rf %s/%s" % (g_opts.clusterToolPath, g_opts.user)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("Clean libcgroup config file failed! Output:%s" % output)
    cmd = "if [ ! -d %s ];then mkdir -p %s && " % (cgroup_etc_dir, cgroup_etc_dir)
    cmd += "chmod 750 %s/../ -R && chown %s:%s %s/../ -R;fi" % (cgroup_etc_dir, g_opts.user, g_opts.group, cgroup_etc_dir)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("Prepare libcgroup etc path failed! Output:%s" % output)
    #libcgroup
    cmd = "ls /usr/local/lib | grep '^libcgroup.so.1$' | wc -l"
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("Check libcgroup failed! Output:%s" % output)
    libcgroup_target = "/usr/local/lib/libcgroup.so.1"
    if(int(output) < 1):
        cmd = "cp %s %s && ldconfig" % (libcgroup_dir, libcgroup_target)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("Copy libcgroup.so.1 failed! Output:%s" % output)
    else:
        if(273405 == os.path.getsize(libcgroup_target)):
            pass
        else:
            cmd = "cp %s %s && ldconfig" % (libcgroup_dir, libcgroup_target)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("Copy libcgroup.so.1 failed! Output:%s" % output)
        
    #call gs_cgroup
    cmd = "%s -U %s -c -H %s/%s" % (cgroup_exe_dir, g_opts.user, g_opts.clusterToolPath, g_opts.user)
    g_logger.debug("Excute gs_cgroup cmd: %s\n" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        if(output.find("CPU Cgroup has been mount on different directory") >= 0):
            g_logger.log(output)
            return
        else:
            g_logger.logExit("Install libcgroup failed! Output:%s" % output)
    #check whether installation succeed
    checkcmd = "ls /dev/cgroups/ | grep '\<Gaussdb:%s\>'" % g_opts.user
    g_logger.debug("Check installation of libcgroup cmd: %s\n" % checkcmd)
    (status, output) = commands.getstatusoutput(checkcmd)
    if (status != 0):
        g_logger.logExit("Check installation of libcgroup failed! Output:%s" % output)

    checkresult = "Gaussdb:%s" % g_opts.user

    if (output != checkresult):
        g_logger.logExit("Install libcgroup failed! Output:%s" % output)
        
    g_logger.debug("set Cgroup finished.")

def handler(signum, frame):
    """
    """
    raise AssertionError
    

def setSctp():
    """
    """
    g_logger.debug("begin set Sctp...")
    try:
        signal.signal(signal.SIGALRM, handler)
        if(g_opts.mpprcFile != ""):
            checksctp_cmd = "source %s&&checksctp" % g_opts.mpprcFile
        else:
            checksctp_cmd = "source /etc/profile&&checksctp"
        signal.alarm(5)
        (status, output) = commands.getstatusoutput(checksctp_cmd)
        signal.alarm(0)
        if(status == 0):
            if(output == "SCTP supported"):
                pass
            else:
                g_logger.logExit("Check sctp unknown result! Cmd:%s Output:%s" % (checksctp_cmd, output))
        elif(status == 256):
            if(output == "checksctp: Protocol not supported"):
                key = "install ipv6 \/bin\/true"
                confFile = "/etc/modprobe.d/*ipv6.conf"
                cmd = "perl -pi.bak -e's/(^\s*%s\s*.*$)/#$1/g' %s" % (key, confFile)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    g_logger.logExit("Comment file %s failed! Cmd:%s Output:%s" % (confFile, cmd, output))
                cmd = "modprobe ipv6"
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    g_logger.logExit("Execute modprobe ipv6 failed! Cmd:%s Output:%s" % (cmd, output))
                signal.alarm(5)
                (status, output) = commands.getstatusoutput(checksctp_cmd)
                signal.alarm(0)
                if(status == 0):
                    if(output == "SCTP supported"):
                        pass
                    else:
                        g_logger.logExit("Check sctp second unknown result! Cmd:%s Output:%s" % (checksctp_cmd, output))
                else:
                    g_logger.logExit("Execute checksctp failed! Cmd:%s Output:%s" % (checksctp_cmd, output))
            else:
                g_logger.logExit("Check sctp unknown result! Cmd:%s Output:%s" % (checksctp_cmd, output))
        else:
            g_logger.logExit("Check sctp failed! Cmd:%s Output:%s" % (checksctp_cmd, output))        
        
    except AssertionError:
        g_logger.logExit("Timeout: checksctp" )
    
    g_logger.debug("set Sctp finished.")

if __name__ == '__main__':
    """
    main function
    """
    parseCommandLine()
    checkParameter()
    initGlobals()

    if(g_opts.action == ACTION_PREPARE_PATH):
        prepareGivenPath(g_opts.preparePath, g_opts.checkEmpty)
    elif(g_opts.action == ACTION_CHECK_OS_VERSION):
        checkOSVersion()
    elif(g_opts.action == ACTION_CREATE_OS_USER):
        createOSUser()
    elif(g_opts.action == ACTION_CREATE_CLUSTER_PATHS):
        createClusterPaths()
    elif(g_opts.action == ACTION_SET_OS_PARAMETER):
        setOSParameter()
    elif(g_opts.action == ACTION_SET_FINISH_FLAG):
        setFinishFlag()
    elif(g_opts.action == ACTION_SET_TOOL_ENV):
        setToolEnv()
    elif(g_opts.action == ACTION_SET_USER_ENV):
        setDBUerProfile()
    elif(g_opts.action == ACTION_PREPARE_USER_CRON_SERVICE):
        prepareUserCronService()
    elif(g_opts.action == ACTION_PREPARE_USER_SSHD_SERVICE):
        prepareUserSshdService()
    elif(g_opts.action == ACTION_SET_WARNING_ENV):
        setWarningEnv()
    elif(g_opts.action == ACTION_SET_FIRE_WALL_PORT):
        setFireWallPort()
    elif(g_opts.action == ACTION_SET_CGROUP):
        setCgroup()
    elif(g_opts.action == ACTION_SET_SCTP):
        setSctp()
    else:
        g_logger.logExit("Parameter input error, unknown action:%s" % g_opts.action)

        
