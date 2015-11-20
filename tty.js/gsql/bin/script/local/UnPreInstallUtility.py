import getopt
import shutil
import sys
import os
import socket
import commands
import statvfs
import platform

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import *
from script.util.OMCommand import OMCommand, ParallelCommand
from script.util.DbClusterInfo import *


ACTION_CLEAN_FIREWALL = 'clean_firewall'
ACTION_CLEAN_TOOL_ENV = 'clean_tool_env'
ACTION_CLEAN_CGROUP = 'clean_cgroup'
ACTION_CHECK_UNPREINSTALL = "check_unpreinstall"
ACTION_CLEAN_GAUSS_ENV = "clean_gauss_env"
PROFILE_FILE = '/etc/profile'
GPHOME = '\\/opt\\/huawei\\/wisequery'
PSSHDIR = 'pssh-2.3.1'
COMPRESSPACKAGE = 'Gauss-MPPDB-tools-bak.tar.gz'
SYSLOGNG_CONFIG_FILE = '/etc/syslog-ng/syslog-ng.conf'
SYSLOGNG_CONFIG_FILE_SERVER = '/etc/sysconfig/syslog'

g_logger = None
g_opts = None


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
        self.userProfile = ""
        self.logFile = ""
        self.clusterToolPath = ""

def initGlobals():
    """
    init global variables
    """
    global g_logger
    
    g_logger = GaussLog(g_opts.logFile, g_opts.action)

    #make sure if we are using env seperate version, and get the right profile
    #we can not check mppenvfile exists here
    mppenvFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
    if(mppenvFile != "" and mppenvFile != None):
        g_opts.userProfile = mppenvFile
    else:
        g_opts.userProfile = "/home/%s/.bashrc" % g_opts.user

def usage():
    """
Usage:
  python UnPreInstallUtility.py -t action -u user [-X xmlfile] [-l log]
Common options:
  -t                                the type of action
  -u                                the os user of cluster
  -X                                the xml file path
  -l                                the path of log file
  --help                            show this help, then exit
    """
    print usage.__doc__

def parseCommandLine():
    """
    Parse command line and save to global variables
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:u:X:l:", ["help"])
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
        elif (key == "-X"):
            g_opts.configfile = value
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

def checkParameter():
    """
    check parameter for different ation
    """

    if (g_opts.action == ""):
        GaussLog.exitWithError("Parameter input error, need '-t' parameter.")

    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.user, "")

    if (g_opts.user == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")
    
def cleanWarningConfig():
    """
    clean syslog-ng config
    """
    g_logger.debug("begin clean syslog-ng config...")

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

    g_logger.debug("clean syslog finished.")

def cleanCgroup():
    """
    clean libcgroup
    """
    g_logger.debug("begin clean libcgroup...")
    dirName = os.path.dirname(os.path.abspath(__file__))
    cgroup_exe_dir = os.path.join(dirName, "../../libcgroup/bin/gs_cgroup")
    #call gs_cgroup
    cmd = "%s -U %s -d" % (cgroup_exe_dir, g_opts.user)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        if(output.find("CPU Cgroup has been mount on different directory") >= 0):
            g_logger.log(output)
        else: 
            g_logger.logExit("Uninstall libcgroup failed! Output:%s" % output)

    try:
        g_opts.clusterToolPath = DefaultValue.getClusterToolPath()
    except Exception, e:
        g_logger.logExit("get cluster tool path failed: %s" % str(e))

    cmd = "rm -rf %s/%s" % (g_opts.clusterToolPath, g_opts.user)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("Clean libcgroup config file failed! Output:%s" % output)
    
    g_logger.debug("clean libcgroup finished")

def cleanEnvSoftware():
    """
    clean environment software and variable
    """
    g_logger.debug("begin clean environment software and variable...")

    try:
        g_opts.clusterToolPath = DefaultValue.getClusterToolPath()
    except Exception, e:
        g_logger.logExit("get cluster tool path failed: %s" % str(e))

    #clean environment software
    cmd = "rm -rf %s/%s; " % (g_opts.clusterToolPath, PSSHDIR)
    cmd += "rm -rf %s/sctp; " % g_opts.clusterToolPath
    cmd += "rm -f %s/%s " % (g_opts.clusterToolPath, COMPRESSPACKAGE)
    g_logger.debug("Clean environment software cmd: %s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("clean environment software failed:Output:%s" % output)

    #clean environment variable
    cmd = "(if [ -s %s ]; then " % PROFILE_FILE
    cmd += "sed -i -e '/^export GPHOME=%s$/d' %s " % (GPHOME,PROFILE_FILE)
    cmd += "-e '/^export PATH=\$GPHOME\/pssh-2.3.1\/bin:\$GPHOME\/sctp:\$PATH$/d' %s " % PROFILE_FILE
    cmd += "-e '/^export LD_LIBRARY_PATH=\$GPHOME\/lib:\$LD_LIBRARY_PATH$/d' %s " % PROFILE_FILE
    cmd += "-e '/^export PYTHONPATH=\$GPHOME\/lib$/d' %s; fi) " % PROFILE_FILE
    g_logger.debug("clean environment variable cmd: %s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("clean environment variable failed:Output:%s" % output)

    g_logger.debug("clean environment software and variable finished.")

def checkUnPreInstall():
    """
    check whether do uninstall before unpreinstall
    """
    g_logger.debug("begin check UnPreInstall...")
    
    #check if user exist
    cmd = "id -nu %s 2>/dev/null" % g_opts.user
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        g_logger.logExit("User[%s] does not exist!" % g_opts.user)

    #check if user profile exist
    if (not os.path.exists(g_opts.userProfile)):
        g_logger.debug("User profile does not exist,skip check UnPreInstall")
        return
    
    #check $GAUSSHOME
    cmd = "su - %s -c 'source %s && echo $GAUSSHOME' 2>/dev/null" % (g_opts.user, g_opts.userProfile)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("get $GAUSSHOME cmd:%s" % cmd)
        g_logger.logExit("Can not get $GAUSSHOME:%s" % output)
    gaussHome = output.strip()
    if (gaussHome != ""):
        g_logger.logExit("Please exec GaussUninstall script first, and then exec this script.")
         
    #check $GAUSS_ENV
    cmd = "su - %s -c 'source %s && echo $GAUSS_ENV' 2>/dev/null" % (g_opts.user, g_opts.userProfile)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("get $GAUSS_ENV cmd:%s" % cmd)
        g_logger.logExit("Can not get $GAUSS_ENV:%s" % output)
    gaussEnv = output.strip()
     
    if (str(gaussEnv) != "1"):
        g_logger.logExit("Please exec PreInstall script first, and then exec this script.")

    g_logger.debug("End check UnPreInstall")

def cleanGaussEnv():
    """
    clean $GAUSS_ENV
    """
    g_logger.debug("begin clean $GAUSS_ENV...")
    
    #check if user profile exist
    userProfile = "/home/%s/.bashrc" % g_opts.user 
    if (not os.path.exists(userProfile)):
        g_logger.debug("User profile does not exist,skip clean $GAUSS_ENV")
        return
        
    #clean $GAUSS_ENV
    cmd = "sed -i '/^\\s*export\\s*GAUSS_ENV=.*$/d' %s " % userProfile
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.debug("clean $GAUSS_ENV cmd:%s" % cmd)
        g_logger.logExit("clean $GAUSS_ENV failed:Output:%s" % output) 
        
    g_logger.debug("end clean $GAUSS_ENV")

if __name__ == '__main__':
    """
    main function
    """
    parseCommandLine()
    checkParameter()
    initGlobals()

    if(g_opts.action == ACTION_CLEAN_FIREWALL):
        cleanWarningConfig()
    elif(g_opts.action == ACTION_CLEAN_TOOL_ENV):
        cleanEnvSoftware()
    elif(g_opts.action == ACTION_CLEAN_CGROUP):
        cleanCgroup()
    elif(g_opts.action == ACTION_CHECK_UNPREINSTALL):
        checkUnPreInstall()
    elif(g_opts.action == ACTION_CLEAN_GAUSS_ENV):
        cleanGaussEnv()
    else:
        g_logger.logExit("Parameter input error, unknown action:%s" % g_opts.action)

