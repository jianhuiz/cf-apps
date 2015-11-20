import getopt
import shutil
import statvfs
import sys
import os
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import *

g_logger = None


class CmdOptions():
    def __init__(self):
        self.oldUser = ""
        self.newUser = ""
        self.logFile = ""


def usage():
    """
Usage:
  python upgradePhase1Cmd_530To1130.py -u newUser -U oldUser [-l log]
Common options:
  -u                                the user of new cluster
  -U                                the user of old cluster
  -l                                the path of log file
  --help                            show this help, then exit
    """
    print usage.__doc__

def parseCommandLine():
    """
    Parse command line and save to global variable
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:U:l:", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-u"):
            g_opts.newUser = value
        elif (key == "-U"):
            g_opts.oldUser = value
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

    if (g_opts.oldUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        
    if (g_opts.newUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")

    if(g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.oldUser)

def initGlobalInfos():
    """
    """
    global g_logger
    g_logger = GaussLog(g_opts.logFile, "upgradePhase1cmd") 

def copyFilesToOldCluster():
    """
    copy pg_controldata and pg_resetxlog to old cluster
    """
    oldInstallDir = DefaultValue.getInstallDir(g_opts.oldUser)
    if(oldInstallDir == ""):
        g_logger.logExit("get install of user %s failed." % g_opts.oldUser) 
    newInstallDir = DefaultValue.getInstallDir(g_opts.newUser)
    if(newInstallDir == ""):
        g_logger.logExit("get install of user %s failed." % g_opts.newUser)
        
    new_pg_controldata_path = "%s/bin/pg_controldata" % newInstallDir
    new_pg_resetxlog_path = "%s/bin/pg_resetxlog" % newInstallDir
    old_pg_controldata_path = "%s/bin/pg_controldata" % oldInstallDir
    old_pg_resetxlog_path = "%s/bin/pg_resetxlog" % oldInstallDir

    #check if files exist in new cluster
    if(not os.path.isfile(new_pg_controldata_path)):
        g_logger.logExit("%s does not exist in new cluster!" % new_pg_controldata_path)
    if(not os.path.isfile(new_pg_resetxlog_path)):
        g_logger.logExit("%s does not exist in new cluster!" % new_pg_resetxlog_path)

    #copy files to old cluster if not exist in old cluster
    g_logger.debug("copy file %s to old cluster..." % old_pg_controldata_path)
    shutil.copy(new_pg_controldata_path, old_pg_controldata_path)
    g_logger.debug("copy file %s to old cluster..." % old_pg_resetxlog_path)
    shutil.copy(new_pg_resetxlog_path, old_pg_resetxlog_path)
        
    #change file mode
    os.chmod(old_pg_controldata_path, 0750)
    os.chmod(old_pg_resetxlog_path, 0750)

if __name__ == '__main__':
    """
    main function
    """
    g_opts = CmdOptions()
    parseCommandLine()
    initGlobalInfos()
    
    try:
        g_logger.log("Begin send files...")
        copyFilesToOldCluster()
        g_logger.log("Send files successfully!")
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.log(str(e))
        g_logger.logExit("Send files failed!")

        