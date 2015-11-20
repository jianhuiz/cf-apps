'''
Created on 2014-2-15

@author: 
''' 

import commands
import getopt
import sys
import os

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue

#############################################################################
# Global variables
#############################################################################
g_opts = None
g_logger = None

class cmdOptions():
    def __init__(self):
        self.script_name = os.path.split(__file__)[-1]
        self.script_dir = os.path.abspath(".")
        self.userInfo = ""
        self.password = ""
        self.logFile = ""
        self.deployAll = False
        
        self.user = ""
        self.group = ""

def usage():
    """
Usage:
    python CreateOsUser.py -U user:group [-l logfile]
    """
    
    print usage.__doc__
    
def parseCommandLine():
    """
    Parse command line
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "U:l:O?", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
    
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
        
    for (key, value) in opts:
        if (key == "-?" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_opts.userInfo = value
        elif (key == "-l"):
            g_opts.logFile = value
        elif (key == "-O"):
            g_opts.deployAll = True
    
def checkParameter():
    """
    Check parameter for create os user
    """
    if (g_opts.userInfo == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        
    strList = g_opts.userInfo.split(":")
    if (len(strList) != 2):
        GaussLog.exitWithError("Parameter input error: -U " + g_opts.userInfo)
    if (strList[0] == "" or strList[1] == ""):
        GaussLog.exitWithError("Parameter input error: -U " + g_opts.userInfo)
    g_opts.user = strList[0]
    g_opts.group = strList[1]
    
        
    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.user, "")
        
    if (not os.path.isabs(g_opts.logFile)):
        GaussLog.exitWithError("Parameter input error, log path need absolute path.")

def initLogger():
    """
    Init logger
    """
    global g_logger
    g_logger = GaussLog(g_opts.logFile, "CreateOsUser")
    
def createUser():
    """
    Create os user
    """
    # Check if group exists
    cmd = "cat /etc/group | awk -F [:] '{print $1}' | grep '^%s$'" % g_opts.group
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
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
        g_logger.log("Create os user[%s:%s]..." % (g_opts.user, g_opts.group))
        cmd = "useradd -m -g %s %s" % (g_opts.group, g_opts.user)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            g_logger.logExit("Create os user failed!Error:%s" % output)
            
        g_logger.log("Change user password...")
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
            
if __name__ == '__main__':
    
    g_opts = cmdOptions()
    parseCommandLine()
    
    checkParameter()
    
    initLogger()
    
    createUser()
    
    g_logger.log("Create os user successfully!")
    g_logger.closeLog()
    sys.exit(0)
    