#!/usr/bin/env python

'''
Created on 2014-2-19

@author: 
'''

import commands
import getopt
import sys
import os

#############################################################################
# Global variables
#############################################################################
g_opts = None

class cmdOptions():
    def __init__(self):
        self.script_name = os.path.split(__file__)[-1]
        self.script_dir = os.path.abspath(".")
        self.action = ""
        self.confLists = []
        self.datadir = ""
        
        self.confKeyValues = {}
        self.confFile = ""
        
#############################################################################
# Functions
#############################################################################
def usage():
    """
Usage:
    gs_guc --help | -h | -?
    gs_guc --version | -v
    gs_guc {set | reload} -c "parameter=value" [-c "parameter=value"] [...] [-D datadir]
    """
    
    print usage.__doc__
    
def printVersion():
    
    print "%s version 1.0.0" % g_opts.script_name
    
def exitWithError(msg, status=1):
    sys.stderr.write("%s\n" % msg)
    sys.exit(status)
    
def parseCommandLine():
    """
    Parse command line
    """
    if (len(sys.argv) < 2):
        usage()
        exitWithError("Parameter input error!")
    
    key = sys.argv[1].strip()
    if (key == "-?" or key == "--help"):
        usage()
        sys.exit(0)
    elif (key == "-v" or key == "--version"):
        printVersion()
        sys.exit(0)
    else:
        g_opts.action = key
    
    try:
        (opts, args) = getopt.getopt(sys.argv[2:], "c:D:")
    except Exception, e:
        usage()
        exitWithError("Error: %s" % str(e))
    
    if (len(args) > 0):
        exitWithError("Parameter input error: %s" % args[0])
        
    for (key, value) in opts:
        if (key == "-c"):
            g_opts.confLists.append(value)
        elif (key == "-D"):
            g_opts.datadir = value
    
def checkParameter():
    """
    Check parameters
    """
    if (g_opts.action != "set" and g_opts.action != "reload"):
        exitWithError("Parameter input error!Unkown action: %s." % g_opts.action)
    
    if (len(g_opts.confLists) == 0):
        exitWithError("Parameter input error! '-c' is needed!")

    # Check the config item
    for item in g_opts.confLists:
        idx = item.find("=")
        if (idx == -1):
            exitWithError("Error conifg parameter: %s" % item)
        
        key = item[:idx].strip()
        value = item[idx+1:].strip()
        if (key == "" or value == ""):
            exitWithError("Error conifg parameter: %s" % item)
        
        g_opts.confKeyValues[key] = value
        
    if (g_opts.datadir == ""):
        envDatadir = os.environ.get("GAUSSDATA")
        if (envDatadir is None):
            exitWithError("Parameter '-D' is needed or set environment variable 'GAUSSDATA'!")
        g_opts.datadir = envDatadir
    
    g_opts.confFile = "%s/postgresql.conf" % g_opts.datadir
    if (not os.path.exists(g_opts.confFile)):
        exitWithError("Config file does not exist!Path : %s" % g_opts.confFile)
        
def doModify():
    """
    Modify parameters
    """
    for pair in g_opts.confKeyValues.items():
        modifyConfigItem(pair[0], pair[1])
        
    if (g_opts.action == "reload"):
        reloadConfig()
    
def modifyConfigItem(key, value):
    """
    Modify a parameter
    """
    # comment out any existing entries for this setting
    cmd = "perl -pi.bak -e's/(^\s*%s\s*=.*$)/#$1/g' %s" % (key, g_opts.confFile)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        exitWithError("Comment parameter failed!Error:%s" % output)
        
    # append new config to file
    cmd = 'echo "%s = %s" >> %s' %  (key, value, g_opts.confFile)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        exitWithError("Append new vaule failed!Error:%s" % output)
        
def reloadConfig():
    """
    Notify postmaster to reload config
    """
    # Find pg_ctl
    binPath = ""
    binDirs = (".", "$GAUSSHOME/bin")
    
    for dir in binDirs:
        cmd = "ls %s/pg_ctl" % dir
        (status, output) = commands.getstatusoutput(cmd)
        if (status == 0):
            binPath = dir
    
    if (binPath == ""):
        exitWithError("Can not find pg_ctl!")
    
    cmd = "%s/pg_ctl reload -D %s" % (binPath, g_opts.datadir)
    (status, output) = commands.getstatusoutput(cmd)
    if (status != 0):
        exitWithError("Reload config failed!Error:%s" % output)
        
if __name__ == '__main__':
    
    g_opts = cmdOptions()
    parseCommandLine()
    
    checkParameter()
    
    doModify()
    
    print "config successfully!"
    sys.exit(0)
    