import os
import sys
import getopt
import commands
sys.path.append(sys.path[0] + "/../")
from script.util.SshTool import SshTool
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.Common import DefaultValue

g_user = ""

class CmdOptions():
    """
    """
    def __init__(self):
        self.cmd = ""
        self.parameterlist = ""

def usage():
    """
ClusterCall.py is a utility to execute same command on all nodes.

Usage:
 python ClusterCall.py -h | --help
 python ClusterCall.py -c cmd [-p parameterlist]

Common options:
 -c                             the command name
 -p                             the command parameter list
 -h --help                      show this help, then exit
    """
    print usage.__doc__

def parseCommandLine():
    """
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "c:p:h", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError(str(e))
    
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif(key == "-c"):
            g_opts.cmd = value
        elif(key == "-p"):
            g_opts.parameterlist = value
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)
    if(g_opts.cmd == ""):
        GaussLog.exitWithError("Parameter input error, need '-c' parameter.")

def initGlobal():
    """
    Init logger
    """
    global g_clusterInfo
    global g_sshTool
    global g_user
    
    try:
        cmd = "id -un" 
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            GaussLog.exitWithError("Get user info failed")  
        g_user = output
        g_clusterInfo = dbClusterInfo()
        g_clusterInfo.initFromStaticConfig(output)
        g_sshTool = SshTool(g_clusterInfo.getClusterNodeNames())
    except Exception, e:
        GaussLog.exitWithError(str(e))

def executeCommand():
    """
    """
    failedNodes = ""
    succeedNodes = ""
    try:
        command = (g_opts.cmd.strip()).split(" ")
        if(len(command) > 1):
            GaussLog.exitWithError("Parameter -c input error, only need command.")
        checkCmd = "which %s" % command[0]
        (status, output) = g_sshTool.getSshStatusOutput(checkCmd)
        for node in status.keys():
            if(status[node] != 0):
                failedNodes += "%s " % node
            else:
                succeedNodes += "%s " % node
        if(failedNodes != ""):
            GaussLog.exitWithError("Command %s not exist or not have executable permissions on %s." % (command, failedNodes))
        failedNodes = ""
        succeedNodes = ""
        executeCmd = g_opts.cmd + " " + g_opts.parameterlist
        #############################################################
        cmdFile = "%s/ClusterCall_%d.sh" % (DefaultValue.getTmpDirFromEnv(), os.getpid())
        cmdCreateFile = "touch %s" % cmdFile 
        (status, output) = commands.getstatusoutput(cmdCreateFile)
        if(status != 0):
            GaussLog.exitWithError("Touch file %s failed!" % cmdFile)

        cmdFileMod = "chmod 640 %s" % cmdFile
        (status, output) = commands.getstatusoutput(cmdFileMod)
        if(status != 0):
            GaussLog.exitWithError("Chmod file %s failed!" % cmdFile)
            
        fp = open(cmdFile, "a")
        fp.write("#!/bin/sh")
        fp.write(os.linesep)
        fp.write("%s" % (executeCmd))
        fp.write(os.linesep)
        fp.flush()
        fp.close()

        ##############################################################
        cmdDir = DefaultValue.getTmpDirFromEnv()
        g_sshTool.scpFiles(cmdFile, cmdDir)
        cmdExecute = "sh %s" % cmdFile
        (status, output) = g_sshTool.getSshStatusOutput(cmdExecute)
        
        for node in status.keys():
            if(status[node] != 0):
                failedNodes += "%s " % node
            else:
                succeedNodes += "%s " % node
        if(failedNodes != "" and succeedNodes != ""):
            GaussLog.printMessage("Execute command failed on %s." % failedNodes)
            GaussLog.printMessage("Execute command succeed on %s.\n" % succeedNodes)
        elif(failedNodes == ""):
            GaussLog.printMessage("Execute command succeed on all nodes.\n")
        elif(succeedNodes == ""):
            GaussLog.printMessage("Execute command failed on all nodes.\n")
            
        GaussLog.printMessage("Output:\n%s" % output)

        cmdFileRm = "rm %s" % cmdFile
        g_sshTool.executeCommand(cmdFileRm, "rm cmdFile")
                
    except Exception, e:
        if(fp):fp.close()
        cmdFileRm = "rm %s" % cmdFile
        g_sshTool.executeCommand(cmdFileRm, "rm cmdFile")
        GaussLog.exitWithError(str(e))
    
def main():
    """
    main function
    """
    if(os.getgid() == 0):
        GaussLog.exitWithError("Can not use root privilege user run this script")

    #parse cmd lines
    parseCommandLine()

    #init globals
    initGlobal()
    
    #execute command
    executeCommand()

    sys.exit(0)

#the may entry for this script
main()