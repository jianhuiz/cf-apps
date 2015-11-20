'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import string
import platform
import logging
import commands
import shutil
import getpass
import statvfs
import time
import math
import string
import errno
import socket
import hashlib

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue

########################################################################
# Global variables define
########################################################################
logFile = "StopLocalServer.log"

logger = None

class StopLocalServer():
    '''
    classdocs
    '''
    

    def __init__(self, installPath, nodename, datadirs):
        '''
        Constructor
        '''
        self.installPath = installPath
        self.nodename = nodename
        self.datadirs = datadirs
        
        self.__user = ""
        self.__group = ""        
        
        self.__logDir = DefaultValue.getOMLogPath(DefaultValue.GURRENT_DIR_FILE, "", installPath)
        
    ####################################################################################
    # This is the main install flow.  
    ####################################################################################
    def run(self):
        '''
        check install 
        '''
        self.iniLogger()
        
        try:
            self.getUserInfo()
            self.__SwitchoverPrimaryInstances()
            self.__StopALLTheInstances()
        except Exception,e:
            logger.logExit("Stop Local Server Failed: %s" % str(e))     
            
        logger.closeLog()  

    def iniLogger(self):
        cmd = "mkdir -p %s" % self.__logDir
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            exitWithError("Create log directory failed!Error:%s" % output)        
        global logger    
        logger = GaussLog(os.path.join(self.__logDir,logFile))   
        
    def getUserInfo(self):
        """
        Get user and group
        """
        logger.log("Getting user and group for application...")
        cmd = "stat -c '%%U:%%G' %s" % self.installPath
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            logger.logExit("Get user info failed!Error :%s" % output)
        
        userInfo = output.split(":")
        if (len(userInfo) != 2):
            logger.logExit("Get user info failed!Error :%s" % output)
        
        self.__user = userInfo[0]
        self.__group = userInfo[1]  

    def __SwitchoverPrimaryInstances(self):
        '''
        Before we shutdown all the instances on this node. we switchover all the primary instances first.
        '''
        for dir in self.datadirs:
            cmd = "su - %s -c \'%s/bin/cm_ctl swithover -n %s -D %s -t 30\'" % (self.__user,self.installPath,self.nodename,dir)
            logger.log("switchover the instance %s. success." % dir)
        
    def __StopALLTheInstances(self):
        '''
        stop instances
        '''
        cmd = "su - %s -c \'%s/bin/cm_ctl stop -n %s -m fast\'" % (self.__user,self.installPath,self.nodename) 
        logger.log("stop the instance %s. success." % dir)            

####################################################################################
# Help context. U:R:oC:v: 
####################################################################################
def usage(self):
    print("StopLocalServer.py is a utility to stop the local instances.")
    print(" ")
    print("Usage:")
    print("  python StopLocalServer.py --help")
    print(" ")
    print("Common options:")
    print("  -R        the database program path")
    print("  -n        the nodename")
    print("  -D        the instance data dir that we should stop")
    print("  --help    show this help, then exit")
    print(" ")
    
def exitWithError(msg, status=1):
    sys.stderr.write("%s\n" % msg)
    sys.exit(status)
    
def writeStderr(msg):
    sys.stderr.write("%s\n" % msg)
    
def main():
    """
    main function
    """
    writeStderr("Stop local server begin...")
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "D:R:n:", ["help"])
    except getopt.GetoptError, e:
        exitWithError("Parameter input error: " + e.msg)
    if(len(args) > 0):
        exitWithError("Parameter input error: " + str(args[0]) )
        
    installPath = ""
    nodename = ""
    datadirs = []

    for key, value in opts:
        if(key == "-R"):
            installPath = value.strip()
        elif(key == "-D"):
            path = value.strip()
            if (os.path.exists(path)):
                datadirs.append(path)
            else:
                writeStderr("the datadir %s is not existed.just ignore it." % path)
        elif(key == "-n"):
            nodename = value
        elif(key == "--help"):
            usage()
            sys.exit(0)
        else:
            exitWithError("Parameter input error: " + value + ".")
    
    if(installPath == ""):
        exitWithError("Parameter input error, need '-R' parameter.")        
    if(not os.path.exists(installPath)):
        exitWithError("Parameter Invalid. -R %s is not existed." % installPath)
    
    hostname = socket.gethostname()
    if (hostname != nodename):
        exitWithError("The hostname %s is not match to the nodename %s." % (hostname,nodename)) 
        
    if (datadirs == []):
        exitWithError("No exist datadir found. Stop local server finished.", 0)
        
    try:
        stopper = StopLocalServer(installPath, nodename, datadirs)
        stopper.run()
    except Exception,e:
        exitWithError("Stop local server failed! Error:%s" % str(e))

    writeStderr("Stop local server succeed.")

if __name__ == '__main__':
    if(os.getgid() != 0):
        exitWithError("Only user with root privilege can run this script")    
    main()
