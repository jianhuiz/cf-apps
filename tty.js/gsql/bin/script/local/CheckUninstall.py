'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import platform
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue,PlatformCommand

class CheckUninstall:
    '''
    classdocs
    '''
    
    def __init__(self):
        '''
        Constructor
        '''
        self.installPath = ""
        self.user = ""
        self.cleanUser = False
        self.cleanData = False
        self.logger = None
        
    ####################################################################################
    # Help context. U:R:oC:v: 
    ####################################################################################
    def usage(self):
        print("GaussInstall.py is a utility to install Gauss MPP Database.")
        print(" ")
        print("Usage:")
        print("  python GaussUninstall.py --help")
        print("  python GaussUninstall.py -R installpath -U user [-d] [-u] [-l log]")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  -R        the database program path")
        print("  -d        clean data path")
        print("  -u        clean user")
        print("  -l        log directory")
        print("  --help    show this help, then exit")
        print(" ")
        
    ####################################################################################
    # check uninstall  
    ####################################################################################
    def checkUninstall(self):
        '''
        Check all kinds of environment. It includes: 
        1. Input parameters.
        2. OS version .
        3. User Info
        4. If it has a old install.
        '''
        self.__checkParameters()
        self.__checkOSVersion()
        self.__checkOsUser()
        self.__checkInstanllPath()
        self.logger.closeLog()
        
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:R:l:du", ["help"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)

        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))

        logFile = ""
        for key, value in opts:
            if(key == "-U"):
                self.user = value
            elif(key == "-R"):
                self.installPath = value
            elif (key == "-l"):
                logFile = value
            elif(key == "-d"):
                self.cleanData = True
            elif(key == "-u"):
                self.cleanUser = True
            elif(key == "--help"):
                self.usage()
                sys.exit(0)
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")

        if(self.user == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")

        if (self.installPath == ""):
            self.logger.logExit("Parameter input error, need '-R' parameter.")
                
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", self.installPath, "")
        
        self.logger = GaussLog(logFile, "CheckUninstall")
        self.logger.debug("Using install program path : " + self.installPath)
        self.logger.debug("Using clean user : %s" % self.cleanUser)
        self.logger.debug("Using clean data : %s" % self.cleanData)
            
    def __checkOSVersion(self):
        '''
        Check operator system version and install binary file version.
        '''
        self.logger.log("Begin check OS version...")
        if (not PlatformCommand.checkOsVersion()):
            self.logger.logExit("can't support current system,current system is : %s" % platform.platform())
        
        self.logger.log("End check OS version")
    
    def __checkOsUser(self):
        """
        Check if user exists and get $GAUSSHOME
        """
        if(self.cleanUser == False):
            self.logger.log("Skip user check!")
            return
            
        self.logger.log("Begin check OS user...")
        try:
            PlatformCommand.checkUser(self.user, False)
        except Exception as e:
            self.logger.logExit(str(e))
            
        # Get GAUSSHOME
        cmd = "echo $GAUSSHOME 2>/dev/null"
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            self.logger.logExit("Get $GAUSSHOME failed!Error:%s" % output)
        
        gaussHome = output.strip()
        if (gaussHome == ""):
            self.logger.logExit("$GAUSSHOME is null!")
            
        if (gaussHome != self.installPath):
            self.logger.debug("$GAUSSHOME : %s" % gaussHome)
            self.logger.debug("Install path parameter: %s" % self.installPath)
            self.logger.logExit("$GAUSSHOME of user is not equal to install path!")
        self.logger.log("End check OS user")
            
    def __checkInstanllPath(self):
        """
        Check if path exists and get owner
        """
        self.logger.log("Begin check install path...")
        if (not os.path.exists(self.installPath)):
            self.logger.log("Install path does not exist: %s!" % self.installPath)
            if (not self.cleanData and not self.cleanUser):
                self.logger.logExit("Uninstall check failed!")
        else:       
            # Get owner
            cmd = "stat -c '%%U:%%G' %s" % self.installPath
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Get owner of path failed!Error :%s" % output)
            
            owerInfo = output.strip() 
            (user, group) = owerInfo.split(':')  
            if (self.user != user.strip()):
                self.logger.debug("Install path owner info : %s" % owerInfo)
                self.logger.debug("User parameter : %s" % self.user)
                self.logger.logExit("The user is not the owner of application!")
        self.logger.log("End check install path")


if __name__ == '__main__':
    """
    main function
    """    
    checker = CheckUninstall()
    checker.checkUninstall()
    
    sys.exit(0)
