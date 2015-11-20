'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue, PlatformCommand

class CleanOsUser:
    '''
    This class is for cleaning os user, it will not cleaning group.
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.userProfile = ""
        self.user = ""
        self.logger = None

    ####################################################################################
    # Help context. 
    ####################################################################################
    def usage(self):
        print("CleanOsUser.py is a utility to clean Gauss MPP Database instance.")
        print(" ")
        print("Usage:")
        print("  python CleanOsUser.py --help")
        print("  python CleanOsUser.py -U user")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  --help    show this help, then exit")
        print(" ")
    
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:l:", ["help"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)

        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))

        logFile = ""
        for key, value in opts:
            if(key == "-U"):
                self.user = value
            elif(key == "-l"):
                logFile = value
            elif(key == "--help"):
                self.usage()
                sys.exit(0)
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")
        
        if(self.user == ""):
            GaussLog.exitWithError("Parameter input error, need -U parameter.")
        try:
            PlatformCommand.checkUser(self.user, False)
        except Exception as e:
            GaussLog.exitWithError(str(e))
        
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, self.user, "")

        self.logger = GaussLog(logFile, "CleanOsUser")
        
    ####################################################################################
    # This is the main clean OS user flow.  
    ####################################################################################
    def cleanOsUser(self):
        '''
        Clean os user 
        '''
        self.__checkParameters()
        self.logger.log("Begin clean os user...")
        try:
            #clean semaphore
            commands.getstatusoutput("ipcs -s|awk '/ %s /{print $2}'|xargs -n1 ipcrm -s" %  self.user)

            #get install path
            cmd = "su - %s -c 'echo $GAUSSHOME' 2>/dev/null" % self.user
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                self.logger.logExit("Get $GAUSSHOME failed!Error:%s" % output)
            gaussHome = output.strip()
            if (gaussHome == ""):
                self.logger.debug("$GAUSSHOME is null, this means you may need to clean install path manually!")
            self.logger.debug("The install path is %s" % gaussHome)
            
            #delete user
            status, output = commands.getstatusoutput("userdel -rf %s; rm -rf %s" % (self.user, gaussHome))
            if(status != 0):
                self.logger.logExit("clean user %s failed. " % self.user)
                
        except Exception, e:
            self.logger.logExit("clean user : " + self.user + " failed, " + str(e))
        self.logger.log("End clean os user")

if __name__ == '__main__':
    """
    main function
    """
    cleaner = CleanOsUser()
    cleaner.cleanOsUser()
    
    sys.exit(0)
