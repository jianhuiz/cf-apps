'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import shutil
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import PlatformCommand, DefaultValue
from script.util.OMCommand import OMCommand

class Uninstall:
    '''
    classdocs
    '''
    
    def __init__(self):
        '''
        Constructor
        '''
        self.installPath = ""
        self.user = ""
        self.keepDir = False
        self.mpprcFile = ""
        self.logFile = ""
        self.logger = None
        self.cleanInstallBin = False
        
    ####################################################################################
    # Help context. U:R:oC:v: 
    ####################################################################################
    def usage(self):
        print("Uninstall.py is a utility to uninstall Gauss MPP Database.")
        print(" ")
        print("Usage:")
        print("  python Uninstall.py --help")
        print("  python Uninstall.py -U user -R installpath [-c] [-l log]")
        print(" ")
        print("Common options:")
        print("  -U         the database program and cluster owner")
        print("  -R         the database program install path")
        print("  -l         the log path")
        print("  -c         clean bin in install path")
        print("  --help     show this help, then exit")
        print(" ")
        
    ####################################################################################
    # This is the main uninstall flow.  
    ####################################################################################
    def uninstall(self):
        '''
        1. We remove install path content, which depend on $GAUSSHOME
        '''
        self.__checkParameters()
        self.__initLogger()
        self.__cleanInstallProgram()
        self.__cleanMonitor()
        self.__cleanUserEnvVariable()
        self.logger.closeLog()

    def __cleanUserEnvVariable(self):
        """
        Clean os user environment variable
        """
        #clean os user environment variable
        self.logger.log("Begin clean user environment variable...")
        try:
            if (self.mpprcFile != ""):
                userProfile = self.mpprcFile
            else:
                userProfile = '~/.bashrc'
				
            if(self.cleanInstallBin):
                cmd = "(if [ -s %s ]; then " % userProfile
                # clean version
                cmd += "sed -i '/^\\s*export\\s*GAUSS_VERSION=.*$/d' %s; " % userProfile
                #clean bin
                cmd += "sed -i '/^\\s*export\\s*PATH=\\$GAUSSHOME\\/bin:\\$PATH$/d' %s; " % userProfile
                #clean GAUSSHOME
                cmd += "sed -i '/^\\s*export\\s*GAUSSHOME=.*$/d' %s; " % userProfile
                cmd += "sed -i '/^\\s*export\\s*PGHOST=.*$/d' %s; " % userProfile
                #clean GAUSSLOG
                cmd += "sed -i '/^\\s*export\\s*GAUSSLOG=.*$/d' %s; " % userProfile
                #clean GS_CLUSTER_NAME
                cmd += "sed -i '/^\\s*export\\s*GS_CLUSTER_NAME=.*$/d' %s; fi) " % userProfile
                (status, output) = commands.getstatusoutput(cmd)
                if (status != 0):
                   self.logger.logExit("Clean os user environment variable failed: %s." % output)
                
        except Exception, e:
            self.logger.logExit("Clean os user environment variable failed: %s" % str(e))
        self.logger.log("End clean user environment variable")
  
    def __cleanMonitor(self):
        """
        clean om_monitor process and delete cron
        """
        self.logger.log("Begin clean monitor...")
        try:
            # Remove cron
            crontabFile = "%s/gauss_crontab_file_%d" % (DefaultValue.getTmpDirFromEnv(), os.getpid())
            cmd = "crontab -l > %s; " % crontabFile
            cmd += "sed -i '/\\/bin\\/om_monitor/d' %s; " % crontabFile
            cmd += "crontab %s; " % crontabFile
            cmd += "rm -f %s " % crontabFile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                #no need raise error here, user can do it manually.
                self.logger.debug("Delete crontab failed: %s\n You can do it manually." % output)
            
            # clean om_monitor process
            pidList = []
            cmd = "pgrep -U %s om_monitor" % self.user
            (status, output) = commands.getstatusoutput(cmd)
            if (status == 0):
                pidList = output.split("\n")
            self.logger.debug("The list of process id is:%s" % pidList)
            
            for pid in pidList:
                PlatformCommand.KillProcess(pid)
        except Exception, e:
            self.logger.logExit("Clean monitor failed: %s" % str(e))
        self.logger.log("End clean monitor")
        
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:R:l:c", ["help"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)

        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))
        
        for key, value in opts:
            if(key == "-U"):
                self.user = value
            elif(key == "-R"):
                self.installPath = value
            elif (key == "-l"):
                self.logFile = value
            elif(key == "--help"):
                self.usage()
                sys.exit(0)
            elif(key == "-c"):
                self.cleanInstallBin = True
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")

        if(self.user == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        
        if (self.installPath == ""):
            GaussLog.exitWithError("Parameter input error, need '-R' parameter.")

        self.mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(self.mpprcFile == None):
            self.mpprcFile = ""
        if(self.mpprcFile != ""):
            if (not os.path.exists(self.mpprcFile)):
                GaussLog.exitWithError("mpprc file does not exist: %s" % self.mpprcFile)
            if (not os.path.isabs(self.mpprcFile)):
                GaussLog.exitWithError("Parameter input error, mpprc file need absolute path.")
        
        if (self.logFile == ""):
            self.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, "", self.installPath)
            
    def __initLogger(self):
        """
        """
        self.logger = GaussLog(self.logFile, "UninstallApp")
   
        
    def __cleanInstallProgram(self):
        '''
        '''
        if (not os.path.exists(self.installPath)):
            self.logger.log("Install directory does not exist!")
            return
        
        self.logger.log("Begin clean LD_LIBRARY_PATH environment variable")
        if (self.mpprcFile != ""):
            userProfile = self.mpprcFile
        else:
            userProfile = '~/.bashrc'
        cmd = "sed -i '/^\\s*export\\s*LD_LIBRARY_PATH=\\$GAUSSHOME\\/lib:\\$LD_LIBRARY_PATH$/d' %s " % userProfile
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean LD_LIBRARY_PATH environment variable failed: %s." % output)
        self.logger.log("End clean LD_LIBRARY_PATH environment variable")

        self.logger.log("Begin remove install directory...")
        try:
            fileList = os.listdir(self.installPath)
            for fileName in fileList:
                filePath = os.path.join(self.installPath, fileName)
                if os.path.isfile(filePath):
                    os.remove(filePath)
                elif os.path.isdir(filePath):
                    if(fileName == "bin"):
                        binFileList = os.listdir(filePath)
                        for binFile in binFileList:
                            fileInBinPath = os.path.join(filePath, binFile)
                            if os.path.isfile(fileInBinPath) and binFile != "cluster_static_config":
                                os.remove(fileInBinPath)
                            elif os.path.islink(fileInBinPath):
                                os.remove(fileInBinPath)
                            elif os.path.isdir(fileInBinPath):
                                shutil.rmtree(fileInBinPath)
                    else:
                        shutil.rmtree(filePath)
                        
                self.logger.debug("Remove path:%s" % filePath)

            if(self.cleanInstallBin):
                self.logger.debug("Begin Clean bin in install path...")
                cmd = "rm -rf %s/bin " % self.installPath
                (status, output) = commands.getstatusoutput(cmd)
                if (status != 0):
                    self.logger.debug("Delete bin in install path failed: %s\n You can do it manually." % output)
                self.logger.debug("Clean bin in install path finished.")

        except Exception, e:
            self.logger.logExit("Remove install directory failed, can not delete install directory: " + str(e))
        self.logger.log("End remove install directory")

if __name__ == '__main__':
    """
    main function
    """
    uninstaller = Uninstall()
    uninstaller.uninstall()

    sys.exit(0)
