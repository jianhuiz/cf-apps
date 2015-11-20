'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import platform
import commands
import statvfs
import math
import re

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue, PlatformCommand

########################################################################
# Global variables define
########################################################################

class CheckInstall:
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.installPath = ""
        self.user = ""
        self.group = ""
        self.userProfile = ""
        self.replaceCheck = False
        self.mpprcFile = ""

        # DB config parameters
        self.confParameters = []
        self.platformString = platform.system()
        self.logger = None
        
    ####################################################################################
    # Help context. U:R:oC:v: 
    ####################################################################################
    def usage(self):
        print("GaussInstall.py is a utility to install Gauss MPP Database.")
        print(" ")
        print("Usage:")
        print("  python GaussInstall.py --help")
        print("  python install.py -U user:group -R installpath [--replace]")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  -R        the database program path")
        print("  -C        configure the database config file, for more detail information see postgresql.conf")
        print("  --replace do check install for replace")
        print("  --help    show this help, then exit")
        print(" ")

    ####################################################################################
    # check install  
    ####################################################################################
    def checkInstall(self):
        '''
        Check all kinds of environment. It includes: 
        1. Input parameters.
        2. OS version .
        3. If it has a old install.
        4. OS kernel parameters.
        5. Install directory size and stauts.
        6. Security.
        7. Binary file integrity verify. 
        '''
        try:
            self.__checkParameters()
            self.__checkOldInstall()
            self.__checkOSKernel()
            self.__checkInstallDir()
            self.__checkSHA256()
        except Exception, e:
            self.logger.logExit("Install step failed!:%s" % str(e))
        self.logger.closeLog()
        
        
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:R:C:l:", ["help", "replace"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: %s" % str(e))
        
        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))

        logFile = ""
        userInfo = ""
        for key, value in opts:
            if(key == "--help"):
                self.usage()
                sys.exit(0)
            elif(key == "-U"):
                userInfo = value
            elif(key == "-R"):
                self.installPath = value
            elif(key == "-C"):
                self.confParameters.append(value)
            elif (key == "-l"):
                logFile = value
            elif(key == "--replace"):
                self.replaceCheck = True
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")

        #check user info
        if (userInfo == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        strList = userInfo.split(":")
        if(len(strList) != 2):
            GaussLog.exitWithError("Parameter input error: user and group should split by ':'.")
        self.user = strList[0].strip()
        self.group = strList[1].strip()
        if(self.user == "" or self.group == ""):
            GaussLog.exitWithError("Parameter input error: user or group is empty!")
        self.__checkOsUser()

        #check log file info
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, self.user, "", "")
        self.logger = GaussLog(logFile, "CheckInstall")

        #check mpprc file path
        self.mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(self.mpprcFile == None):
            self.mpprcFile = ""
        if(self.mpprcFile != ""):
            if (not os.path.exists(self.mpprcFile)):
                GaussLog.exitWithError("mpprc file does not exist: %s" % self.mpprcFile)    
            if (not os.path.isabs(self.mpprcFile)):
                GaussLog.exitWithError("mpprc file need absolute path:%s" % self.mpprcFile)

        #check install path
        if(self.installPath == ""):
            GaussLog.exitWithError("Parameter input error, need '-R' parameter.")
        self.installPath = os.path.normpath(self.installPath)
        self.installPath = os.path.abspath(self.installPath)
        if(not self.__checkPath(self.installPath)):
            self.logger.logExit("Install program path invalid: " + self.installPath)
        self.logger.debug("Using install program path:%s" % self.installPath)
        self.logger.debug("Using set config parameters:%s" % str(self.confParameters))
        
    def __checkOsUser(self):
        """
        Check os user if exist
        """
        cmd = "id -gn '%s'" % self.user
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            GaussLog.exitWithError("User[%s] does no exist!Output: %s" % (self.user, output))
        
        if (output != self.group):
            GaussLog.exitWithError("User not in the group[%s]." % self.group)

        #get user env file
        if(self.mpprcFile == ""):
            cmd = "echo ~ 2>/dev/null"
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                GaussLog.exitWithError("Can not get user home.")
            self.userProfile = os.path.join(output, ".bashrc")
        else:
            self.userProfile = self.mpprcFile
    
    def __checkPath(self, path_type_in):
        path_len = len(path_type_in)
        i = 0
        a_ascii = ord('a')
        z_ascii = ord('z')
        A_ascii = ord('A')
        Z_ascii = ord('Z')
        num0_ascii = ord('0')
        num9_ascii = ord('9')
        blank_ascii = ord(' ')
        sep1_ascii = ord('/')
        sep2_ascii = ord('_')
        sep3_ascii = ord('-')
        for i in range(0, path_len):
            char_check = ord(path_type_in[i])
            if(not (a_ascii <= char_check <= z_ascii or A_ascii <= char_check <= Z_ascii or num0_ascii <= char_check <= num9_ascii or char_check == blank_ascii or char_check == sep1_ascii or char_check == sep2_ascii or char_check == sep3_ascii)):
                return False
        return True
    
    def __checkOldInstall(self):
        '''
        Check old database install. If this user have old install, report error and exit.
        ''' 
        self.logger.log("Begin check old install...")
        # Check $GAUSSHOME.
        fp = None
        isFind = False
        try:
            fp = open(self.userProfile, "r")
            while True:
                strLine = fp.readline()   
                if(not strLine):
                    break
                strLine = strLine.strip()
                if(strLine.startswith("#")):
                    continue
                if(re.match("^\\s*export\\s*GAUSSHOME=.*$", strLine) != None):
                    isFind = True
                    break
                else:
                    continue
        except Exception, ex:
            self.logger.logExit("Can not read user profile: " + str(ex))
        finally:
            if(fp):
                fp.close()
        if(isFind):
            self.logger.logExit("The environment variable $GAUSSHOME is found in .bashrc file, maybe the database has been installed already.")
            
        self.logger.log("End check old install")
                
    def __checkOSKernel(self):
        '''
        Check OS kernel parameters: share memory size and semaphore.(postgresql.conf/gtm.conf)
        '''
        self.logger.log("Begin check kernel parameters...")
        if(self.platformString == "Linux"):
            # GB MB kB
            GB = 1 * 1024 * 1024 * 1024
            MB = 1 * 1024 * 1024
            kB = 1 * 1024
            shared_buffers = 1 * GB
            max_connections = 800

            for item in self.confParameters:
                tmp = item.strip()
                list = tmp.split("=")
                try:
                    if(cmp(list[0].lower(), "shared_buffers") == 0):
                        if((list[1][0:-2].isdigit() == True) and cmp(list[1][-2:], "GB") == 0):
                            shared_buffers = int(list[1][0:-2]) * GB
                        if((list[1][0:-2].isdigit() == True) and cmp(list[1][-2:], "MB") == 0):
                            shared_buffers = int(list[1][0:-2]) * MB
                        if((list[1][0:-2].isdigit() == True) and cmp(list[1][-2:], "kB") == 0):
                            shared_buffers = int(list[1][0:-2]) * kB
                        if((list[1][0:-1].isdigit() == True) and cmp(list[1][-1:], "B") == 0):
                            shared_buffers = int(list[1][0:-1])
                    if(cmp(list[0].lower(), "max_connections") == 0):
                        if(list[1].isdigit() == True):
                            max_connections = int(list[1])
                except ValueError, ex:
                    self.logger.logExit("check kernel parameter failed: " + str(ex))

            # check shared_buffers
            strCmd = "cat /proc/sys/kernel/shmmax"
            status, shmmax = commands.getstatusoutput(strCmd)
            if (status != 0):
                self.logger.logExit("can not get shmmax parameters.")
            strCmd = "cat /proc/sys/kernel/shmall"
            status, shmall = commands.getstatusoutput(strCmd)
            if (status != 0):
                self.logger.logExit("can not get shmall parameters.")
            strCmd = "getconf PAGESIZE"
            status, PAGESIZE = commands.getstatusoutput(strCmd)
            if (status != 0):
                self.logger.logExit("can not get PAGESIZE.")
            if(shared_buffers < 128 * kB):
                self.logger.logExit("shared_buffers should bigger than or equal to 128kB, please check it!") 
            try:
                if(shared_buffers > int(shmmax)):
                    self.logger.logExit("shared_buffers should smaller than shmmax, please check it!")
                if(shared_buffers > int(shmall) * int(PAGESIZE)): 
                    self.logger.logExit("shared_buffers should smaller than shmall*PAGESIZE, please check it!")
            except ValueError, ex:
                self.logger.logExit("check kernel parameter failed: " + str(ex))
            # check sem
            strCmd = "cat /proc/sys/kernel/sem"
            status, output = commands.getstatusoutput(strCmd)
            if (status != 0):
                self.logger.logExit("can not get sem parameters.") 
            paramList = output.split("\t")
            try:
                if(int(paramList[0]) < 17):
                    self.logger.logExit("It occurs when the system limit for the maximum number of semaphores per set (SEMMSL), and current SEMMSL value is: " + 
                    paramList[0] + ", please check it!") 
                if(int(paramList[3]) < math.ceil((max_connections + 150) / 16)):
                    self.logger.logExit("It occurs when the system limit for the maximum number of semaphore sets (SEMMNI), and current SEMMNI value is: " + paramList[3] + ", please check it!")
                if(int(paramList[1]) < math.ceil((max_connections + 150) / 16) * 17):
                    self.logger.logExit("It occurs when the system limit for the maximum number of semaphores (SEMMNS), and current SEMMNS value is: " + 
                    paramList[1] + ", please check it!") 
            except ValueError, ex:
                self.logger.logExit("check kernel parameter failed: " + str(ex))
        if(self.platformString == "Windows"):
            pass
        self.logger.log("End check kernel parameters")
        
    def __checkInstallDir(self):
        '''
        Check database program file install directory size.
        The free space size should not smaller than 100M.
        '''
        self.logger.log("Begin check dir...")
        if(self.platformString == "Linux"):
            # check if install path exists
            if(not os.path.exists(self.installPath)):
                self.logger.logExit("Database program install path[%s] does not exist.\n Please create it first." % self.installPath)
                
            # check install path is empty or not.
            fileList = os.listdir(self.installPath)
            if(len(fileList) != 0):
                #cluster_static_config may exists, but we should not use it.
                if(len(fileList) == 1 and fileList[0] == "bin" and len(os.listdir("%s/bin" % self.installPath)) == 1 and os.path.exists("%s/bin/cluster_static_config" % self.installPath)):
                    pass
                else:
                    self.logger.logExit("Database program install path should be empty.")
                
            # check install path uasge
            vfs = os.statvfs(self.installPath)
            availableSize = vfs[statvfs.F_BAVAIL] * vfs[statvfs.F_BSIZE] / (1024 * 1024)
            self.logger.log("Database program install path available size %sM" % str(availableSize))
            if(availableSize < 100):
                self.logger.logExit("Database program install path available size smaller than 100M, current size is: " + str(availableSize) + "M")
        if(self.platformString == "Windows"):
            pass
        
        self.logger.log("End check dir")


    def __checkSHA256(self):
        '''
        Check the sha256 number for database install binary file.
        '''
        self.logger.log("Begin check sha256...")
        try:
            binPath = DefaultValue.getBinFilePath()
            sha256Path = DefaultValue.getSHA256FilePath()
            self.logger.debug("binPath:%s sha256Path:%s" % (binPath, sha256Path))
            
            fileSHA256 = PlatformCommand.getFileSHA256(binPath)
            sha256Value = PlatformCommand.readFileLine(sha256Path)
            if(fileSHA256 != sha256Value):
                self.logger.logExit("The sha256 value is different!\nBin file%s\nSHA256 file:%s." % (fileSHA256, sha256Value))
        except Exception, e:
            self.logger.logExit("Check sha256 failed.Error: %s" % str(e))
        self.logger.log("End check sha256")
    
if __name__ == '__main__':
    """
    main function
    """    
    checker = CheckInstall()
    checker.checkInstall()

    sys.exit(0)