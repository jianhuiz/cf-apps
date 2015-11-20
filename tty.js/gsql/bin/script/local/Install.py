'''
Created on 2014-2-7

@author: 
'''
import getopt
import os
import sys
import commands
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import VersionInfo, DefaultValue, PlatformCommand
from script.util.DbClusterInfo import *

class Install:
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.installPath = ""
        self.logPath = ""
        self.tmpPath = ""
        self.user = ""
        self.group = ""
        self.clusterName = ""
        self.userProfile = ""
        self.configFile = ""
        self.mpprcFile = ""
        self.clusterInfo = None
        self.logger = None
        
    ####################################################################################
    # This is the main install flow.  
    ####################################################################################
    def install(self):
        '''
        check install
        '''
        self.__checkParameters()
        self.__decompressBin()
        self.clusterInfo = dbClusterInfo()
        self.__checkUserProfile()
        self.__cleanUserEnv()
        self.__setUserEnv()    
        if(self.configFile != ""):
            self.clusterInfo.initFromXml(self.configFile)
        else:
            self.clusterInfo.initFromStaticConfig(self.user)
        self.__setManualStart()
        self.__createStaticConfig()
        self.__bakInstallPackage()
        self.__fixInstallPathPermission()
        self.__securitySet() 
        self.logger.closeLog()
    def __setOSKernelParameter(self):
        """
        Set os kernel parameter
        """
        self.logger.log("Set os kernel parameter...")
        kernelParameterFile = "/etc/sysctl.conf"
        kernelParameterList = DefaultValue.getOSKernelParameterList()

        #clean old kernel parameter
        for key in kernelParameterList:
            cmd = "sed -i '/^\\s*%s *=.*$/d' %s" % (key, kernelParameterFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Clean old kernel parameter failed!Output:%s" % output)

        #set new kernel parameter
        for key in kernelParameterList:
            cmd = "echo %s = %s  >> %s 2>/dev/null" % (key, kernelParameterList[key], kernelParameterFile)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                self.logger.logExit("Set os kernel parameter failed!Error:%s" % str(e))

        #enforce the kernel parameter
        cmd = "sysctl -p"
        (status, output) = commands.getstatusoutput(cmd)
        for key in kernelParameterList:
            if key not in output:
                self.logger.logExit("Enforce os kernel parameter failed!Error:%s" % output)

        #set fd num
        cmd = """sed -i '/^.* soft *nofile .*$/d' /etc/security/limits.conf; 
               sed -i '/^.* hard *nofile .*$/d' /etc/security/limits.conf; 
               echo "*       soft    nofile  1000000" >> /etc/security/limits.conf;
               echo "*       hard    nofile  1000000" >> /etc/security/limits.conf"""
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Set file handle number failed!Output:%s" % output)
            
    def __setManualStart(self):
        """
        Set manual start
        """
        self.logger.log("Set manual start...")
        cmd = "touch %s/bin/cluster_manual_start" % self.installPath
        self.logger.debug("Set manual cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            self.logger.logExit("Set manual start failed!Error:%s" % str(output))
            
    def __createStaticConfig(self):
        """
        Save cluster info to static config
        """
        staticConfigPath = "%s/bin/cluster_static_config" % self.installPath
        hostName = socket.gethostname()
        dbNode = self.clusterInfo.getDbNodeByName(hostName)
        if(dbNode == None):
            self.logger.logExit("No node named %s" % hostName)
        nodeId = dbNode.id
        self.clusterInfo.saveToStaticConfig(staticConfigPath, nodeId)
        cmd = "chown %s:%s %s;chmod 640 %s" % (self.user, self.group, staticConfigPath, staticConfigPath)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Create cluster static config failed!Error:%s" % output)
            
    def __bakInstallPackage(self):
        """
        backup install package for replace
        """
        dirName = os.path.dirname(os.path.abspath(__file__))
        packageFile = "%s/Gauss-MPPDB-Package-bak.tar.gz" % os.path.join(dirName, "./../../")
        
        #Check if MPPDB package exist
        if (not os.path.exists(packageFile)):
            self.logger.logExit("MPPDB package does not exist, can not do backup!") 

        #Save MPPDB package to bin path
        cmd = "mv %s %s/bin" % (packageFile, self.installPath)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            self.logger.logExit("Save Gauss MPPDB package failed.Error:%s" % output)

    def __fixInstallPathPermission(self):
        """
        fix the whole install path's permission
        """
        installPathFileTypeDict = {}
        try:
            installPathFileTypeDict = PlatformCommand.getFilesType(self.installPath)
        except Exception, e:
            self.logger.logExit("get file type of install path failed: %s" % str(e))
        for key in installPathFileTypeDict:
            if(not os.path.exists(key)):
                self.logger.debug("[%s] does not exist, skip it." % key)
                continue
            if(os.path.islink(key)):
                self.logger.debug("[%s] is a link file, skip it." % key)
                continue
            if(installPathFileTypeDict[key].find("executable") >= 0 or
                      installPathFileTypeDict[key].find("directory") >= 0):
                strCmd = "chmod 750 %s" % key
            else:
                strCmd = "chmod 640 %s" % key
            (status, output) = commands.getstatusoutput(strCmd)
            if(status != 0):
                self.logger.logExit("change mode of %s failed. cmd: %s output: %s" % (key, strCmd, output))


    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            #option '-M' specify the environment parameter GAUSSLOG
            #option '-P' specify the environment parameter PGHOST
            opts, args = getopt.getopt(sys.argv[1:], "U:X:R:M:P:l:c:")
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)
        
        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))

        logFile = ""
        for key, value in opts:
            if(key == "-U"):
                strTemp = value
                strList = strTemp.split(":")
                if(len(strList) != 2):
                    GaussLog.exitWithError("Parameter input error: -U " + value)
                if(strList[0] == "" or strList[1] == ""):
                    GaussLog.exitWithError("Parameter input error: -U " + value)
                self.user = strList[0]
                self.group = strList[1]
            elif(key == "-X"):
                self.configFile = value
            elif(key == "-R"):
                self.installPath = value
            elif (key == "-l"):
                logFile = value
            elif (key == "-c"):
                self.clusterName = value
            elif (key == "-M"):
                self.logPath = value
            elif(key == "-P"):
                self.tmpPath = value
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")

        if (self.configFile != "" and not os.path.exists(self.configFile)):
            GaussLog.exitWithError("Config file does not exist: %s" % self.configFile)

        if (self.logPath != "" and not os.path.exists(self.logPath) and not os.path.isabs(self.logPath)):
            GaussLog.exitWithError("GaussLog Path input error: " + self.logPath + ".")

        #check mpprc file path
        self.mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(self.mpprcFile == None):
            self.mpprcFile = ""
        if (self.mpprcFile != ""):
            if (not os.path.exists(self.mpprcFile)):
                GaussLog.exitWithError("mpprc file does not exist: %s" % self.mpprcFile)    
            if (not os.path.isabs(self.mpprcFile)):
                GaussLog.exitWithError("mpprc file need absolute path:%s" % self.mpprcFile)

        if(self.user == ""):
            GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        if(self.installPath == ""):
            GaussLog.exitWithError("Parameter input error, need '-R' parameter.")
        if(self.clusterName == ""):
            GaussLog.exitWithError("Parameter input error, need '-c' parameter.")  
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, self.user, "", self.configFile)

        self.logger = GaussLog(logFile, "InstallApp")
        (status, output) = commands.getstatusoutput("id -u " + self.user)
        if(status != 0):
            self.logger.logExit(self.user + " : no such user.")
        self.installPath = os.path.normpath(self.installPath)
        self.installPath = os.path.abspath(self.installPath)
        self.logger.log("Using " + self.user + ":" + self.group + " to install database")
        self.logger.log("Using install program path : " + self.installPath)
        
####################################################################################
# Decompress binary file.  
####################################################################################

    def __decompressBin(self):
        '''
         Install database binary file. 
        '''
        self.logger.log("Begin decompress bin file...")
        binFile = DefaultValue.getBinFilePath()

        # let bin executable
        strCmd = "chmod 750 " + binFile
        self.logger.log("Let bin file executable cmd: " + strCmd)
        status, output = commands.getstatusoutput(strCmd)
        if(status != 0):
            self.logger.logExit("Let bin file executable return: " + str(status) + os.linesep + output)
        # decompress bin file.
        strCmd = "\"" + binFile + "\" -yo\"" + self.installPath + "\"" 
        self.logger.log("Decompress cmd: " + strCmd)
        status, output = commands.getstatusoutput(strCmd)
        if(status != 0):
            self.logger.logExit("Decompress bin return: " + str(status) + os.linesep + output)
        # change owner to user:group
        strCmd = "chown " + self.user + ":" + self.group + " -R \"" + self.installPath + "\""
        self.logger.log("Change owner cmd: " + strCmd)
        status, output = commands.getstatusoutput(strCmd)
        if(status != 0):
            self.logger.logExit("chown to " + self.user + ":" + self.group + " return: " + str(status) + os.linesep + output)
        # check libcgroup config file path exists
        try:
            clusterToolPath = DefaultValue.getClusterToolPath()
        except Exception, e:
            self.logger.logExit("get cluster tool path failed: %s" % str(e))
            
        cgroupCfgPath = "%s/%s/etc" % (clusterToolPath, self.user)
        if(os.path.exists(cgroupCfgPath)):
            self.logger.log("libcgroup config file path exists")
        else:
            self.logger.logExit("libcgroup config file path not exists %s " % cgroupCfgPath)
        # check libcgroup etc file exists
        cgroupCfgFile = "%s/%s/etc/gscgroup_%s.cfg" % (clusterToolPath, self.user, self.user)
        if(os.path.exists(cgroupCfgFile)):
            # copy libcgroup etc file to install path
            strCmd = "cp %s %s/etc" % (cgroupCfgFile, self.installPath)
            self.logger.log("copy libcgroup etc file cmd: " + strCmd)
            status, output = commands.getstatusoutput(strCmd)
            if(status != 0):
                self.logger.logExit("Copy libcgroup etc file return: " + str(status) + os.linesep + output)
        else:
            self.logger.log("libcgroup config file is not exists")            
        self.logger.log("End decompress bin file")
        
    def __checkUserProfile(self):
        """
        Check user profile
        """
        if(self.mpprcFile != ""):
            self.userProfile = self.mpprcFile
            mpprcFilePath = os.path.dirname(self.mpprcFile)
            cmd = "mkdir -p %s -m 750" % mpprcFilePath
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Create user profile path failed!Error:%s" % output)
        else:
            cmd = "echo ~ 2>/dev/null"
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                self.logger.logExit("Can not get user home.")
            self.userProfile = os.path.join(output, ".bashrc")
        
        if (not os.path.exists(self.userProfile)):
            self.logger.log("User profile does not exist! Create: %s" % self.userProfile)
            cmd = "touch %s" % self.userProfile
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Create user profile failed!Error:%s" % output)
        
    def __cleanUserEnv(self):
        self.logger.log("Clean old user environment variables...")

        # clean version
        cmd = "sed -i '/^\\s*export\\s*GAUSS_VERSION=.*$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean GAUSS_VERSION in user environment failed!Output:%s" % output)
        self.logger.debug("Clean GAUSS_VERSION in user environment variables")

        #clean lib
        cmd = "sed -i '/^\\s*export\\s*LD_LIBRARY_PATH=\\$GAUSSHOME\\/lib:\\$LD_LIBRARY_PATH$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean lib in user environment failed!Output:%s" % output)
        self.logger.debug("Clean lib in user environment variables")
        
        #clean bin
        cmd = "sed -i '/^\\s*export\\s*PATH=\\$GAUSSHOME\\/bin:\\$PATH$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean bin in user environment variables failed!Output:%s" % output)      
        self.logger.debug("Clean bin in user environment variables")
        
        #clean GAUSSHOME
        cmd = "sed -i '/^\\s*export\\s*GAUSSHOME=.*$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean GAUSSHOME in user environment variables failed!Output:%s" % output)
        self.logger.debug("Clean GAUSSHOME in user environment variables")

        #clean PGHOST
        cmd = "sed -i '/^\\s*export\\s*PGHOST=.*$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean PGHOST in user environment variables failed!Output:%s" % output)
        self.logger.debug("Clean PGHOST in user environment variables")       
        
        #clean GS_CLUSTER_NAME
        cmd = "sed -i '/^\\s*export\\s*GS_CLUSTER_NAME=.*$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean GS_CLUSTER_NAME in user environment variables failed!Output:%s" % output)
        self.logger.debug("Clean GS_CLUSTER_NAME in user environment variables")
        
        #clean GAUSSLOG
        cmd = "sed -i '/^\\s*export\\s*GAUSSLOG=.*$/d' %s" % self.userProfile
        status, output = commands.getstatusoutput(cmd)
        if (status != 0):
            self.logger.logExit("Clean GAUSSLOG in user environment variables failed!Output:%s" % output)
        self.logger.debug("Clean GAUSSLOG in user environment variables")
        
        self.logger.log("End clean user environment variables...")
    
    def __setUserEnv(self):
        """
        Set Gaussdb database DBA user environment.
        """
        self.logger.log("Begin set user environment...")
        fp = None
        try:
            fp = open(self.userProfile, "a")
            fp.write("export GAUSSHOME=%s" % self.installPath)
            fp.write(os.linesep)
            fp.write("export PATH=$GAUSSHOME/bin:$PATH")
            fp.write(os.linesep)
            fp.write("export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH")
            fp.write(os.linesep)
            fp.write("export GAUSS_VERSION=%s" % VersionInfo.getPackageVersion())
            fp.write(os.linesep)
            if(self.tmpPath != ""):
                fp.write("export PGHOST=%s" % self.tmpPath)
                fp.write(os.linesep)
            #parameter -P is empty, we need read tmp path from xml file
            elif(self.tmpPath == "" and self.configFile != ""):
                self.tmpPath = self.clusterInfo.readClusterTmpMppdbPath(self.user, self.configFile)
                fp.write("export PGHOST=%s" % self.tmpPath)
                fp.write(os.linesep)
            else:
                self.logger.logExit("Set user environment parameter GAUSSLOG failed!")
            fp.write("export GS_CLUSTER_NAME=%s" % self.clusterName)
            fp.write(os.linesep)
            #parameter -M is not empty, the log path is with user
            if(self.logPath != ""):
                fp.write("export GAUSSLOG=%s" % self.logPath)
                fp.write(os.linesep)
            #parameter -M is empty, the log path is not with user
            elif(self.logPath == "" and self.configFile != ""):
                self.logPath = self.clusterInfo.readClusterLogPath(self.configFile)
                fp.write("export GAUSSLOG=%s/%s" % (self.logPath, self.user))
                fp.write(os.linesep)
            else:
                self.logger.logExit("Set user environment parameter GAUSSLOG failed!")

            fp.flush()
            fp.close()
            
        except Exception, e:
            if(fp):fp.close()
            self.logger.logExit("Set user environment failed!Error:%s" % str(e))
            
    def __securitySet(self):
        """
        chmod of ReplaceConfig.py and InitInstance.py.
        """
        self.logger.log("Begin set mod of file...")
        currentPath = os.path.dirname(os.path.abspath(__file__))
        currentReplaceConfigPath = "%s/ReplaceConfig.py" % currentPath
        self.logger.log("%s" % currentReplaceConfigPath)
        currentInitInstancePath = "%s/InitInstance.py" % currentPath
        self.logger.log("%s" % currentInitInstancePath)
        if(os.path.exists(currentReplaceConfigPath)):
            os.chmod(currentReplaceConfigPath, 0600)
        if(os.path.exists(currentInitInstancePath)):
            os.chmod(currentInitInstancePath, 0600)
        installReplaceConfigPath = "%s/bin/script/local/ReplaceConfig.py" % self.installPath 
        self.logger.log("%s" % installReplaceConfigPath)
        installInitInstancePath = "%s/bin/script/local/InitInstance.py" % self.installPath
        self.logger.log("%s" % installInitInstancePath)
        if(os.path.exists(installReplaceConfigPath)):
            os.chmod(installReplaceConfigPath, 0600)
        if(os.path.exists(installInitInstancePath)):
            os.chmod(installInitInstancePath, 0600)
    
if __name__ == '__main__':
    """
    main function
    """ 
    installer = Install()
    installer.install()
    
    sys.exit(0)
