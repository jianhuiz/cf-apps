'''
Created on 2014-2-7

@author:
'''
import getopt
import os
import thread
import sys
import shutil
import socket
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.DbClusterInfo import *
from script.util.Common import DefaultValue, PlatformCommand

class CleanInstance:
    '''
    classdocs
    '''
    TYPE_DATADIR = "data-dir"
    TYPE_LOCKFILE = "lock-file"
    def __init__(self):
        '''
        Constructor
        '''
        self.clusterInfo = None
        self.dbNodeInfo = None
        self.logger = None
        
        self.cleanType = []
        self.Instancedirs = []
        self.tblspcdirs = []
        self.user = ""
        self.group = ""
        self.xmlFile = ""
        self.failedDir = ""
        self.nodedirCount = 0
        self.parallel = False
        self.lock = thread.allocate_lock()
    def run(self):
        """
        Do clean instance
        """
        self.__checkParameters()
        self.__readConfigInfo()
        self.__cleanInstance()
        self.logger.closeLog()

    ####################################################################################
    # Help context. 
    ####################################################################################
    def usage(self):
        print("CleanInstance.py is a utility to clean Gauss MPP Database instance.")
        print(" ")
        print("Usage:")
        print("  python CleanInstance.py --help")
        print("  python CleanInstance.py -U user [-t cleanType...] [-D datadir...] [-l logfile] [-X xmlfile]")
        print(" ")
        print("Common options:")
        print("  --help    show this help, then exit")
        print("  -U        the user of Gauss MPP Database")
        print("  -t        the content to be cleaned, can be data-dir, lock-file.")
        print("  -D        the directory of instance to be clean.")
        print("  -l        the log file path")
        print("  -X        the path of xml configuration file")
        print(" ")
    
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:D:l:t:X:", ["help"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)
            
        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))
    
        logFile = ""
        for key, value in opts:
            if(key == "-U"):
                self.user = value
            elif (key == "-D"):
                self.Instancedirs.append(os.path.normpath(value))
            elif (key == "-t"):
                self.cleanType.append(value)
            elif (key == "-l"):
                logFile = value
            elif (key == "-X"):
                self.xmlFile = value
            elif(key == "--help"):
                self.usage()
                sys.exit(0)
            else:
                GaussLog.exitWithError("Parameter input error: " + value + ".")
        
        # check if user exist and is the right user
        if(self.user == ""):
            GaussLog.exitWithError("Parameter input error: need '-U' parameter.")
        try:
            PlatformCommand.checkUser(self.user, False)
        except Exception as e:
            GaussLog.exitWithError(str(e))
  
        if (os.getgid()==0 and self.xmlFile is None):
            GaussLog.exitWithError("Parameter input error: need '-X' parameter.")

        if (len(self.cleanType) == 0):
            self.cleanType = [CleanInstance.TYPE_DATADIR, CleanInstance.TYPE_LOCKFILE]
            
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, self.user, "", "")
            
        self.logger = GaussLog(logFile, "CleanInstance")
        self.logger.debug("my logfile is %s." % logFile)
        
    def __readConfigInfo(self):
        """
        Read config from static config file
        """
        try:
            self.clusterInfo = dbClusterInfo()
            if (os.getgid()==0):
                self.clusterInfo.initFromXml(self.xmlFile)
            else:
                self.clusterInfo.initFromStaticConfig(self.user)
            (self.user, self.group) = PlatformCommand.getPathOwner(self.clusterInfo.appPath)
            hostName = socket.gethostname()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.debug("Instance info on local node:\n%s" % str(self.dbNodeInfo))
    
    ####################################################################################
    # This is the main clean instance flow.  
    ####################################################################################
    def __cleanInstance(self):
        '''
        Clean node instances. 
        '''
        self.logger.log("Begin clean instance...")
        if (len(self.Instancedirs) == 0):
            self.parallel = True
            self.Instancedirs = self.__getInstanceDirs()
        self.__getInstanceTblspcDirs()
        socketFiles = self.__getLockFiles()
        dataNodeDirs = self.__getDataNodeDirs()
        dirsExcDataNode = self.__getInstanceDirsExcDN()
        
        self.logger.debug("All instances directories: %s." % self.Instancedirs)
        self.logger.debug("All instances tablespace directories: %s." % self.tblspcdirs)
        self.logger.debug("All instances socket files: %s." % socketFiles)
        try:
            if (CleanInstance.TYPE_DATADIR in self.cleanType):
                for tblspcDir in self.tblspcdirs:
                    if(os.getgid() == 0):
                        cmd = "rm -rf %s" % tblspcDir
                    else:
                        cmd = "rm -rf %s/*" % tblspcDir
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        raise Exception("delete %s failed:%s" % (tblspcDir, output))
                if(self.parallel == True):       
                    for instDir in dirsExcDataNode:
                        if(os.getgid() == 0):
                            cmd = "rm -rf %s" % instDir
                        else:
                            cmd = "rm -rf %s/*" % instDir
                        (status, output) = commands.getstatusoutput(cmd)
                        if(status != 0):
                            raise Exception("delete %s failed:%s" % (instDir, output))
                    self.logger.debug("begin delete dataNodeDirs...")        
                    for dnDir in dataNodeDirs:
                        self.logger.debug("%s" % dnDir)
                        if(os.getgid() == 0):
                            cmd = "rm -rf %s" % dnDir
                        else:
                            cmd = "rm -rf %s/*" % dnDir
                        try:
                            thread.start_new_thread(self.__cleanDir,(dnDir,cmd))
                        except Exception as e:
                            raise Exception("delete failed:%s" % output)
                    self.logger.debug("begin check clean dataNodeDirs...")     
                    if(os.getgid() == 0):
                        for instDir in dataNodeDirs:
                            while(os.path.exists(instDir)):
                                if(self.failedDir != ""):
                                    raise Exception("delete %s failed" % self.failedDir)
                                else:
                                    pass
                    else:
                        while(self.nodedirCount < len(dataNodeDirs)):
                            if(self.failedDir != ""):
                                raise Exception("Clean instance failed!Error:%s" % self.failedDir)
                            else:
                                pass
                    self.nodedirCount = 0
                    self.logger.debug("end check clean dataNodeDirs")
                else:
                    for instDir in self.Instancedirs:
                        cmd = "rm -rf %s/*" % instDir
                        (status, output) = commands.getstatusoutput(cmd)
                        if(status != 0):
                            raise Exception("delete %s failed:%s" % (instDir, output))
            if (CleanInstance.TYPE_LOCKFILE in self.cleanType):
                for sktFile in socketFiles:
                    if (os.path.exists(sktFile)):
                        os.remove(sktFile)
        except Exception, e:
            self.logger.logExit("Clean instance failed!Error:%s" % str(e))
        self.logger.log("Clean instance finished.")

    def __isNotEmpty(self,instDir):
        for root,dirs,files in os.walk(instDir):
            if(len(dirs)!=0):
                return True
            return False    
        
    def __cleanDir(self,instDir,cmd):
        status  = os.system(cmd)
        if(status != 0):
            self.failedDir = instDir
        else:
            self.lock.acquire()
            self.nodedirCount = self.nodedirCount + 1    
            self.lock.release()
            
    def __getInstanceDirs(self):
        dirs = []
        for dbInst in self.dbNodeInfo.gtms:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.cmservers:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.coordinators:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.datanodes:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.cmagents:
            dirs.append(dbInst.datadir)
        dirs.append(self.dbNodeInfo.cmDataDir)
        
        return dirs

    def __getDataNodeDirs(self):
        dirs = []
        for dbInst in self.dbNodeInfo.datanodes:
            if(dbInst.datadir in dirs):
                pass
            else:    
                dirs.append(dbInst.datadir)
        return dirs

    def __getInstanceDirsExcDN(self):
        dirs = []
        for dbInst in self.dbNodeInfo.gtms:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.cmservers:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.coordinators:
            dirs.append(dbInst.datadir)
        for dbInst in self.dbNodeInfo.cmagents:
            dirs.append(dbInst.datadir)
        dirs.append(self.dbNodeInfo.cmDataDir)
        
        return dirs
        
    def __getInstanceTblspcDirs(self):
        CnDnInfos = {}
        for dbInst in self.dbNodeInfo.coordinators:
            CnDnInfos[dbInst.datadir] = "cn_%d" % dbInst.instanceId
        for dbInst in self.dbNodeInfo.datanodes:
            if(dbInst.instanceType == DUMMY_STANDBY_INSTANCE):continue
            peerInsts = self.clusterInfo.getPeerInstance(dbInst)
            if (len(peerInsts) != 2 and len(peerInsts) != 1):
                g_logger.logExit("Get peer instance failed!")
            masterInst = None
            standbyInst = None
            dummyStandbyInst = None
            nodename = ""
            for i in range(len(peerInsts)):
                if(peerInsts[i].instanceType == MASTER_INSTANCE):
                    masterInst = peerInsts[i]
                elif(peerInsts[i].instanceType == STANDBY_INSTANCE):
                    standbyInst = peerInsts[i]
                elif(peerInsts[i].instanceType == DUMMY_STANDBY_INSTANCE):
                    dummyStandbyInst = peerInsts[i]
            if(dbInst.instanceType == MASTER_INSTANCE):
                masterInst = dbInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            elif(dbInst.instanceType == STANDBY_INSTANCE):
                standbyInst = dbInst
                nodename = "dn_%d_%d" % (masterInst.instanceId, standbyInst.instanceId)
            CnDnInfos[dbInst.datadir] = nodename
            
        self.logger.debug("cn dn info: %s" % CnDnInfos)
        for instanceDir in self.Instancedirs:
            if(CnDnInfos.has_key(instanceDir)):
                if not os.path.exists("%s/pg_tblspc" % instanceDir):
                    self.logger.debug("%s/pg_tblspc does not exists" % instanceDir)
                    continue
                fileList = os.listdir("%s/pg_tblspc" % instanceDir)
                if(len(fileList)):
                    for filename in fileList:
                        if(os.path.islink("%s/pg_tblspc/%s" % (instanceDir, filename))):
                            linkDir = os.readlink("%s/pg_tblspc/%s" % (instanceDir, filename))
                            if(os.path.isdir(linkDir)):
                                tblspcDir = "%s/%s_%s" % (linkDir, DefaultValue.TABLESPACE_VERSION_DIRECTORY, CnDnInfos[instanceDir])
                                self.logger.debug("table space dir is %s" % tblspcDir)
                                self.tblspcdirs.append(tblspcDir)
                            else:
                                self.logger.debug("%s is not link dir" % linkDir)
                        else:
                            self.logger.debug("%s is not a link file" % filename)
                else:
                    self.logger.debug("%s/pg_tblspc is empty" % instanceDir)
            else:
                self.logger.debug("%s does not find in dict" % instanceDir)
            
            

    def __getLockFiles(self):
        files = []
        if(self.xmlFile == ""):
            tmp_dir = DefaultValue.getTmpDirFromEnv()
        else:
            tmp_dir = DefaultValue.getTmpDir(self.user, self.xmlFile)
            
        instances = []
        instances += self.dbNodeInfo.gtms
        instances += self.dbNodeInfo.cmservers
        instances += self.dbNodeInfo.coordinators
        instances += self.dbNodeInfo.datanodes
        instances += self.dbNodeInfo.datanodes
        instances += self.dbNodeInfo.cmagents
        
        for dbInst in instances:
            if (dbInst.datadir not in self.Instancedirs):
                continue

            pgsql = ".s.PGSQL.%d" % dbInst.port
            pgsql_lock = ".s.PGSQL.%d.lock" % dbInst.port
            files.append(os.path.join(tmp_dir, pgsql))
            files.append(os.path.join(tmp_dir, pgsql_lock))

        for cooInst in self.dbNodeInfo.coordinators:
            if (cooInst.datadir not in self.Instancedirs):
                continue

            pgpool = ".s.PGPOOL.%d" % cooInst.haPort
            pgpool_lock = ".s.PGPOOL.%d.lock" % cooInst.haPort
            files.append(os.path.join(tmp_dir, pgpool))
            files.append(os.path.join(tmp_dir, pgpool_lock))

        return files

if __name__ == '__main__':
    """
    main function
    """
    cleaner = CleanInstance()
    cleaner.run()
    
    sys.exit(0)
