'''
Created on 2014-2-13

@author: 
'''

import os
import sys
import time
import commands
import re
from Common import DefaultValue,PlatformCommand

MAXLOGFILESIZE = 16*1024*1024 #16M

class GaussLog:
    
    def __init__(self, logFile, module = ""):
        '''
        Constructor
        '''
        self.logFile = ""
        self.moduleName = module
        self.fp = None
        self.size = 0
        self.suffix = ""
        self.prefix = ""
        self.dir = ""
        self.pid = os.getpid()
        
        logFileList = "" 
        fp = None
        try:
            dirName = os.path.dirname(logFile)
            
            if(not os.path.exists(dirName)):
                topDirPath = PlatformCommand.getTopPathNotExist(dirName)
                if (not os.path.isdir(dirName)):
                    os.makedirs(dirName, 0750)
                cmd = "echo %s > %s/topDirPath.dat 2>/dev/null" % (topDirPath, dirName)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("The top path to be created failed:%s" % output)
            
            self.dir = dirName
            originalFileName = os.path.basename(logFile)
            resList = originalFileName.split(".")
            if(len(resList) > 2):
                raise Exception("There is more than one . in log file name %s" % originalFileName)
                
            (self.prefix, self.suffix) = os.path.splitext(originalFileName)
            if(self.suffix != ".log"):
                raise Exception("The suffix of log file name should be .log")
            
            logFileList = "%s/logFileList_%s.dat" % (self.dir, self.pid)
            cmd = "ls %s | grep '^%s-' | grep '%s$' > %s" % (self.dir, self.prefix, self.suffix, logFileList)
            (status, output) = commands.getstatusoutput(cmd)
            if(status == 0):
                fp = open(logFileList, "r")
                filenameList = []
                while True:
                    filename = (fp.readline()).strip()
                    if not filename:break
                    existedResList = filename.split(".")
                    if(len(existedResList) > 2):
                        continue
                    (existedPrefix, existedSuffix) = os.path.splitext(filename)
                    if(existedSuffix != ".log"):
                        continue  
                    if(len(originalFileName) + 18 != len(filename)):
                        continue
                    timeStamp = filename[-21:-4]
                    
                    if(self.is_valid_date(timeStamp)):
                        pass
                    else:
                        continue

                    filenameList.append(filename)

                if(len(filenameList)):
                    fileName = max(filenameList)
                    self.logFile = self.dir + "/" + fileName.strip()
                    self.fp = open(self.logFile, "a")
                    PlatformCommand.cleanTmpFile(logFileList, fp)
                    return

            PlatformCommand.cleanTmpFile(logFileList, fp) 
            #create new log file
            currentTime = time.strftime("%Y-%m-%d_%H%M%S")
            self.logFile = self.dir + "/" + self.prefix + "-" + currentTime + self.suffix
            if(not os.path.exists(self.logFile)):
                os.mknod(self.logFile, 0640)
            self.fp = open(self.logFile, "a")
        except Exception, ex:
            PlatformCommand.cleanTmpFile(logFileList, fp)
            print("Error: Can not create or open log file: %s" % logFile)
            print(str(ex))
            sys.exit(1)

    def is_valid_date(self, str):
        try:
            time.strptime(str, "%Y-%m-%d_%H%M%S")
            return True
        except:
            return False
            
    def closeLog(self):
        if(self.fp):
            self.fp.flush()
            self.fp.close()
            self.fp = None
        
    def log(self, msg):
        print(msg)
        self.__writeLog("info", msg)
            
    def debug(self, msg):
        self.__writeLog("debug", msg)
    
    def logExit(self, msg):
        print(msg)
        self.__writeLog("error", msg)
        self.closeLog()
        sys.exit(1)
        
    def __writeLog(self, level, msg):
        """
        Write log to file
        """
        if (self.fp is None):
            return
        
        try:
            #check if need switch to an new log file
            self.size = os.path.getsize(self.logFile)
            if(self.size >= MAXLOGFILESIZE):
                self.closeLog()
                currentTime = time.strftime("%Y-%m-%d_%H%M%S")
                self.logFile = self.dir + "/" + self.prefix + "-" + currentTime + self.suffix
                if(not os.path.exists(self.logFile)):
                    os.mknod(self.logFile, 0640)
                self.fp = open(self.logFile, "a")
        except Exception, ex:
            print("Error: Can not create or open log file: %s" % self.logFile)
            print(str(ex))
            sys.exit(1)
            
        strTime = time.strftime("%Y-%m-%d %H:%M:%S")
        print >>self.fp, "[%s][%s][%s]:%s" % (strTime, self.moduleName, level, msg)
        self.fp.flush()
    
    @staticmethod
    def exitWithError(msg, status=1):
        sys.stderr.write("%s\n" % msg)
        sys.exit(status)
        
    @staticmethod
    def printMessage(msg):
        sys.stdout.write("%s\n" % msg)