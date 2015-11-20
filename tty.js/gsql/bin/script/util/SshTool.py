'''
Created on 2014-2-18

@author: 
'''

import commands
import os
import re

from script.util.Common import DefaultValue

class SshTool():
    """
    Class for controling multi-hosts
    """
    def __init__(self, hostNames, logFile = None):
        self.hostNames = hostNames
        self.__logFile = logFile
        self.__pid = os.getpid()

        #can tmp path always access?
        self.__hostsFile = "/tmp/gauss_hosts_file_%d" % self.__pid
        self.__resultFile = "/tmp/gauss_result_%d.log" % self.__pid
        self.__outputPath = "/tmp/gauss_output_files_%d" % self.__pid
        self.__errorPath = "/tmp/gauss_error_files_%d" % self.__pid
        self.__resultStatus = {}
        if (logFile is None):
            self.__logFile = "/dev/null"
        
        self.__writeHostFiles()
        
    def __del__(self):
        """
        """
        if (os.path.exists(self.__hostsFile)):
            os.remove(self.__hostsFile)
            
        if (os.path.exists(self.__resultFile)):
            os.remove(self.__resultFile)

        if (os.path.exists(self.__outputPath)):
            cmd = "rm -rf %s" % self.__outputPath
            (status, output) = commands.getstatusoutput(cmd)

        if (os.path.exists(self.__errorPath)):
            cmd = "rm -rf %s" % self.__errorPath
            (status, output) = commands.getstatusoutput(cmd)
    
    def exchangeHostnameSshKeys(self, user, pwd, mpprcFile = ""):
        """
        Exchange ssh public keys for specified user, using hostname
        """
        if(mpprcFile != ""):
            exkeyCmd = "su - %s -c 'source %s&&mvxssh-exkeys -f %s -p %s' 2>>%s" % (user, mpprcFile, self.__hostsFile, pwd, self.__logFile)
        else:  
            exkeyCmd = "su - %s -c 'source /etc/profile&&mvxssh-exkeys -f %s -p %s' 2>>%s" % (user, self.__hostsFile, pwd, self.__logFile)
        (status, output) = commands.getstatusoutput(exkeyCmd)
        if (status != 0):
            raise Exception("Using hostname exchange ssh keys for user[%s] failed!%s\nYou can comment Cipher 3des, Ciphers aes128-cbc and MACs in /etc/ssh/ssh_config and try again" % (user, output.replace(pwd, "******")))
        
    def exchangeIpSshKeys(self, user, pwd, ips, mpprcFile = ""):
        """
        Exchange ssh public keys for specified user, using ip address
        """
        if(mpprcFile != ""):
            exkeyCmd = "su - %s -c 'source %s&&mvxssh-exkeys " % (user, mpprcFile)
        else:
            exkeyCmd = "su - %s -c 'source /etc/profile&&mvxssh-exkeys " % user
        for ip in ips:
            exkeyCmd += " -h %s " % ip.strip()
        exkeyCmd += "-p %s' 2>>%s" % (pwd, self.__logFile)
        (status, output) = commands.getstatusoutput(exkeyCmd)
        if(status != 0):
            raise Exception("Using ip address exchange ssh keys for user[%s] failed!%s\nYou can comment Cipher 3des, Ciphers aes128-cbc and MACs in /etc/ssh/ssh_config and try again" % (user, output.replace(pwd, "******"))) 

    def createTrust(self, user, pwd, ips = []):
        """
        create trust for specified user with both ip and hostname, when using N9000 tool create trust failed 
        do not support using a normal user to create trust for another user. 
        """
        tmp_hosts = "/tmp/tmp_hosts_%d" % self.__pid
        try:
            #1.prepare hosts file
            cmd = "cat %s > %s 2>/dev/null" % (self.__hostsFile, tmp_hosts)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("cmd:%s\nprepare hosts file for create trust failed:%s" % (cmd, output))
            for ip in ips:
                cmd = "echo %s >> %s 2>/dev/null" % (ip, tmp_hosts)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception("cmd:%s\nprepare hosts file for create trust failed:%s" % (cmd, output))

            #2.call createtrust script
            dirName = os.path.dirname(os.path.abspath(__file__))
            create_trust_file = os.path.join(dirName, "./../local/create_trust.sh")
            if(os.getgid() == 0):
                cmd = "su - %s -c \"sh %s %s %s %s %s \" 2>&1" % (user, create_trust_file, user, pwd, tmp_hosts, self.__logFile)
            else:
                cmd = "sh %s %s %s %s %s 2>&1" % (create_trust_file, user, pwd, tmp_hosts, self.__logFile)

            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                #we can not print cmd here, because it include user's passwd
                raise Exception("create trust failed, please check the log to find the reason.")

            #3.delete hosts file
            cmd = "rm -rf %s" % tmp_hosts
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("cmd:%s\nclean hosts file failed:%s" % (cmd, output))
        except Exception, e:
            commands.getstatusoutput("rm -rf %s" % tmp_hosts)
            raise Exception("create trust for %s failed:%s" % (user, str(e)))

    def executeCommand(self, cmd, descript, cmdReturn = 0, hostList = [], env_file = ""):
        """
        Execute command on all hosts
        """
        sshCmd = ""
        outputCollect = ""
        prefix = ""
        fp = None
        try:
            if(env_file != ""):
                mpprcFile = env_file
            else:
                mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
            if(mpprcFile != "" and mpprcFile != None):
                userProfile = mpprcFile
                osProfile = mpprcFile
            else:
                userProfile = "~/.bashrc"
                osProfile = "/etc/profile"
                
            #clean result file
            if (os.path.exists(self.__resultFile)):
                os.remove(self.__resultFile)

            if (len(hostList) == 0):
                if(os.getgid() == 0 and (mpprcFile == "" or mpprcFile == None)):
                    sshCmd = "source %s&&pssh -t 0 -h %s -P -p 300 -o %s -e %s \"%s\" 2>&1 | tee %s" % (osProfile, self.__hostsFile, self.__outputPath, 
                              self.__errorPath, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s&&pssh -t 0 -h %s -P -p 300 -o %s -e %s \"source %s;%s\" 2>&1 | tee %s" % (osProfile, self.__hostsFile, self.__outputPath, 
                              self.__errorPath, userProfile, cmd, self.__resultFile)
                hostList = self.hostNames
            else:
                if(os.getgid() == 0 and (mpprcFile == "" or mpprcFile == None)):
                    sshCmd = "source %s&&pssh -t 0 -H %s -P -p 300 -o %s -e %s \"%s\" 2>&1 | tee %s" % (osProfile, " -H ".join(hostList), self.__outputPath, 
                              self.__errorPath, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s&&pssh -t 0 -H %s -P -p 300 -o %s -e %s \"source %s;%s\" 2>&1 | tee %s" % (osProfile, " -H ".join(hostList), self.__outputPath, 
                              self.__errorPath, userProfile, cmd, self.__resultFile)

            (status, output) = commands.getstatusoutput(sshCmd)
            if (status != 0):
                raise Exception("Execute pssh command[%s] failed!\n%s" % (cmd, output))
            #ip and host name should match here
            resultMap = self.__readCmdResult(self.__resultFile, len(hostList), cmd)
            for host in hostList:
                sshOutPutFile = "%s/%s" % (self.__outputPath, host)
                sshErrorPutFile = "%s/%s" % (self.__errorPath, host)
                if(resultMap[host] == 0):
                    prefix = "SUCCESS"
                else:
                    prefix = "FAILURE"
                    
                outputCollect += "[%s] %s:\n" % (prefix, host)
                if(os.path.isfile(sshOutPutFile)):
                    context = ""
                    fp = open(sshOutPutFile, "r")
                    context = fp.read()
                    fp.close()
                    outputCollect += context
                if(os.path.isfile(sshErrorPutFile)):
                    context = ""
                    fp = open(sshErrorPutFile, "r")
                    context = fp.read()
                    fp.close()
                    outputCollect += context
        except Exception, e:
            if(fp): fp.close()
            raise Exception("get ssh status and output failed: %s" % str(e))

        for host in hostList:
            if (resultMap.get(host) != cmdReturn):
                raise Exception("%s failed! Result:%s.\nCommand: %s.\nOutput:\n%s" % (descript, resultMap, cmd, outputCollect))
    
    def getSshStatusOutput(self, cmd, hostList = [], env_file = ""):
        """
        Get command status and output
        """
        sshCmd = ""
        outputCollect = ""
        prefix = ""
        fp = None
        try:
            if(env_file != ""):
                mpprcFile = env_file
            else:
                mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
            if(mpprcFile != "" and mpprcFile != None):
                userProfile = mpprcFile
                osProfile = mpprcFile
            else:
                userProfile = "~/.bashrc"
                osProfile = "/etc/profile"
            #clean result file
            if (os.path.exists(self.__resultFile)):
                os.remove(self.__resultFile) 

            if (len(hostList) == 0):
                if(os.getgid() == 0 and (mpprcFile == "" or mpprcFile == None)):
                    sshCmd = "source %s&&pssh -t 0 -h %s -P -p 300 -o %s -e %s \"%s\" 2>&1 | tee %s" % (osProfile, self.__hostsFile, self.__outputPath, 
                                self.__errorPath, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s&&pssh -t 0 -h %s -P -p 300 -o %s -e %s \"source %s;%s\" 2>&1 | tee %s" % (osProfile, self.__hostsFile, self.__outputPath, 
                                self.__errorPath, userProfile, cmd, self.__resultFile)
                hostList = self.hostNames
            else:
                if(os.getgid() == 0 and (mpprcFile == "" or mpprcFile == None)):
                    sshCmd = "source %s&&pssh -t 0 -H %s -P -p 300 -o %s -e %s \"%s\" 2>&1 | tee %s" % (osProfile, " -H ".join(hostList), self.__outputPath, 
                                self.__errorPath, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s&&pssh -t 0 -H %s -P -p 300 -o %s -e %s \"source %s;%s\" 2>&1 | tee %s" % (osProfile, " -H ".join(hostList), self.__outputPath, 
                                self.__errorPath, userProfile, cmd, self.__resultFile)

            (status, output) = commands.getstatusoutput(sshCmd)
            if(status != 0):
                raise Exception("Execute pssh command[%s] failed!\n%s" % (cmd, output)) 
            resultMap = self.__readCmdResult(self.__resultFile, len(hostList), cmd)
            for host in hostList:
                sshOutPutFile = "%s/%s" % (self.__outputPath, host)
                sshErrorPutFile = "%s/%s" % (self.__errorPath, host)
                if(resultMap[host] == 0):
                    prefix = "SUCCESS"
                else:
                    prefix = "FAILURE"
                    
                outputCollect += "[%s] %s:\n" % (prefix, host)
                if(os.path.isfile(sshOutPutFile)):
                    context = ""
                    fp = open(sshOutPutFile, "r")
                    context = fp.read()
                    fp.close()
                    outputCollect += context
                    
                if(os.path.isfile(sshErrorPutFile)):
                    context = ""
                    fp = open(sshErrorPutFile, "r")
                    context = fp.read()
                    fp.close()
                    outputCollect += context
                    
        except Exception, e:
            if(fp): fp.close()
            raise Exception("get ssh status and output failed: %s" % str(e))
        
        return (resultMap, outputCollect) 

    def parseSshOutput(self, hostList):
        """
        function:
          parse ssh output on every host
        input:
          hostList: the hostname list of all hosts
        output:
          a dict, like this "hostname : info of this host"
        hiden info:
          the output info of all hosts
        ppp:
          for a host in hostlist
            if outputfile exists
              open file with the same name
              read context into a str
              close file
              save info of this host
            else
              raise exception
          return host info list
        """
        resultMap = {}
        fp = None
        try:
            for host in hostList:
                context = ""
                sshOutPutFile = "%s/%s" % (self.__outputPath, host)
                sshErrorPutFile = "%s/%s" % (self.__errorPath, host)
                
                if(os.path.isfile(sshOutPutFile)):
                    fp = open(sshOutPutFile, "r")
                    context = fp.read()
                    fp.close()
                    resultMap[host] = context
                if(os.path.isfile(sshErrorPutFile)): 
                    fp = open(sshErrorPutFile, "r")
                    context += fp.read()
                    fp.close()
                    resultMap[host] = context
                else:
                    raise Exception("result file does not exists:%s" % sshOutPutFile)
        except Exception, e:
            if(fp): fp.close()
            raise Exception("parse ssh output failed: %s" % str(e))

        return resultMap
        
    def scpFiles(self, srcFile, targetDir, hostList = [], env_file = ""):
        """
        """
        scpCmd = "source /etc/profile"

        if(env_file != ""):
            mpprcFile = env_file
        else:
            mpprcFile = os.getenv(DefaultValue.MPPRC_FILE_ENV)
        if(mpprcFile != "" and mpprcFile != None):
            scpCmd += "&&source %s" % mpprcFile
        if (len(hostList) == 0):
            scpCmd += "&&pscp -r -v -t 0 -p 300 -h %s %s %s" % (self.__hostsFile, srcFile, targetDir)
        else:
            scpCmd += "&&pscp -r -v -t 0 -p 300 -H %s %s %s" % (" -H ".join(hostList), srcFile, targetDir)
        (status, output) = commands.getstatusoutput(scpCmd)
        if (status != 0):
            raise Exception("Scp file[%s] to directory[%s] failed!\n%s" % (srcFile, targetDir, output))

    def __writeHostFiles(self):
        """
        Write all hostname to a file
        """
        fp = None
        try:
            fp = open(self.__hostsFile, "w")
            for host in self.hostNames:
                fp.write("%s\n" % host)
            fp.flush()
            fp.close()
        except Exception, e:
            if (fp):fp.close()
            raise Exception("Write host file failed!Error:%s" % str(e))
        
        #change the mode
        #if it created by root user,and permission is 640, then
        #install user will have no permission to read it, so we should set
        #its permission 644.
        cmd = "chmod 644 %s" % self.__hostsFile
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("Change file[%s] mode failed!Error:%s" % (self.__hostsFile, output))
    
    def __readCmdResult(self, resultFile, hostNum, cmd):
        """
        """
        resultMap = {}
        fp = None
        try:
            fp = open(resultFile, "r")
            lines = fp.readlines()
            fp.close()
            context = "".join(lines)
            for line in lines:
                resultPair = line.strip().split(" ")
                if (len(resultPair) >= 4 and resultPair[2] == "[FAILURE]"):
                    resultMap[resultPair[3]] = 1
                if (len(resultPair) >= 4 and resultPair[2] == "[SUCCESS]"):
                    resultMap[resultPair[3]] = 0
                
            if(len(resultMap) != hostNum):
                raise Exception("the valid return item number(%d) is not match with host number(%d).The return result:\n%s" % (len(resultMap), hostNum, context))
        except Exception, e:
            if (fp):fp.close()
            raise Exception("cmd: %s\nGet ssh command result failed!!Error:%s" % (cmd, str(e)))
        
        return resultMap
