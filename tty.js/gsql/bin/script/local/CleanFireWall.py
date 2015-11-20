'''
Created on 2015-3-10

@author: 
'''
import getopt
import os
import sys
import commands
import socket

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import DefaultValue, PlatformCommand
from script.util.DbClusterInfo import *

FIREWALL_CONFIG_FILE = '/etc/sysconfig/SuSEfirewall2'

class CleanFireWall:
    '''
    This class is for cleanning cluster firewall.
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.user = ""
        self.xmlfile = ""
        self.clusterInfo = None
        self.dbNodeInfo = None
        self.logger = None
        self.hostIps = []
        self.cooPortList = [] 
        self.backPortList = []
        self.newBackPortTcpList = []
        self.newBackPortUdpList = []
        self.newFrontPortTcpList = []
        self.newFrontPortUdpList = []
  

    ####################################################################################
    # Help context. 
    ####################################################################################
    def usage(self):
        print("CleanFireWall.py is a utility to clean Gauss MPP Database firewall.")
        print(" ")
        print("Usage:")
        print("  python CleanFireWall.py --help")
        print("  python CleanFireWall.py -U user -X xmlfile [-l logfile]")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  -X        the path of xml configuration file")
        print("  -l        the path of log file")
        print("  --help    show this help, then exit")
        print(" ")
    
    def __checkParameters(self):
        '''
        check input parameters
        '''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:X:l:", ["help"])
        except getopt.GetoptError, e:
            GaussLog.exitWithError("Parameter input error: " + e.msg)

        if(len(args) > 0):
            GaussLog.exitWithError("Parameter input error: " + str(args[0]))

        logFile = ""
        for key, value in opts:
            if(key == "-U"):
                self.user = value
            elif(key == "-X"):
                self.xmlfile = value
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

        if(self.xmlfile == ""):
            GaussLog.exitWithError("Parameter input error, need -X parameter.")
        
        if(logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, self.user, "")

        self.logger = GaussLog(logFile, "CleanFireWall")
        
    def __readConfigInfo(self):
        """
        Read config from xml config file
        """
        self.logger.debug("Begin read config file...")
        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromXml(self.xmlfile)
            hostName = socket.gethostname()
            self.dbNodeInfo = self.clusterInfo.getDbNodeByName(hostName)
            if (self.dbNodeInfo is None):
                self.logger.logExit("Get local instance info failed!There is no host named %s!" % hostName)
        except Exception, e:
            self.logger.logExit(str(e))

        self.logger.debug("Instance info on local node:\n%s" % str(self.dbNodeInfo))
        self.logger.debug("End read config file")

    def __getClusterIp(self):
        """
        get cluster ip information
        """
        self.logger.debug("Begin get cluster host ip...")
        try:
            oldHostIps = []
            for dbNode in self.clusterInfo.dbNodes:
                if dbNode.backIps is not None:
                    oldHostIps.append(dbNode.backIps)
                if dbNode.sshIps is not None:
                    oldHostIps.append(dbNode.sshIps)
            newHostIps = []
            for i in range(len(oldHostIps)):
                for oldHostIp in oldHostIps[i]:
                    if oldHostIp not in newHostIps:
                        newHostIps.append(oldHostIp)
            self.hostIps = newHostIps
        except Exception, e:
            self.logger.logExit(str(e))

        self.logger.debug("Cluster Ips %s " % self.hostIps)
        self.logger.debug("End get cluster host ip")
	
    def __getClusterPort(self):
        """
        get cluster port information
        """
        self.logger.debug("Begin get cluster host port...")
        try:
            for cooInst in self.dbNodeInfo.coordinators:
                if (cooInst.port > 0):
                    self.cooPortList.append(cooInst.port)
                if (cooInst.haPort > 0):
                    self.backPortList.append(cooInst.haPort)
            for cmsInst in self.dbNodeInfo.cmservers:
                if (cmsInst.port > 0):
                    self.backPortList.append(cmsInst.port)
                if (cmsInst.haPort > 0):
                    self.backPortList.append(cmsInst.haPort)
            for gtmInst in self.dbNodeInfo.gtms:
                if (gtmInst.port > 0):
                    self.backPortList.append(gtmInst.port)
                if (gtmInst.haPort > 0):
                    self.backPortList.append(gtmInst.haPort)
            for dnInst in self.dbNodeInfo.datanodes:
                if (dnInst.port > 0):
                    self.backPortList.append(dnInst.port)
                if (dnInst.haPort > 0):
                    self.backPortList.append(dnInst.haPort)
        except Exception, e:
            self.logger.logExit(str(e))

        self.logger.debug("Coodinator port %s, back port %s " % (self.cooPortList, self.backPortList))
        self.logger.debug("End get cluster host port")

    def __setIpPortList(self):
        """
        set ip-port list
        """
        self.logger.debug("Begin construct ip-port list...")
        try:
            #construct CN port list
            self.newFrontPortTcpList = self.cooPortList
            self.newFrontPortUdpList = self.cooPortList
            self.logger.debug("Coo tcp port list %s " % self.newFrontPortTcpList )
            self.logger.debug("Coo udp port list %s " % self.newFrontPortUdpList )
            
            #construct CMS/GTM/DN ip-port list      
            for ip in self.hostIps:
                for port in self.backPortList:
                    ipTcpPort = "%s,tcp,%s" % (ip, port)
                    ipUdpPort = "%s,udp,%s" % (ip, port)
                    self.newBackPortTcpList.append(ipTcpPort)
                    self.newBackPortUdpList.append(ipUdpPort)
            self.logger.debug("Back tcp  port list %s " % self.newBackPortTcpList)
            self.logger.debug("Back udp port list %s " % self.newBackPortUdpList)
        except Exception, e:
            self.logger.logExit(str(e))
        self.logger.debug("End construct ip-port list")

    ####################################################################################
    # This is the main set cluster firewall flow.  
    ####################################################################################
    def cleanFirewall(self):
        '''
        set cluster firewall 
        '''
        self.__checkParameters()
        if (not os.path.exists(FIREWALL_CONFIG_FILE)):
            self.logger.log("[%s] does not exists,skip clean firewall config" % FIREWALL_CONFIG_FILE)
            return 
        self.__readConfigInfo()
        self.__getClusterIp()
        self.__getClusterPort()
        self.__setIpPortList()

        self.logger.log("Begin clean cluster firewall...")
        try:
            #clean CM GTM DN firewall port 
            if (len(self.newBackPortTcpList)):
                for newIpPort in self.newBackPortTcpList:
                    cmd = ""
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/ %s / /g' %s && " % (newIpPort, FIREWALL_CONFIG_FILE)
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/\\\"%s /\\\"/g' %s && " % (newIpPort, FIREWALL_CONFIG_FILE)
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/ %s\\\"/\\\"/g' %s " % (newIpPort, FIREWALL_CONFIG_FILE)
                    self.logger.debug("clean CM GTM DN firewall Tcp port cmd: " + cmd)
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        self.logger.logExit("config CM GTM DN firewall tcp port failed!Error:%s" % output)
            if (len(self.newBackPortUdpList)):
                for newIpPort in self.newBackPortUdpList:
                    cmd = ""
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/ %s / /g' %s && " % (newIpPort, FIREWALL_CONFIG_FILE)
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/\\\"%s /\\\"/g' %s && " % (newIpPort, FIREWALL_CONFIG_FILE)
                    cmd += "sed -i '/^FW_SERVICES_ACCEPT_EXT=.*$/s/ %s\\\"/\\\"/g' %s " % (newIpPort, FIREWALL_CONFIG_FILE)
                    self.logger.debug("clean CM GTM DN firewall Udp port cmd: " + cmd)
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        self.logger.logExit("config CM GTM DN firewall udp port failed!Error:%s" % output)

            #clean CN firewall port
            if (len(self.newFrontPortTcpList)):
                cmd = "sed -i "
                for newFrontPortTcp in self.newFrontPortTcpList:
                    cmd += "-e '/^FW_SERVICES_EXT_TCP=.*$/s/ %s / /g' %s " % (newFrontPortTcp, FIREWALL_CONFIG_FILE)
                    cmd += "-e '/^FW_SERVICES_EXT_TCP=.*$/s/\\\"%s /\\\"/g' %s " % (newFrontPortTcp, FIREWALL_CONFIG_FILE)
                    cmd += "-e '/^FW_SERVICES_EXT_TCP=.*$/s/ %s\\\"/\\\"/g' %s " % (newFrontPortTcp, FIREWALL_CONFIG_FILE)
                self.logger.debug("Clean CN firewall tcp port cmd: " + cmd)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    self.logger.logExit("Clean CN firewall tcp port failed!Error:%s" % output)
            if (len(self.newFrontPortUdpList)):
                cmd = "sed -i "
                for newFrontPortUdp in self.newFrontPortUdpList:
                    cmd += "-e '/^FW_SERVICES_EXT_UDP=.*$/s/ %s / /g' %s " % (newFrontPortUdp, FIREWALL_CONFIG_FILE)
                    cmd += "-e '/^FW_SERVICES_EXT_UDP=.*$/s/\\\"%s /\\\"/g' %s " % (newFrontPortUdp, FIREWALL_CONFIG_FILE)
                    cmd += "-e '/^FW_SERVICES_EXT_UDP=.*$/s/ %s\\\"/\\\"/g' %s " % (newFrontPortUdp, FIREWALL_CONFIG_FILE)
                self.logger.debug("Clean CN firewall udp port cmd: " + cmd)
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    self.logger.logExit("config CN firewall udp port failed!Error:%s" % output)
        except Exception, e:
            self.logger.logExit("Clean user : " + self.user + " firewall failed, " + str(e))
        self.logger.log("End clean cluster firewall")

if __name__ == '__main__':
    """
    main function
    """
    firewall = CleanFireWall()
    firewall.cleanFirewall()
    
    sys.exit(0)
