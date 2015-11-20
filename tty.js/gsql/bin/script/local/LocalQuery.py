'''
Created on 2014-3-1

@author: 
'''
import commands
import getopt
import sys
import time
import os
import re
import shutil

sys.path.append(sys.path[0] + "/../../")
try:
    from script.util.Common import DefaultValue, ClusterCommand, PlatformCommand
    from script.util.DbClusterInfo import *
    from script.util.GaussLog import GaussLog
    from script.util.OMCommand import LocalCommand, CommandThread, CommandProcess
except ImportError, e:
    sys.exit("ERROR: Cannot import modules: %s" % str(e))

g_clusterUser = ""

class LocalQuery(LocalCommand):
    """
    Do query on local node
    """
    def __init__(self, logFile, user):
        LocalCommand.__init__(self, logFile, user)
        self.lockThread = None
        self.schemaCoordinatorFile = ""
        self.schemaDatanodeFile = ""
        
    def init(self):
        """
        Init info
        """
        self.logger.log("Begin to init...")
        
        self.readConfigInfo()
        self.getUserInfo()
        self.__getSqlFileInfo()
        
        self.logger.log("Init finished...")
        
    def uninit(self):
        """
        Release the cluster
        """
        if (self.lockThread is not None):
            pass
        
        self.logger.log("Uninit finished.")
        self.logger.closeLog()
    
    ### if we will change the lock cluster sql, change the unlock sql file too.
    LOCK_CLUSTER_SQL = "select case (select pgxc_lock_for_backup()) when true then (select pg_sleep(%d)) end;"
    LOCK_CLUSTER_CMD = "(gsql -p %d postgres -X -c %s &)"
    UNLOCK_SQL_FILE = "bin/script/local/unlock_cluster.sql"
    LEAST_LOCK_TIME = 5
    def lockCluster(self, setDaemon, lockTime):
        """
        Lock the cluster
        """
        self.logger.log("Begin to lock cluster...")
        
        try:
            if (len(self.dbNodeInfo.coordinators) == 0):
                    self.logger.logExit("There is no coordinator on local node!")
            cooInst = self.dbNodeInfo.coordinators[0]
            
            sql = self.LOCK_CLUSTER_SQL % max(self.LEAST_LOCK_TIME, lockTime)
            cmd = self.LOCK_CLUSTER_CMD % (cooInst.port, "\"%s\"" % sql)
            
            if (not setDaemon):
                self.lockThread = CommandThread(cmd)
                self.lockThread.start()
                self.logger.log("lock cluster thread has started.")
            
                time.sleep(self.LEAST_LOCK_TIME)
                if (not self.lockThread.isAlive() or self.lockThread.cmdStauts != 0 or (self.lockThread.cmdOutput != ""
                    and self.lockThread.cmdOutput.find("please do not close this session until you are done adding the new node") < 0)):
                    self.logger.logExit("Lock cluster failed!Output:%s" % self.lockThread.cmdOutput)
            else:
                self.lockThread = CommandThread(cmd)
                self.lockThread.setDaemon(True)
                self.lockThread.start()
                self.logger.log("lock cluster thread has started.")
                time.sleep(self.LEAST_LOCK_TIME)
                if (not self.lockThread.isAlive()):
                    self.logger.log("Lock cluster failed! retrying...")
                    self.releaseCluster(lockTime, False)
                    self.lockThread = CommandThread(cmd)
                    self.lockThread.setDaemon(True)
                    self.lockThread.start()
                    time.sleep(self.LEAST_LOCK_TIME)
                    if (not self.lockThread.isAlive()):
                        self.releaseCluster(lockTime, False)
                        self.logger.logExit("Lock cluster failed!")
                self.logger.log("Lock cluster success.")
                  
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.log("Lock cluster finished.")
        
    def releaseCluster(self, lockTime, killProc = True):
        if (len(self.dbNodeInfo.coordinators) == 0):
            self.logger.logExit("There is no coordinator on local node!")
        cooInst = self.dbNodeInfo.coordinators[0]
        
        ### first, try to connect to database and cancel backend.
        unlockCmd = "gsql -p %d postgres -X -f %s" % (cooInst.port, os.path.join(self.clusterInfo.appPath, self.UNLOCK_SQL_FILE))		
        self.logger.debug("unlock cmd %s" % unlockCmd)
        (status, output) = commands.getstatusoutput(unlockCmd)
        if (status != 0):
            self.logger.debug("unlock cluster failed! status: %d" % status)
        self.logger.debug("unlock cluster result:\n%s" % output)
        if (not killProc):
            return
        psCmd = "ps xo pid,cmd"
        (status, output) = commands.getstatusoutput(psCmd)
        if (status != 0):
            self.logger.logExit("Query process failed!Output: %s" % output)            
        self.logger.debug("Query result:\n%s" % (output))
        sql = self.LOCK_CLUSTER_SQL % max(self.LEAST_LOCK_TIME, lockTime)
        targetCmd = self.LOCK_CLUSTER_CMD % (cooInst.port, sql)
        pids = []
        for line in output.split("\n"):
            pid, cmd = line.strip().split(" ", 1)
            if (cmd.strip().find(targetCmd) == 0):
                self.logger.debug("find pid %s.cmd \n%s" % (pid, cmd))
                pids.append(pid)               
        if (len(pids) != 0):
            killCmd = "kill -2 %s" % (" ".join(pids))
            self.logger.debug("Kill cmd %s" % killCmd)
            (status, output) = commands.getstatusoutput(killCmd)
            if (status != 0):
                self.logger.logExit("Kill sigint signal failed!Output: %s" % output)
        else:
            self.logger.debug("Can't find lock cluster process.")
    def dropXCNode(self, nodeList):
        """
        Drop record from pgxc_node
        """
        self.logger.log("Begin to drop xc node...")
        
        try:
            sql = ""
            for name in nodeList:
                sql += "DROP NODE %s;" % name
            sql += "SELECT pgxc_pool_reload();"
            
            if (len(self.dbNodeInfo.coordinators) == 0):
                self.logger.logExit("There is no coordinator on local node!")
            cooInst = self.dbNodeInfo.coordinators[0]
            
            (status, output) = ClusterCommand.execSQLCommand(sql, self.user, "localhost", cooInst.port)
            if (status != 0):
                self.logger.logExit("Drop record from pgxc_node failed!Output: %s" % output)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.log("Drop xc node finished!")
    
    def createNewCoo(self, cnList):
        """
        Create new coordinator node info
        """
        self.logger.log("Begin to create new coordinator...")
        
        try:
            sql = ""
            for cnInfo in cnList:
                infoList = cnInfo.split(":")
                sql += "CREATE NODE %s WITH (type='coordinator', host='%s', port=%s);" % (infoList[0], infoList[1], infoList[2])
            sql += "SELECT pgxc_pool_reload();"
            
            if (len(self.dbNodeInfo.coordinators) == 0):
                self.logger.logExit("There is no coordinator on local node!")
            cooInst = self.dbNodeInfo.coordinators[0]
            
            (status, output) = ClusterCommand.execSQLCommand(sql, self.user, "localhost", cooInst.port)
            if (status != 0):
                self.logger.logExit("Create new coordinator node failed!Output: %s" % output)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.log("Create new coordinator node finished!")
    
    def buildInstance(self, buildNode):
        """
        Build new instances
        """
        self.logger.log("Begin to restore instance ...")
        
        dbInst = None
        try:
            if (buildNode == "coordinators"):
                self.logger.log("build coordinators...")
                instances = self.dbNodeInfo.coordinators
            elif (buildNode == "datanodes"):
                self.logger.log("build datanodes...")
                instances = self.dbNodeInfo.datanodes
            elif (buildNode == "all"):
                self.logger.log("build all...")
                instances = self.dbNodeInfo.coordinators + self.dbNodeInfo.datanodes
            else:
                self.logger.logExit("invalid bulid-node commands: %s" % buildNode)
            for dbInst in instances:
                if (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR or dbInst.instanceType == MASTER_INSTANCE or dbInst.instanceType == STANDBY_INSTANCE):
                    self.logger.debug("begin set passwd policy")
                    self.__setGUCParameters(dbInst, 0, "true")
                    self.logger.debug("begin start restore instance")
                    self.__startRestoreInstance(dbInst)
                    self.logger.debug("begin restore instance")
                    self.__restoreInstance(dbInst)
                    self.logger.debug("begin stop restore instance")
                    self.__stopRestoreInstance(dbInst)
                    self.logger.debug("begin set passwd policy")
                    self.__setGUCParameters(dbInst, 1, "false")
    
            dbInst = None
        except Exception, e:
            if (dbInst):
                self.__stopRestoreInstance(dbInst)
                self.__setGUCParameters(dbInst, 1, "false")
            self.logger.logExit(str(e))
            
        self.logger.log("Restore instance successfully.")
    def IgnorePatternFunction_InitialUser(self):
        INITIAL_USER_IGNORE_PATTERN = "^(CREATE|ALTER) (ROLE|USER) (%s|\"%s\"|'%s')" % (self.user, self.user, self.user)
        return re.compile(INITIAL_USER_IGNORE_PATTERN)
    def IgnorePatternFunction_CstoreSchema(self):
        CSTORE_SCHEMA_IGNORE_PATTERN = "^CREATE SCHEMA cstore;"
        return re.compile(CSTORE_SCHEMA_IGNORE_PATTERN)
    def getIgnorePatterns(self):
        list = []
        for i in dir(self):
            if (i.startswith("IgnorePatternFunction_")):
                self.logger.debug("find function %s" % i)
                list.append(getattr(self, i)())
        return list    
    def needToBeClean(self, line, patterns):
        line = line.strip()
        for pattern in patterns:
            if (pattern.match(line) is not None):
                return True
        return False
    def cleanSchemaFile(self, desc, file):
        self.logger.log("Begin to clean %s schema file..." % desc)
        self.logger.log("file: %s" % file)
        try:
            patterns = self.getIgnorePatterns()
            fp = open(file, "r")
            readLines = fp.readlines()
            fp.close()
            writeLines = []
            for line in readLines:
                if (self.needToBeClean(line, patterns)):
                    self.logger.log("This line will not add to schema file:\n%s" % (line.strip("\n")))
                    continue
                writeLines.append(line)
            fp = open(file, "w")
            fp.writelines(writeLines)
            fp.close()
        except Exception as e:
            raise Exception("Clean %s schema file failed. Error:\n%s" % (desc, str(e)))
        self.logger.log("Clean %s schema file successfully." % desc)
    
    def dumpCooSchema(self):
        """
        Dump schema from coordinator
        """
        self.logger.log("Begin to dump coordinator schema...")
        
        try:
            if (len(self.dbNodeInfo.coordinators) == 0):
                self.logger.logExit("There is no coordinator on local node!")
            cooInst = self.dbNodeInfo.coordinators[0]
            
            if(os.path.exists(self.schemaCoordinatorFile)):
                try:
                    os.remove(self.schemaCoordinatorFile)      
                except:
                    pass  
            cmd = "gs_dumpall -p %d -s --include-nodes --dump-nodes --include-buckets --dump-wrm --file=%s" % \
                    (cooInst.port, self.schemaCoordinatorFile)
            self.logger.debug("Dump coordinator command:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Dump the schema of coordinator failed!Output: %s" % output)
            
            ips = ",".join(cooInst.listenIps)
            cmd = "echo \"ALTER NODE cn_%d WITH (HOST = '%s', HOST1 = '%s');\" >> %s" % (cooInst.instanceId, ips, ips, self.schemaCoordinatorFile)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Append alter sql failed!Output: %s" % output)
            self.cleanSchemaFile("coordinator", self.schemaCoordinatorFile)
        except Exception, e:
            self.logger.logExit(str(e))
        
        self.logger.log("Dump coordinator schema successfully.")
    def dumpDnSchema(self):
        """
        Dump schema from datanode
        """
        self.logger.log("Begin to dump datanode schema...")
        try:
            if (len(self.dbNodeInfo.datanodes) == 0):
                self.logger.logExit("There is no datanodes on local node!")
            dumpSuccess = False
            for dnInst in self.dbNodeInfo.datanodes:
                if(os.path.exists(self.schemaDatanodeFile)):
                    try:
                        os.remove(self.schemaDatanodeFile)      
                    except:
                        pass 
                try:
                    cmd = "gs_dumpall -p %d -s --file=%s" % (dnInst.port, self.schemaDatanodeFile)
                    self.logger.debug("Dump datanode command:%s" % cmd)
                    (status, output) = commands.getstatusoutput(cmd)
                    if (status != 0):
                        self.logger.debug("Dump the schema of datanode[%s] failed!Output: %s" % (dnInst.datadir, output))
                    else:
                        dumpSuccess = True
                        break
                except Exception as e:
                    self.logger.debug("Dump the schema of datanode[%s] failed!Error: %s" % (dnInst.datadir, str(e)))  
            if (not dumpSuccess):
                raise Exception("Dump datanode schema on [%s] failed." % (self.dbNodeInfo.name))
            self.cleanSchemaFile("datanode", self.schemaDatanodeFile)
        except Exception, e:
            self.logger.logExit(str(e))
        self.logger.log("Dump datanode schema successfully.")

    def __startRestoreInstance(self, dbInst):
        """
        Start local instances in restore mode
        """
        pgCmd = "gs_ctl start -Z restoremode -D %s" % dbInst.datadir
        self.logger.debug("start local instance in restore mode cmd is %s" % pgCmd)
        (status, output) = commands.getstatusoutput(pgCmd)
        if (status != 0):
            self.logger.debug(output)
            raise Exception("Start instance in restore mode failed!Datadir: %s" % dbInst.datadir)
    
    def __stopRestoreInstance(self, dbInst):
        """
        Stop local instance in restore mode
        """
        pgCmd = "gs_ctl stop -Z restoremode -D %s" % dbInst.datadir 
        self.logger.debug("stop local instance in restore mode cmd is %s" % pgCmd)
        (status, output) = commands.getstatusoutput(pgCmd)
        if (status != 0):
            self.logger.debug("Stop instance failed!Output: %s" % output)
            

    def __setGUCParameters(self, dbInst, password_policy_value, support_extended_features_value, mode = "set"):
        """
        set two guc parameters: 
        password_policy: There is no need to check passwd policy for restoring database  
        support_extended_features: adapt SQL blank list for gs_dump
        """
        instType = ""
        if (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            instType = "coordinator"
        elif (dbInst.instanceRole == INSTANCE_ROLE_DATANODE):
            instType = "datanode"
        else:
            self.logger.debug("Don't need to restore: %s" % dbInst.datadir)
            return
        pgCmd = "gs_guc %s -Z %s -N %s -D %s -c \"password_policy=%d\"" % (mode, instType, dbInst.hostname, dbInst.datadir, password_policy_value)
        self.logger.debug("set cmd is %s" % pgCmd)
        (status, output) = commands.getstatusoutput(pgCmd)
        if (status != 0):
            self.logger.debug(output)
            raise Exception("Set instance password_policy failed!Data %s" % dbInst.datadir)
            
        pgCmd = "gs_guc %s -Z %s -N %s -D %s -c \"support_extended_features=%s\"" % (mode, instType, dbInst.hostname, dbInst.datadir, support_extended_features_value)
        self.logger.debug("set cmd is %s" % pgCmd)
        (status, output) = commands.getstatusoutput(pgCmd)
        if (status != 0):
            self.logger.debug(output)
            raise Exception("Set support extended features failed!Data %s" % dbInst.datadir)
            
    def findError(self, sqlFile, output):
        GSQL_BIN_FILE = "gsql"
        ERROR_MSG_FLAG = "(ERROR|FATAL|PANIC)"
        GSQL_ERROR_PATTERN = "^%s:%s:(\d*): %s:.*" % (GSQL_BIN_FILE, sqlFile, ERROR_MSG_FLAG)
        pattern = re.compile(GSQL_ERROR_PATTERN)
        for line in output.split("\n"):
            line = line.strip()
            result = pattern.match(line)
            if (result is not None):
                self.logger.log("This line contain errors:\n%s" % line)
                return True
        return False
    def __restoreInstance(self, dbInst):
        """
        Build new instance.Restore from dump file
        """
        sqlFile = ""
        if (dbInst.instanceRole == INSTANCE_ROLE_COODINATOR):
            sqlFile = self.schemaCoordinatorFile
            pgCmd = "gsql -d postgres -X -f %s -p %d" % (sqlFile, dbInst.port)
        elif (dbInst.instanceRole == INSTANCE_ROLE_DATANODE):
            sqlFile = self.schemaDatanodeFile
            pgCmd = "gsql -d postgres -X -f %s -p %d" % (sqlFile, dbInst.port)
        else:
            self.logger.debug("Don't need to restore: %s" % dbInst.datadir)
            return
        self.logger.debug("begin create table space")    
        self.__createTableSpaceDir(sqlFile, dbInst.instanceRole, dbInst.instanceId)
 
        (status, output) = commands.getstatusoutput(pgCmd)
        self.logger.debug(output)
        if (status != 0 or self.findError(sqlFile, output)):
            raise Exception("Restore instance failed!Data %s" % dbInst.datadir)
            
    def __createTableSpaceDir(self, sqlFile, instanceRole, instanceId):
        """
        Check if table space dir exists before restore, if not, create it first.
        """
        matchMode = '^ *CREATE TABLESPACE .* LOCATION .*'
        cmd = "grep -G '%s' %s 2>/dev/null" % (matchMode, sqlFile)
        self.logger.debug("search tablespace dir cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status == 0):
            pass
        elif(status == 256):
            self.logger.debug("There is no tablespace in current sql file.")
            return
        else:
            raise Exception("Get tablespace directory failed!Errror %s" % output)
            
        sqlList = output.split('\n')
        dirNum = len(sqlList)
        for i in range(dirNum):
            if(re.findall(matchMode, sqlList[i]) == []):
                raise Exception("The model of sql(%s) is invalid! Please check it fisrt." % sqlList[i])

        for i in range(dirNum):
            tablespaceDir = ""
            resList = sqlList[i].split(' ')
            j = 0
            while(resList[j] != "LOCATION"): j += 1
            tablespaceDir = resList[j + 1]
            if(tablespaceDir[-1] == ";"):
                tablespaceDir = tablespaceDir[:-1] 
            
            self.logger.debug("Tablespace directory is %s" % tablespaceDir)
            cmd = "(if [ ! -d %s ];then mkdir -p %s -m 700;fi)" % (tablespaceDir, tablespaceDir)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Check tablespace directory[%s] failed!Error: %s" % (tablespaceDir, output))
                
            cmd = "chown %s:%s %s" % (self.user, self.group, tablespaceDir)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Change owner of tablespace directory[%s] failed!Error: %s" % (tablespaceDir, output))

            cmd = "chmod 700 %s" % tablespaceDir
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit("Change mode of tablespace directory[%s] failed!Error: %s" % (tablespaceDir, output))
            
            if (not PlatformCommand.checkDirWriteable(tablespaceDir, self.user)):
                self.logger.logExit("Tablespace directory[%s] is not writeable for database administrator." % tablespaceDir)

            if (instanceRole == INSTANCE_ROLE_COODINATOR):
                instancename = "cn_%s" % instanceId
                instanceTblspcDir = "%s/%s_%s" % (tablespaceDir, DefaultValue.TABLESPACE_VERSION_DIRECTORY, instancename)
                cmd = "(if [ -d %s ];then rm -rf %s ;fi)" % (instanceTblspcDir, instanceTblspcDir)
                (status, output) = commands.getstatusoutput(cmd)
                if (status != 0):
                    self.logger.logExit("Delete instance tablespace directory[%s] failed!Error: %s" % (instanceTblspcDir, output))
    
    def __getSqlFileInfo(self):
        """
        Get sql file path
        """
        tmpDir = DefaultValue.getTmpDirFromEnv()
        self.schemaCoordinatorFile = "%s/%s" % (tmpDir, DefaultValue.SCHEMA_COORDINATOR)
        self.schemaDatanodeFile = "%s/%s" % (tmpDir, DefaultValue.SCHEMA_DATANODE)
        
def usage():
    """
Usage:
    python LocalQuery.py --help
    python LocalQuery.py -U user [-l log]
    """
    
    print usage.__doc__
    
def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "U:l:h", ["lock-cluster", "release-cluster", "set-daemon", "lock-time=", "drop-node=", "create-cn=", "build-node=", "dump-cn", "dump-dn", "help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError(str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    global g_clusterUser
    logFile = ""
    dumpCn = False
    dumpDn = False
    buildNode = None
    lockCluster = False
    releaseCluster = False
    setDaemon = False
    lockTime = 30
    dropList = []
    newCnList = []
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-U"):
            g_clusterUser = value
        elif (key == "-l"):
            logFile = value
        elif (key == "--lock-cluster"):
            lockCluster = True
        elif (key == "--release-cluster"):
            releaseCluster = True
        elif (key == "--set-daemon"):
            setDaemon = True
        elif (key == "--lock-time"):
            lockTime = int(value)
        elif (key == "--drop-node"):
            dropList.append(value)
        elif (key == "--create-cn"):
            newCnList.append(value)
        elif (key == "--dump-cn"):
            dumpCn = True
        elif (key == "--dump-dn"):
            dumpDn = True
        elif (key == "--build-node"):
            buildNode = value
        else:
            GaussLog.exitWithError("Parameter input error, unknown options %s." % key)

    # check if user exist and is the right user
    if(g_clusterUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
    PlatformCommand.checkUser(g_clusterUser)
        
    if (logFile == ""):
        logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_clusterUser, "")
        
    if (not os.path.isabs(logFile)):
        GaussLog.exitWithError("Parameter input error, log file need absolute path.")       
        
    query = LocalQuery(logFile, g_clusterUser)
    
    # Notice: don't change the order
    query.init()
    if (len(dropList) != 0):
        query.dropXCNode(dropList)

    if (lockCluster):
        query.lockCluster(setDaemon, lockTime)
    if (dumpCn):
        query.dumpCooSchema()
    if (dumpDn):
        query.dumpDnSchema()
    if (releaseCluster):
        query.releaseCluster(lockTime)
        
    if (buildNode is not None):
        query.buildInstance(buildNode)
    
    if (len(newCnList) != 0):
        query.createNewCoo(newCnList)
    query.uninit()
    
    sys.exit(0)

if __name__ == '__main__':
    main()
