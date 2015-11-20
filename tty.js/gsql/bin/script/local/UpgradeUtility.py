import getopt
import shutil
import sys
import os
import socket
import commands

sys.path.append(sys.path[0] + "/../../")
from script.util.GaussLog import GaussLog
from script.util.Common import *
from script.util.OMCommand import OMCommand, ParallelCommand
from script.util.DbClusterInfo import *

DEBUG_SWITCH = True

ACTION_CLEAN_ENV = "clean_env"
ACTION_CHECK_PATH_USAGE = "check_path_usage"
ACTION_START_IN_UPGRADE_MODE = "start_in_upgrade_mode"
ACTION_REPAIR_OLD_CLUSTER = "repair_old_cluster"
ACTION_SET_GUC_PARAMETER = "set_guc_parameter"

g_oldVersionModules = None
g_newClusterInfo = None
g_oldClusterInfo = None
g_logger = None
g_hostname = ""

class CmdOptions():

    def __init__(self):
        self.action = ""
        self.newUser = ""
        self.oldUser = ""
        self.configfile = ""
        self.password_policy_value = None
        self.support_extended_features_value = None
        self.logFile = ""

class OldVersionModules():
    def __init__(self): 
        self.oldDbClusterInfoModule = None
        self.oldDbClusterStatusModule = None

def importOldVersionModules():
    """
    import some needed modules from the old cluster.
    currently needed are: DbClusterInfo
    """
    installDir = DefaultValue.getInstallDir(g_opts.oldUser)
    if(installDir == ""):
        GaussLog.exitWithError("get install of user %s failed." % g_opts.oldUser)
        
    global g_oldVersionModules
    g_oldVersionModules = OldVersionModules()
    sys.path.append("%s/bin/script/util" % installDir)
    g_oldVersionModules.oldDbClusterInfoModule = __import__('DbClusterInfo')

def initGlobals():
    """
    init global variables
    """
    global g_oldVersionModules
    global g_newClusterInfo
    global g_oldClusterInfo
    global g_logger
    global g_hostname
    
    g_logger = GaussLog(g_opts.logFile, g_opts.action)
    g_hostname = socket.gethostname()
    if(g_opts.configfile != ""):
        g_newClusterInfo = dbClusterInfo()
        g_newClusterInfo.initFromXml(g_opts.configfile)
    elif(g_opts.newUser != ""):
        g_newClusterInfo = dbClusterInfo()
        g_newClusterInfo.initFromStaticConfig(g_opts.newUser)
    
    if(g_opts.oldUser != ""):
        importOldVersionModules()
        g_oldClusterInfo = g_oldVersionModules.oldDbClusterInfoModule.dbClusterInfo()
        g_oldClusterInfo.initFromStaticConfig(g_opts.oldUser)

def cleanNewClusterInstallEnv():
    """
    clean install environment for new cluster
    """
    #delete contrb
    commands.getstatusoutput("crontab -r -u " + g_opts.newUser)
    
    # clean user process
    pidList = PlatformCommand.getPIDofUser(g_opts.newUser)
    g_logger.debug("The list of process id is:%s" % pidList)
    for pid in pidList:
        PlatformCommand.KillProcess(pid)
    
    #clean install and data path
    newNodePaths = getNodeDirs()
    g_logger.debug("the following paths will be cleaned:")
    for path in newNodePaths:
        g_logger.debug(path)
    
    for path in newNodePaths:
        if(os.path.exists(path)):
            cmd = "rm -rf %s" % path
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("delete %s failed:%s" % (path, output))

    #clean semaphore
    commands.getstatusoutput("ipcs -s|awk '/ %s /{print $2}'|xargs -n1 ipcrm -s" %  g_opts.newUser)

    #clean user
    status, output = commands.getstatusoutput("userdel -rf %s" % g_opts.newUser)
    if(status != 0):
        g_logger.debug("clean user %s failed. Error: %s" % (g_opts.newUser, output)) 

    #clean tmp path
    tmpDir = DefaultValue.getTmpDirAppendMppdb(g_opts.newUser)()
    g_logger.log("user tmp path is %s" % tmpDir)
    if(os.path.exists(tmpDir)):
        cmd = "rm -rf %s" % tmpDir
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("delete %s failed:%s" % (tmpDir, output))

    #clean log
    logDir = DefaultValue.getUserLogDirWithUser(g_opts.newUser)
    g_logger.log("user log path is %s" % logDir)
    if(os.path.exists(logDir)):
        cmd = "rm -rf %s" % logDir
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("delete %s failed:%s" % (logDir, output))
            

def getNodeDirs():
    """
    get the install path and data path of cluster
    """
    dbNode = g_newClusterInfo.getDbNodeByName(g_hostname)
    if(dbNode == None):
        g_logger.logExit("can not get config infomation about node %s!" % g_hostname)
    newNodePaths = []
    CnDnInfos = []
            
    #collect install path
    newNodePaths.append(g_newClusterInfo.appPath)
    
    #collect data path
    newNodePaths.append(dbNode.cmDataDir)
    for instance in dbNode.coordinators:
        newNodePaths.append(instance.datadir)
        CnDnInfos.append(instance.datadir)
    for instance in dbNode.datanodes:
        newNodePaths.append(instance.datadir)
        if(instance.instanceType != DUMMY_STANDBY_INSTANCE):
            CnDnInfos.append(instance.datadir)
    for instance in dbNode.gtms:
        newNodePaths.append(instance.datadir)

    #collect tablespc path
    for instanceDir in CnDnInfos:
        if not os.path.exists("%s/pg_tblspc" % instanceDir):
            g_logger.debug("%s/pg_tblspc does not exists" % instanceDir)
            continue
        fileList = os.listdir("%s/pg_tblspc" % instanceDir)
        if(len(fileList)):
            for filename in fileList:
                if(os.path.islink("%s/pg_tblspc/%s" % (instanceDir, filename))):
                    linkDir = os.readlink("%s/pg_tblspc/%s" % (instanceDir, filename))
                    newNodePaths.append(linkDir)
                else:
                    g_logger.debug("%s is not a link file" % filename)
        else:
            g_logger.debug("%s/pg_tblspc is empty" % instanceDir)
    
    return newNodePaths

def checkDiskUsage():
    """
    check dist usage
    """
    #get install and data path
    newNodePaths = getNodeDirs()
    g_logger.debug("the usage of following paths will be checked:")
    for path in newNodePaths:
        g_logger.debug(path)

    #check and clean warning msg record file
    warningMsgRecordFile = DefaultValue.getWarningFilePath(g_opts.newUser)
    if os.path.isfile(warningMsgRecordFile):
        os.remove(warningMsgRecordFile)
    elif os.path.isdir(warningMsgRecordFile):
        shutil.rmtree(warningMsgRecordFile)

    #create a empty file for collect later
    warningFilePath = os.path.dirname(warningMsgRecordFile)
    if(not os.path.exists(warningFilePath)):
        os.makedirs(warningFilePath)
    fp = open(warningMsgRecordFile, 'w')
    fp.close()
    
    #check path usage one by one
    for path in newNodePaths:
        checkSingleDirUsage(path, warningMsgRecordFile)

def checkSingleDirUsage(path, warningMsgRecordFile, percentage = 50):
    """
    """
    #if path does not exists, just return
    if(not os.path.exists(path)):
        return
        
    cmd = "df -P -B 1048576 %s | awk '{print $5}' | tail -1" % path
    (status, output) = commands.getstatusoutput(cmd)
    if(status == 0):
        resList = output.split('\n')
        rateNum = int(resList[0].split('%')[0])
        if(rateNum > percentage):
            warningMsg = "[%s]Warning:the usage of path (%s) is %d%%, more than %d%%." % (g_hostname, path, rateNum, percentage)
            cmd = "echo '%s'  >> %s 2>/dev/null" % (warningMsg, warningMsgRecordFile)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("save path(%s) usage warning info to %s failed!Error:%s" % (path, warningMsgRecordFile, output))
        else:
            g_logger.debug("the usage of path (%s) is %d%%" % (path, rateNum))  
    else:
        g_logger.logExit("get path(%s) usage failed!Error: %s" % (path, output))

def startNodeInUpgradeMode():
    """
    """
    #get node config info
    if(g_opts.oldUser != ""):
        userForStartInUpgradeMode = g_opts.oldUser
        dbNode = g_oldClusterInfo.getDbNodeByName(g_hostname)
    elif(g_opts.newUser != ""):
        userForStartInUpgradeMode = g_opts.newUser
        dbNode = g_newClusterInfo.getDbNodeByName(g_hostname)
    else:
        g_logger.logExit("you need to provide a user name to start node in upgrade mode!")
    if(dbNode == None):
        g_logger.logExit("can not get config infomation about node %s!" % g_hostname)

    #start instance in upgrade mode
    for instance in dbNode.coordinators:
        cmd = "su - %s -c 'gs_ctl start -Z coordinator -D %s -o \"-b\"'" % (userForStartInUpgradeMode, instance.datadir)
        g_logger.debug("start cn cmd:%s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            g_logger.logExit("start cn(%s) failed:%s" % (instance.datadir, output))
    for instance in dbNode.datanodes:
        if(hasattr(instance, 'isMaster')):
            cmd = "su - %s -c 'gs_ctl start -Z datanode -D %s -M pending -o \"-b\"'" % (userForStartInUpgradeMode, instance.datadir)
            g_logger.debug("start datanode cmd:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("start dn(%s) failed:%s" % (instance.datadir, output))
        elif(hasattr(instance, 'instanceType')):
            if(instance.instanceType == DUMMY_STANDBY_INSTANCE):continue
            cmd = "su - %s -c 'gs_ctl start -Z datanode -D %s -M pending -o \"-b\"'" % (userForStartInUpgradeMode, instance.datadir)
            g_logger.debug("start datanode cmd:%s" % cmd)
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                g_logger.logExit("start dn(%s) failed:%s" % (instance.datadir, output))
        else:
            g_logger.logExit("the script file may be modified unexcepted, please recover it first!")

    #start current node
    cmd = "su - %s -c 'cm_ctl start -n %s'" % (userForStartInUpgradeMode, dbNode.id)
    g_logger.debug("start node command is %s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        raise Exception("start node failed:%s" % output)
    

def repairOldCluster():
    """
    """
    #get node config info
    dbNode = g_oldClusterInfo.getDbNodeByName(g_hostname)
    if(dbNode == None):
        g_logger.logExit("can not get config infomation about node %s!" % g_hostname)

    #collect cn and dn instance data path
    dataPath = []
    for instance in dbNode.coordinators:
        dataPath.append(instance.datadir)
    for instance in dbNode.datanodes:
        if(hasattr(instance, 'isMaster')):
            if(instance.isMaster):
                dataPath.append(instance.datadir)
        elif(hasattr(instance, 'instanceType')):
            if(instance.instanceType == MASTER_INSTANCE):
                dataPath.append(instance.datadir)
        else:
            g_logger.logExit("the script file may be modified unexcepted, please recover it first!")

    #repair cn and dn instance
    for path in dataPath:
        recoverOneInstance(path, g_opts.oldUser)

def recoverOneInstance(instanceDataDir, user):
    """
    """
    pg_controlOldFile = "%s/global/pg_control.old" % instanceDataDir
    pg_controlFile = "%s/global/pg_control" % instanceDataDir
    if(not os.path.isfile(pg_controlOldFile)):
        g_logger.debug("Warning: %s does not exist!" % pg_controlOldFile)
        return
    cmd = "su - %s -c 'mv %s %s'" % (user, pg_controlOldFile, pg_controlFile)
    g_logger.debug("recover coordinator cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        g_logger.logExit("repair instance(%s) failed:%s" % (instanceDataDir, output))

def setNodeGUCParameters():
    """
    set guc parameter for current node
    """
    #get node config info
    dbNode = g_newClusterInfo.getDbNodeByName(g_hostname)
    if(dbNode == None):
        g_logger.logExit("can not get config infomation about node %s!" % g_hostname)

    #set cn instance guc parameter
    for instance in dbNode.coordinators:
        setGUCParameters(g_opts.newUser, instance, g_opts.password_policy_value, g_opts.support_extended_features_value)

    #set dn instances guc parameter
    for instance in dbNode.datanodes:
        if(instance.instanceType == MASTER_INSTANCE or instance.instanceType == STANDBY_INSTANCE):
            setGUCParameters(g_opts.newUser, instance, g_opts.password_policy_value, g_opts.support_extended_features_value)    
    

def setGUCParameters(user, dbInst, password_policy_value, support_extended_features_value, mode = "set"):
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
        g_logger.debug("Don't need to set: %s" % dbInst.datadir)
        return
        
    pgCmd = "gs_guc %s -Z %s -N %s -D %s -c \\\"password_policy=%d\\\"" % (mode, instType, dbInst.hostname, dbInst.datadir, password_policy_value)
    pgCmd = "su - %s -c \"%s\"" % (user, pgCmd)
    g_logger.debug("set cmd is %s" % pgCmd)
    (status, output) = commands.getstatusoutput(pgCmd)
    if (status != 0):
        g_logger.debug(output)
        g_logger.logExit("Set instance password_policy failed!Data %s" % dbInst.datadir)
        
    pgCmd = "gs_guc %s -Z %s -N %s -D %s -c \\\"support_extended_features=%s\\\"" % (mode, instType, dbInst.hostname, dbInst.datadir, support_extended_features_value)
    pgCmd = "su - %s -c \"%s\"" % (user, pgCmd)
    g_logger.debug("set cmd is %s" % pgCmd)
    (status, output) = commands.getstatusoutput(pgCmd)
    if (status != 0):
        g_logger.debug(output)
        g_logger.logExit("Set support extended features failed!Data %s" % dbInst.datadir)

def usage():
    """
Usage:
  python UpgradeUtility.py -t action [-u newUser] [-U oldUser] [-X xmlfile] [--password_policy_value=value] [--support_extended_features=value] [-l log]
Common options:
  -t                                the type of action
  -u                                the user of new cluster
  -U                                the user of old cluster
  -X                                the xml file path
  -l                                the path of log file
  --password_policy_value=value     the value of password_policy
  --support_extended_features=value the value of support_extended_features
  --help                            show this help, then exit
    """
    print usage.__doc__
    
def parseCommandLine():
    """
    Parse command line and save to global variables
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:u:U:X:l:", ["password_policy_value=", "support_extended_features=", "help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-u"):
            g_opts.newUser = value
        elif(key == "-U"):
            g_opts.oldUser = value
        elif (key == "-X"):
            g_opts.configfile = value
        elif (key == "--password_policy_value"):
            g_opts.password_policy_value = value
        elif (key == "--support_extended_features"):
            g_opts.support_extended_features_value = value
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

def checkParameter():
    """
    check parameter for different ation
    """
    if (g_opts.action == ""):
        GaussLog.exitWithError("Parameter input error, need '-t' parameter.")
    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.newUser, "")
    
    if (g_opts.action == ACTION_CLEAN_ENV):
        checkCleanEnvParameter()
    elif (g_opts.action == ACTION_CHECK_PATH_USAGE):
        checkPathUsageParameter()
    elif(g_opts.action == ACTION_START_IN_UPGRADE_MODE):
        checkStartInUpgradeModeParameter()
    elif (g_opts.action == ACTION_REPAIR_OLD_CLUSTER):
        checkRepairOldClusterParameter()
    elif (g_opts.action == ACTION_SET_GUC_PARAMETER):
        checkSetNodeGUCParameters()
    else:
        GaussLog.exitWithError("Invalid Action : %s" % g_opts.action)

def checkCleanEnvParameter():
    """
    """
    if (g_opts.newUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")
    if (g_opts.configfile == ""):
        GaussLog.exitWithError("Parameter input error, need '-X' parameter.")

def checkPathUsageParameter():
    """
    """
    if (g_opts.newUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")
    if (g_opts.configfile == ""):
        GaussLog.exitWithError("Parameter input error, need '-X' parameter.")

def checkStartInUpgradeModeParameter():
    """
    """
    if((g_opts.newUser == "" and g_opts.oldUser == "") or (g_opts.newUser != "" and g_opts.oldUser != "")):
        GaussLog.exitWithError("Parameter input error, '-U' and '-u' should provide only one in current action!")

def checkRepairOldClusterParameter():
    """
    """
    if (g_opts.oldUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")

def checkSetNodeGUCParameters():
    """
    """
    if (g_opts.newUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")
        
    if (g_opts.password_policy_value == None):
        GaussLog.exitWithError("Parameter input error, need '--password_policy_value' parameter.")
    else:
        if(not g_opts.password_policy_value.isdigit()):
            GaussLog.exitWithError("Parameter input error, '--password_policy_value' parameter should be integer.")
        #should convert it to int
        g_opts.password_policy_value = int(g_opts.password_policy_value)

    if (g_opts.support_extended_features_value == None):
        GaussLog.exitWithError("Parameter input error, need '--support_extended_features' parameter.")



if __name__ == '__main__':
    """
    main function
    """
    g_opts = CmdOptions()
    parseCommandLine()
    checkParameter()
    initGlobals()
    
    if(g_opts.action == ACTION_CLEAN_ENV):
        cleanNewClusterInstallEnv()
    elif(g_opts.action ==  ACTION_CHECK_PATH_USAGE):
        checkDiskUsage()
    elif(g_opts.action == ACTION_START_IN_UPGRADE_MODE):
        startNodeInUpgradeMode()
    elif(g_opts.action == ACTION_REPAIR_OLD_CLUSTER):
        repairOldCluster()
    elif(g_opts.action == ACTION_SET_GUC_PARAMETER):
        setNodeGUCParameters()
    else:
        g_logger.logExit("Parameter input error, unknown action:%s" % g_opts.action)



