import getopt
import sys
import os
import socket
import commands

sys.path.append(sys.path[0] + "/../../")
try:
    from script.util.Common import *
    from script.util.GaussLog import GaussLog
    from script.util.DbClusterInfo import *
except ImportError, e:
    sys.exit("ERROR: Cannot import modules: %s" % str(e))

#global variables
g_logger = None
g_opts = None
g_oldClusterInfo = None
g_newClusterInfo = None
g_oldClusterGroupInfo = []
g_newClusterGroupInfo = []
g_hostname = None
g_oldVersionModules = None
group_info_copy_from_file = ""



class OldVersionModules():
    def __init__(self): 
        self.oldDbClusterInfoModule = None
        self.oldDbClusterStatusModule = None

class CmdOptions():
    def __init__(self):
        self.oldUser = ""
        self.newUser = ""
        self.bucketNum = None
        self.logFile = ""

def usage():
    """
Usage:
  python upgradePhase2Cmd_530To1130.py -u newUser -U oldUser -N bucketnum [-l log]
Common options:
  -u                                the user of new cluster
  -U                                the user of old cluster
  -N                                the number of bucket in new cluster
  -l                                the path of log file
  --help                            show this help, then exit
    """
    print usage.__doc__

def parseCommandLine():
    """
    Parse command line and save to global variable
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:U:N:l:", ["help"])
    except Exception, e:
        usage()
        GaussLog.exitWithError("Error: %s" % str(e))
        
    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error: %s" % str(args[0]))
    
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-u"):
            g_opts.newUser = value
        elif (key == "-U"):
            g_opts.oldUser = value
        elif (key == "-N"):
            g_opts.bucketNum = value
        elif (key == "-l"):
            g_opts.logFile = os.path.abspath(value)
        else:
            GaussLog.exitWithError("Unknown parameter:%s" % key)

    if (g_opts.oldUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-U' parameter.")
        
    if (g_opts.newUser == ""):
        GaussLog.exitWithError("Parameter input error, need '-u' parameter.")

    if (g_opts.bucketNum == None):
        GaussLog.exitWithError("Parameter input error, need '-N' parameter.")
    else:
        if(not g_opts.bucketNum.isdigit()):
            GaussLog.exitWithError("Parameter input error, '-N' parameter should be integer.")
        #should convert it to int
        g_opts.bucketNum = int(g_opts.bucketNum)
        if(g_opts.bucketNum <= 0 or (g_opts.bucketNum % 1024) != 0):
            GaussLog.exitWithError("'-N' parameter should be larger than 0 and should be a multiple of 1024.")

    if(g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.DEFAULT_LOG_FILE, g_opts.oldUser)

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
    

def initGlobalInfos():
    """
    """
    global g_logger
    global g_oldClusterInfo
    global g_newClusterInfo
    global g_hostname
    global group_info_copy_from_file
    
    g_logger = GaussLog(g_opts.logFile, "upgradePhase2cmd")
    
    importOldVersionModules()
    g_oldClusterInfo = g_oldVersionModules.oldDbClusterInfoModule.dbClusterInfo()
    g_oldClusterInfo.initFromStaticConfig(g_opts.oldUser)

    g_newClusterInfo = dbClusterInfo()
    g_newClusterInfo.initFromStaticConfig(g_opts.newUser)
    
    g_hostname = socket.gethostname()
    
    tmpDir = DefaultValue.getTmpDirAppendMppdb(g_opts.newUser)
    group_info_copy_from_file = "%s/groupinfo_in" % tmpDir

def checkCNNum():
    """
    """
    g_logger.log("begin check cn num on current node...")
    try:
        currentNode = g_newClusterInfo.getDbNodeByName(g_hostname)
        if(currentNode == None):
            raise Exception("there is no node named %s in current cluster" % g_hostname)
        if(len(currentNode.coordinators) != 1):
            g_logger.debug("there is no cn on this node, nothing need to do.")
            g_logger.closeLog()
            sys.exit(0)

    except Exception, e:
        g_logger.log("check cn num on current node failed:%s" % str(e))
        raise Exception("check cn num on current node failed:%s" % str(e))

def getOldClusterGroupInfo(user, groupInfoArray):
    """
    get group info of old cluster
    """
    g_logger.log("begin get old cluster group info...")
    try:
        #get old cluster group info file
        tmpDir = DefaultValue.getTmpDirAppendMppdb(user)
        groupInfoFile = "%s/groupinfo_out" % tmpDir
        if(not os.path.isfile(groupInfoFile)):
            raise Exception("the group info file of old cluster does not exists!")
            
        #pick up group info
        fp = open(groupInfoFile, 'r')
        res = fp.readlines()
        fp.close()
        if(len(res) != 1):
            raise Exception("the group number in current cluster is %d, excepted is 1." % len(res))    
        groupInfoArray.extend((res[0].strip()).split('\t'))
        if(len(groupInfoArray) != 4):
            raise Exception("the group info field number is %d, excepted is 4." % len(groupInfoArray))
            
        #clean tmp file
        if(os.path.isfile(groupInfoFile)):
            os.remove(groupInfoFile)

        g_logger.log("get old cluster group info finished.")
    except Exception, e:
        g_logger.log("get old cluster group info failed: %s" % str(e))
        raise Exception("get old cluster group info failed: %s" % str(e))

def getNewClusterGroupInfo(user, clusterInfo, groupInfoArray):
    """
    get group info of cluster
    """
    g_logger.log("begin get new cluster group info...")
    tmpDir = DefaultValue.getTmpDirAppendMppdb(user)
    groupInfoFile = "%s/groupinfo_out" % tmpDir
    if(os.path.isfile(groupInfoFile)):
        os.remove(groupInfoFile)

    sqlFile = "%s/upgrade_groupinfo.sql" % tmpDir
    if(os.path.isfile(sqlFile)):
        os.remove(sqlFile)
    
    try:
        #check if there is a cn on this node
        currentNode = clusterInfo.getDbNodeByName(g_hostname)
        if(currentNode == None):
            raise Exception("there is no node named %s in current cluster" % g_hostname)
        if(len(currentNode.coordinators) != 1):
            raise Exception("the coordinator number on current node is %d, excepted is 1." % len(currentNode.coordinators))

        #get cn port
        cnPort = currentNode.coordinators[0].port
            
        #build sql statement
        sql = "COPY pgxc_group TO '%s';" % groupInfoFile

        #build sql file
        cmd = "echo \"%s\" > %s 2>/dev/null;chmod 640 %s" % (sql, sqlFile, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("create sql file failed: %s" % output)
            
        #exec sql file
        cmd = "su - %s -c 'gsql -d postgres -p %s -X -f %s'" % (user, cnPort, sqlFile)
        g_logger.debug("copy cluster group info to tmp file cmd: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0 or output.find("ERROR") >= 0):
            raise Exception("copy group info to tmp file failed: %s" % output)
            
        #pick up group info
        fp = open(groupInfoFile, 'r')
        res = fp.readlines()
        fp.close()
        if(len(res) != 1):
            raise Exception("the group number in current cluster is %d, excepted is 1." % len(res))    
        groupInfoArray.extend((res[0].strip()).split('\t'))
        if(len(groupInfoArray) != 4):
            raise Exception("the group info field number is %d, excepted is 4." % len(groupInfoArray))
            
        #clean tmp file
        if(os.path.isfile(groupInfoFile)):
            os.remove(groupInfoFile)
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
            
        g_logger.log("get new cluster group info finished.")  
    except Exception, e:
        g_logger.debug("get new cluster group info failed:%s" % str(e))
        if(os.path.isfile(groupInfoFile)):
            os.remove(groupInfoFile)
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
        raise Exception("get new cluster group info failed:%s" % str(e))

def buildNewClusterGroupInfo():
    """
    build new cluster group info
    """
    g_logger.log("begin build new cluster group info...")
    #convert old cluster bucket string to int array
    oldBucketArray = []
    oldBucketArray.extend(g_oldClusterGroupInfo[1].split(','))
    oldBucketNum = len(oldBucketArray)
    if(oldBucketNum != 1024):
        raise Exception("the bucket number is %d, expected is 1024." % oldBucketNum)
    for i in range(oldBucketNum):
        oldBucketArray[i] = int(oldBucketArray[i])
    
    #compute new cluster bucket array
    newBucketArray = [ 0 for i in range(g_opts.bucketNum)]
    for i in range(oldBucketNum):
        j = i
        while(j < g_opts.bucketNum):
            newBucketArray[j] = oldBucketArray[i]
            j += 1024

    #convert bucket array to string
    newBucketStrArray = [str(newBucketArray[i]) for i in range(g_opts.bucketNum)]

    #build new cluster group info
    #in new cluster, the 4th field is bucket string
    g_newClusterGroupInfo[3] = ','.join(newBucketStrArray)

    g_logger.log("build new cluster group info finished.")

def createCopyFile():
    """
    create copy file for upgrade new cluster group info
    """
    g_logger.log("begin create copy file...")
    #clean copy file if exists
    if(os.path.isfile(group_info_copy_from_file)):
        os.remove(group_info_copy_from_file)
    #create new copy file
    groupInfoStr = '\t'.join(g_newClusterGroupInfo)
    cmd = "echo \"%s\" > %s 2>/dev/null;chmod 640 %s" % (groupInfoStr, group_info_copy_from_file, group_info_copy_from_file)
    g_logger.debug("create copy file cmd:%s" % cmd)
    (status, output) = commands.getstatusoutput(cmd)
    if(status != 0):
        raise Exception("create copy file failed:%s" % output)
    g_logger.log("create copy file finished.")

def deleteCopyFile():
    """
    delete copy file for upgrade new cluster group info 
    """
    #clean copy file if exists
    if(os.path.isfile(group_info_copy_from_file)):
        os.remove(group_info_copy_from_file)

def updateNewClusterGroupInfo():
    """
    update new cluster group info
    """
    g_logger.log("begin update new cluster group info...")
    #get cn port
    currentNode = g_newClusterInfo.getDbNodeByName(g_hostname)
    cnPort = currentNode.coordinators[0].port

    #check and clean tmp sql file
    tmpDir = DefaultValue.getTmpDirAppendMppdb(g_opts.newUser)
    sqlFile = "%s/upgrade_groupinfo.sql" % tmpDir
    if(os.path.isfile(sqlFile)):
        os.remove(sqlFile)
        
    try:
        #build sql statement
        sql = "start transaction;"
        sql += "SET xc_maintenance_mode=on;"
        sql += "drop node group %s;" % g_newClusterGroupInfo[0].strip()
        sql += "COPY PGXC_GROUP FROM '%s';" % group_info_copy_from_file
        sql += "SET xc_maintenance_mode=off;"
        sql += "commit;"

        #create new sql file
        cmd = "echo \"%s\" > %s 2>/dev/null;chmod 640 %s" % (sql, sqlFile, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("create sql file failed: %s" % output)

        #exec sql file
        cmd = "su - %s -c 'gsql -d postgres -p %s -X -f %s'" % (g_opts.newUser, cnPort, sqlFile)
        g_logger.debug("update new cluster group info cmd:%s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0 or output.find("ERROR") >= 0):
            raise Exception("exec sql file failed: %s" % output)

        #delete sql file
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
            
        g_logger.log("update new cluster group info finished.")
    except Exception, e:
        g_logger.debug("update new cluster group info failed:%s" % str(e))
        if(os.path.isfile(sqlFile)):
            os.remove(sqlFile)
        raise Exception("update new cluster group info failed:%s" % str(e))

if __name__ == '__main__':
    """
    main function
    """
    g_opts = CmdOptions()
    parseCommandLine()
    initGlobalInfos()
    
    try:
        g_logger.log("Begin upgrade bucket info in new cluster...")
        checkCNNum()
        getOldClusterGroupInfo(g_opts.oldUser, g_oldClusterGroupInfo)
        getNewClusterGroupInfo(g_opts.newUser, g_newClusterInfo, g_newClusterGroupInfo)
        buildNewClusterGroupInfo()
        createCopyFile()
        updateNewClusterGroupInfo()
        deleteCopyFile()
        g_logger.log("Upgrade bucket info in new cluster successfully!")
        g_logger.closeLog()
        sys.exit(0)
    except Exception, e:
        g_logger.log(str(e))
        deleteCopyFile()
        g_logger.logExit("Upgrade bucket info in new cluster failed!")
    

