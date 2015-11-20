import commands
import os
import sys

sys.path.append(sys.path[0] + "/../../")
from script.util.Common import *

def isNumber(num):
    try:
        ### try to convert num to float. if catch error, it means num is not a number
        float(num)
    except:
        return False
    return True

def isIp(ip):
    try:
        ### only support ipv4...
        import socket
        socket.inet_aton(ip)
    except:
        return False
    return True
    
class statItem():
    def __init__(self, value, unit = None):
        value = value.strip()
        if(isNumber(value)):
            self.value = value
        else:
            self.value = None      
        self.unit = unit
        
    def __str__(self):
        if(self.value == None):
            return ""        
        elif(self.unit != None):
            return "%-10s %s" % (self.value, self.unit)
        else:
            return "%s" % self.value    

class clusterStatistics():
    def __init__(self):
        self.cluster_stat_generate_time                     = None        
        
        ### Host cpu time
        self.cluster_host_total_cpu_time                    = None
        self.cluster_host_cpu_busy_time                     = None
        self.cluster_host_cpu_iowait_time                   = None
        self.cluster_host_cpu_busy_time_perc                = None
        self.cluster_host_cpu_iowait_time_perc              = None
        
        ### MPP cpu time
        self.cluster_mppdb_cpu_time_in_busy_time            = None
        self.cluster_mppdb_cpu_time_in_total_time           = None
        
        ### Shared buffer
        self.cluster_share_buffer_read                      = None
        self.cluster_share_buffer_hit                       = None
        self.cluster_share_buffer_hit_ratio                 = None
        
        ### In memory sort ratio
        self.cluster_in_memory_sort_count                   = None
        self.cluster_disk_sort_count                        = None
        self.cluster_in_memory_sort_ratio                   = None
        
        ### IO statistics
        self.cluster_io_stat_number_of_files                = None
        self.cluster_io_stat_physical_reads                 = None
        self.cluster_io_stat_physical_writes                = None
        self.cluster_io_stat_read_time                      = None
        self.cluster_io_stat_write_time                     = None
        
        ### Disk usage
        self.cluster_disk_usage_db_size                     = None
        self.cluster_disk_usage_tot_physical_writes         = None
        self.cluster_disk_usage_avg_physical_write          = None
        self.cluster_disk_usage_max_physical_write          = None
        
        ### Activity statistics
        self.cluster_activity_active_sql_count              = None
        self.cluster_activity_session_count                 = None
        
class nodeStatistics():
    def __init__(self, nodename):
        self.nodename                               = nodename
        self.node_mppdb_cpu_busy_time               = None
        self.node_host_cpu_busy_time                = None
        self.node_host_cpu_total_time               = None
        self.node_mppdb_cpu_time_in_busy_time       = None
        self.node_mppdb_cpu_time_in_total_time      = None
        self.node_physical_memory                   = None
        self.node_db_memory_usage                   = None
        self.node_shared_buffer_size                = None
        self.node_shared_buffer_hit_ratio           = None
        self.node_in_memory_sorts                   = None
        self.node_in_disk_sorts                     = None
        self.node_in_memory_sort_ratio              = None
        self.node_number_of_files                   = None
        self.node_physical_reads                    = None
        self.node_physical_writes                   = None
        self.node_read_time                         = None
        self.node_write_time                        = None
    
class sessionStatistics():
    def __init__(self, nodename, dbname, username):
        self.nodename                                   = nodename
        self.dbname                                     = dbname
        self.username                                   = username
        self.session_cpu_time                           = None
        self.session_db_cpu_time                        = None
        self.session_cpu_percent                        = None
        
        self.session_buffer_reads                       = None
        self.session_buffer_hit_ratio                   = None
        self.session_in_memory_sorts                    = None
        self.session_in_disk_sorts                      = None
        self.session_in_memory_sorts_ratio              = None
        self.session_total_memory_size                  = None
        self.session_used_memory_size                   = None
        
        self.session_physical_reads                     = None
        self.session_read_time                          = None


    
class GaussStat():
    
    def __init__(self, installPath = "", user = "", localport = "", logger = None, showDetail = False, database = "postgres"):           
        ### gsql paramter, must be set
        if(installPath == "" or user == "" or localport == "" or logger == None):
            raise Exception("indispensable paramter missed.")
        else:
            self.installPath = installPath            
            self.user = user
            self.localport = localport
            self.logger = logger
        
        ### show detail or not
        self.showDetail = showDetail
        
        ### which database we should connect.
        self.database = database
        
        ###initialize statistics 
        self.cluster_stat = clusterStatistics()
        self.node_stat = []
        self.session_cpu_stat = []
        self.session_mem_stat = []
        self.session_io_stat  = []
        
        ### internal parameter
        self.__baselineFlag = "gauss_stat_output_time" ##default baseline check flag.
        self.__TopNSessions = 10
            
    def writeOutput(self, str):
        sys.stderr.write(str + "\n")
        sys.stderr.flush() 
        
     
    ### NOTICE: itemCounts must be more than two. so that we can distinguish records and (%d rows)
    def execQueryCommand(self, sql, itemCounts, baselineflag = ""):        
        if(baselineflag == ""):
            baselineflag = self.__baselineFlag

        #save sql statement to file to reduce quot nesting
        sqlFile = os.path.join(DefaultValue.getTmpDirFromEnv(), "gaussdb_query_%s.sql" % os.getpid())
        cmd = "echo \"%s\" > %s && chown %s %s" % (sql, sqlFile, self.user, sqlFile)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            raise Exception("Save sql statement to file failed!Error:%s" % output)        
        
        try:
            if(os.getgid() == 0):
                cmd = "su - %s -c \'%s -U %s -h 127.0.0.1 -p %s -d %s -f %s -X\' 2>/dev/null" % (self.user, os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database, sqlFile)
            else:
                cmd = "%s -U %s -h 127.0.0.1 -p %s -d %s -f %s -X 2>/dev/null" % (os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database, sqlFile)
            self.logger.debug("execute command: %s" % (cmd))
            
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                raise Exception("execute query command error: %s" % (output))

            PlatformCommand.cleanTmpFile(sqlFile)
            
            baseline = self.checkExpectedOutput(output, baselineflag, False)
            
            if(baseline == -1):
                raise Exception("can't not fetch query baseline: %s" % (output))
            
            if(self.checkExpectedOutput(output, "(0 rows)", True, baseline) != -1):
                ### can not support now
                return None
                
            lines = output.split("\n")
            linesCount = len(lines)
            
            ### result must more than 4 lines
            if(linesCount <= baseline + 4):
                raise Exception("Unexpected lines returned: %s" % (output))
            
            records = []
            for ino in range(baseline + 2, linesCount):
                line = lines[ino]
                record = line.split("|")
                if(len(record) != itemCounts):
                    break
                records.append(record)
                
            self.logger.debug("execQueryCommand successed.")
            self.logger.debug("Query results: %s." % str(records))      
            return records
        except Exception, e:
            ### execute query command failed. log and raise
            self.logger.debug("execQueryCommand(%s) on local host failed." % sql)
            PlatformCommand.cleanTmpFile(sqlFile)
            raise Exception("execQueryCommand(%s) on local host failed." % sql)
    
    ## check if the expected line existed in output.
    def checkExpectedOutput(self, output, expect, strict = True, starter = 0):
        lines = output.split("\n")
        expect = expect.strip()
        if(starter < len(lines)):
            for i in range(starter, len(lines)):
                line = lines[i]
                if(strict):
                    if(expect == line.strip()):
                        return i - starter
                else:
                    if(line.strip().find(expect) != -1):
                        return i - starter   
        return -1
    
    def installPMKSchema(self):
        try:
            self.logger.debug("begin install pmk schema...")
            ### test coordinator and coordinator mode...
            needCheckWithUid = False
            cmd = "ps -eo user,cmd|awk '{if($1 == \"%s\" && $2 == \"%s\")print $0}'" % (self.user, os.path.join(self.installPath,"bin/gaussdb"))
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0 or output.strip() == ""):
                self.logger.debug("test coordinator and coordinator mode with user name failed: %s" % (output))
                self.logger.debug("try to do it again with user id.")
                needCheckWithUid = True

            if(needCheckWithUid == True):
                cmd = "grep ^%s: /etc/passwd | tail -n 1" % self.user
                (status, output) = commands.getstatusoutput(cmd)
                if(status == 0):
                    nameString = output.split(':')
                    user_id = nameString[2].strip()
                else:
                    raise Exception("get user id failed: %s." % output)
                    
                cmd = "ps -eo user,cmd|awk '{if($1 == \"%s\" && $2 == \"%s\")print $0}'" % (user_id, os.path.join(self.installPath,"bin/gaussdb"))
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    self.logger.debug("test coordinator and coordinator mode with user id failed: %s." % (output))
                    raise Exception("test coordinator and coordinator mode with user id failed: %s." % (output))
            
            self.logger.debug("test coordinator output:\n%s" % output)
            if(self.checkExpectedOutput(output, "--restoremode", False) != -1):
                self.logger.debug("coordinator is running in restore mode.")
                raise Exception("coordinator is running in restore mode.")
            elif(self.checkExpectedOutput(output, "--coordinator", False) != -1):
                self.logger.debug("coordinator is running in normal mode.")
                pass
            else:
                self.logger.debug("there is no running coordinator instance in this node.")
                raise Exception("there is no running coordinator instance in this node.")

            if(os.getgid() == 0):
                cmd = "su - %s -c \'%s -U %s -h 127.0.0.1 -p %s -d %s -X -f " % (self.user, os.path.join(self.installPath,"bin/gsql"), 
                                                        self.user, str(self.localport), self.database)
                cmd += "%s" % (os.path.join(self.installPath,"bin/script/local/test_data_node.sql")) 
                cmd += "\'"
            else:
                cmd = "%s -U %s -h 127.0.0.1 -p %s -d %s -X -f " % (os.path.join(self.installPath,"bin/gsql"), 
                                                        self.user, str(self.localport), self.database)
                cmd += "%s" % (os.path.join(self.installPath,"bin/script/local/test_data_node.sql")) 
            
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0):
                self.logger.debug("query pgxc node failed output %s." % (output))
                raise Exception("query pgxc node failed output %s." % (output))
            
            lines = output.split("\n")
            if(len(lines) < 4 or self.checkExpectedOutput(output, "(0 rows)") >= 2):
                self.logger.debug("There is no datanode in cluster.")
                raise Exception("There is no datanode in cluster.")
            
            ### test pmk schema exist or not.
            if(os.getgid() == 0):
                cmd = "su - %s -c \'%s -U %s -h 127.0.0.1 -p %s -d %s -X -f " % (self.user, os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
                cmd += "%s" % (os.path.join(self.installPath,"bin/script/local/test_pmk.sql"))
                cmd += "\'" 
            else:
                cmd = "%s -U %s -h 127.0.0.1 -p %s -d %s -X -f " % (os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
                cmd += "%s" % (os.path.join(self.installPath,"bin/script/local/test_pmk.sql"))
            
            (status, output) = commands.getstatusoutput(cmd)
            self.logger.debug("test pmk schema output: %s" % (output))
            if(self.checkExpectedOutput(output, "ERROR:  query returned no rows", False) != -1):
                ### schema not exist, so we create it.
                self.logger.debug("pmk schema not exist. install it for the first time.")
                pass
            elif(self.checkExpectedOutput(output, "pmk schema exist. class count is 14, proc count is 30", False) != -1):
                ### schema already created.
                self.logger.debug("pmk schema already exist.")
                return
            else:
                ### maybe class count or proc count not the same.
                self.logger.debug("pmk schema not complete! try to execute \"drop schema pmk cascade;\".")
                if(os.getgid() == 0):
                    cmd = "su - %s -c 'gsql -d %s -p %s -h 127.0.0.1 -X -c \"drop schema pmk cascade;\"'" % (self.user,
                                                  self.database, str(self.localport))
                else:
                    cmd = "gsql -d %s -p %s -h 127.0.0.1 -X -c \"drop schema pmk cascade;\"" % (self.database, str(self.localport))
                (status, output) = commands.getstatusoutput(cmd)
                if(status != 0):
                    self.logger.debug("Drop schema pmk failed: %s" % output)
                    raise Exception("Drop schema pmk failed: %s" % output)
                else:
                    self.logger.debug("Drop schema pmk success: %s" % output)
            
            ### add pmk schema to database.
            if(os.getgid() == 0):
                cmd = "su - %s -c \'%s -U %s -h 127.0.0.1 -p %s -d %s -X -f \"" % (self.user, os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
                cmd += "%s" % (os.path.join(self.installPath,"share/postgresql/pmk_schema.sql"))
                cmd += "\"\'"
            else:
                cmd = "%s -U %s -h 127.0.0.1 -p %s -d %s -X -f \"" % (os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
                cmd += "%s" % (os.path.join(self.installPath,"share/postgresql/pmk_schema.sql"))
                cmd += "\""
            
            (status, output) = commands.getstatusoutput(cmd)
            if(status != 0 or self.checkExpectedOutput(output, "COMMIT", False) == -1):
                raise Exception("execute add pmk schema command error: %s" % (output))
            
            ### create schema success.
            self.logger.debug("install PMKSchema finished.")
            return
        except Exception,e:
            raise Exception("%s" % str(e))
            
        raise Exception("Failed without available coordinator.")
    
    def collect(self):
        try:
            self.logger.debug("Start Collect performance statistics...")
            
            self.installPMKSchema()
            self.loadStat()
            self.collectClusterHostCpuStat()
            self.collectClusterMPPDBCpuStat()
            self.collectShareBufferStat()
            self.collectClusterSortStat()
            self.collectClusterIOStat()
            self.collectClusterDiskStat()
            self.collectClusterActiveSqlCount()
            self.collectClusterSessionCount()
            
            self.collectNodeCpuStat()
            self.collectNodeMemoryStat()
            self.collectNodeIOStat()
            
            self.collectSessionCpuStat()
            self.collectSessionMemoryStat()
            self.collectSessionIOStat()
            
            self.logger.debug("Collect performance statistics successed.")        
                
        except Exception, e:
            raise Exception("Query GaussDB statistics failed. Error: %s" % str(e))
        
    def outPut(self):
        try:
            if(self.showDetail == True):
                self.detailDisplay()
            else:
                self.summaryDisplay()
        except Exception, e:
            raise Exception("output statistics failed. Error: %s" % str(e))
        
    def loadStat(self):

        if(os.getgid() == 0):
            cmd = "su - %s -c \'%s -U %s -h 127.0.0.1 -p %s -d %s -X -c \"" % (self.user, os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
            cmd += "select * from pmk.collect_stat();" + "\"\'"
        else:
            cmd = "%s -U %s -h 127.0.0.1 -p %s -d %s -X -c \"" % (os.path.join(self.installPath,"bin/gsql"), 
                                                    self.user, str(self.localport), self.database) 
            cmd += "select * from pmk.collect_stat();" + "\""
        
        self.logger.debug("begin to load statistics...")
        
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("load statistics query execute error: %s" % (output))
        
        if(self.checkExpectedOutput(output, "t") != -1):
            ### normal case
            self.logger.debug("Collect statistics success.")
        elif(self.checkExpectedOutput(output, "Collect statistics is executed by another session.", False) != -1):
            ### execute perfcheck concurrently
            self.logger.debug("Collect statistics is executed by another session.")
        else:
            raise Exception("load statistics failed with error result: %s" % output)   
        
    def collectClusterHostCpuStat(self):        
        sql = "select o_stat_collect_time as %s, " % (self.__baselineFlag)
        sql += "o_avg_cpu_total_time, o_avg_cpu_busy_time, o_avg_cpu_iowait_time, o_cpu_busy_perc, o_cpu_io_wait_perc "
        sql += "from pmk.get_cluster_host_cpu_stat(null, null);"
        
        self.logger.debug("Start collect cluster host cpu statistics...")
        try:
            records = self.execQueryCommand(sql, 6)
        except Exception, e:
            raise Exception("Collect Cluster Host CPU statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Host CPU statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_host_total_cpu_time           = statItem(items[1], "Jiffies")
            self.cluster_stat.cluster_host_cpu_busy_time            = statItem(items[2], "Jiffies")
            self.cluster_stat.cluster_host_cpu_iowait_time          = statItem(items[3], "Jiffies")
            self.cluster_stat.cluster_host_cpu_busy_time_perc       = statItem(items[4], "%")
            self.cluster_stat.cluster_host_cpu_iowait_time_perc     = statItem(items[5], "%")
        self.logger.debug("collect ClusterHostCpuStat finished.")

    def collectClusterMPPDBCpuStat(self):        
        sql = "select o_stat_collect_time as %s, o_mppdb_cpu_time_perc_wrt_busy_time, o_mppdb_cpu_time_perc_wrt_total_time from " % (self.__baselineFlag)
        sql += "pmk.get_cluster_mppdb_cpu_stat(null, null);"
        
        self.logger.debug("Start collect mppdb cpu statistics...")
        try:  
            records = self.execQueryCommand(sql, 3)
        except Exception, e:
            raise Exception("Collect Cluster MPPDB CPU statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster MPPDB CPU statistics with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time       = statItem(items[1], "%")
            self.cluster_stat.cluster_mppdb_cpu_time_in_total_time      = statItem(items[2], "%")
        self.logger.debug("collect ClusterMPPDBCpuStat finished.")
           
    def collectShareBufferStat(self):
        sql = "select o_stat_collect_time as %s, o_total_blocks_read, o_total_blocks_hit, o_shared_buffer_hit_ratio " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_shared_buffer_stat(null, null);"
            
        self.logger.debug("Start collect shared buffer statistics...")
        try:  
            records = self.execQueryCommand(sql, 4)
        except Exception, e:
            raise Exception("Collect Cluster Shared buffer statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Shared buffer statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_share_buffer_read                  = statItem(items[1])   
            self.cluster_stat.cluster_share_buffer_hit                   = statItem(items[2]) 
            self.cluster_stat.cluster_share_buffer_hit_ratio             = statItem(items[3], "%")  
        self.logger.debug("collect ShareBufferStat finished.")
  
    def collectClusterSortStat(self):
        sql = "select o_stat_collect_time as %s, o_total_memory_sorts, o_total_disk_sorts, o_memory_sort_ratio " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_memory_sort_stat(null, null);"
        
        self.logger.debug("Start collect sort statistics...")
        try:  
            records = self.execQueryCommand(sql, 4)
        except Exception, e:
            raise Exception("Collect Cluster Sort statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Sort statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_in_memory_sort_count      = statItem(items[1])
            self.cluster_stat.cluster_disk_sort_count           = statItem(items[2])
            self.cluster_stat.cluster_in_memory_sort_ratio      = statItem(items[3], "%")
        self.logger.debug("collect ClusterSortStat finished.")
        
    def collectClusterIOStat(self):
        sql = "select o_stat_collect_time as %s, o_number_of_files, o_physical_reads, o_physical_writes, o_read_time, o_write_time " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_io_stat(null, null);"               
        
        self.logger.debug("Start collect io statistics...")
        try:
            records = self.execQueryCommand(sql, 6)
        except Exception, e:
            raise Exception("Collect Cluster IO statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster IO statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_io_stat_number_of_files    = statItem(items[1])
            self.cluster_stat.cluster_io_stat_physical_reads     = statItem(items[2])
            self.cluster_stat.cluster_io_stat_physical_writes    = statItem(items[3])
            self.cluster_stat.cluster_io_stat_read_time          = statItem(items[4], "ms")
            self.cluster_stat.cluster_io_stat_write_time         = statItem(items[5], "ms")
        self.logger.debug("collect ClusterIOStat finished.")
        
    def collectClusterDiskStat(self):
        sql = "select o_stat_collect_time as %s, o_tot_datanode_db_size, o_tot_physical_writes, o_avg_write_per_sec, o_max_node_physical_writes " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_disk_usage_stat(null, null);"
           
        self.logger.debug("Start collect disk usage statistics...") 
        try:  
            records = self.execQueryCommand(sql, 5)
        except Exception, e:
            raise Exception("Collect Cluster Disk usage statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Disk usage statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_disk_usage_db_size             = statItem(items[1].split()[0], items[1].split()[1])
            self.cluster_stat.cluster_disk_usage_tot_physical_writes = statItem(items[2])
            self.cluster_stat.cluster_disk_usage_avg_physical_write  = statItem(items[3])
            self.cluster_stat.cluster_disk_usage_max_physical_write  = statItem(items[4])
        self.logger.debug("collect ClusterDiskStat finished.") 
        
    def collectClusterActiveSqlCount(self):
        sql = "select o_stat_collect_time as %s, o_tot_active_sql_count " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_active_sql_count(null, null);" 
            
        self.logger.debug("Start collect active sql statistics...")
        try:  
            records = self.execQueryCommand(sql, 2)
        except Exception, e:
            raise Exception("Collect Cluster Active sql count statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Active sql count statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_activity_active_sql_count          = statItem(items[1])
        self.logger.debug("collect ClusterActiveSqlCount finished.")
            
    def collectClusterSessionCount(self):
        sql = "select o_stat_collect_time as %s, o_tot_session_count " % (self.__baselineFlag)
        sql += "from pmk.get_cluster_session_count(null, null);"
            
        self.logger.debug("Start collect session count statistics...")
        try:  
            records = self.execQueryCommand(sql, 2)
        except Exception, e:
            raise Exception("Collect Cluster Session count statistics failed! Error: %s" % str(e))
        
        if(len(records) != 1):
            raise Exception("Collect Cluster Session count statistics failed with wrong records number: %d" % len(records))
        items = records[0]
        if (items != None):
            self.cluster_stat.cluster_activity_session_count             = statItem(items[1])
        self.logger.debug("collect ClusterSessionCount finished.")
            
    def collectNodeCpuStat(self):
        sql = "select o_stat_collect_time as %s, o_node_name, " % (self.__baselineFlag)
        sql += "o_mppdb_cpu_time, o_host_cpu_busy_time, o_host_cpu_total_time, o_mppdb_cpu_time_perc_wrt_busy_time, "
        sql += "o_mppdb_cpu_time_perc_wrt_total_time from pmk.get_node_cpu_stat('all', null, null);"
        
        self.logger.debug("Start collect Node cpu statistics...")
        try:
            records = self.execQueryCommand(sql, 7)
        except Exception, e:
            raise Exception("Collect Node Cpu statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Node Cpu statistics failed! Error: No records returned")
        
        for i in range(0, recordsCount):
            record = records[i]
            
            found = False
            for node in self.node_stat:
                if(node.nodename == record[1].strip()):
                    found = True
                    break
            
            if(not found):
                node = nodeStatistics(record[1].strip())
                self.node_stat.append(node)
            
            node.node_mppdb_cpu_busy_time               = statItem(record[2], "Jiffies")
            node.node_host_cpu_busy_time                = statItem(record[3], "Jiffies")
            node.node_host_cpu_total_time               = statItem(record[4], "Jiffies")
            node.node_mppdb_cpu_time_in_busy_time       = statItem(record[5], "%")
            node.node_mppdb_cpu_time_in_total_time      = statItem(record[6], "%")
        self.logger.debug("collect NodeCpuStat finished.")
            
    def collectNodeMemoryStat(self):
        sql = "select o_stat_collect_time as %s, o_node_name, " % (self.__baselineFlag)
        sql += "o_physical_memory, o_shared_buffer_size, o_shared_buffer_hit_ratio, o_sorts_in_memory, "
        sql += "o_sorts_in_disk, o_in_memory_sort_ratio, o_db_memory_usage from pmk.get_node_memory_stat('all', null, null);"
        
        self.logger.debug("Start collect Node memory statistics...")
        try:
            records = self.execQueryCommand(sql, 9)
        except Exception, e:
            raise Exception("Collect Node memory statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Node memory statistics failed! Error: No records returned")
        
        for i in range(0, recordsCount):
            record = records[i]
            
            found = False
            for node in self.node_stat:
                if(node.nodename == record[1].strip()):
                    found = True
                    break
               
            if(not found):
                node = nodeStatistics(record[1].strip())
                self.node_stat.append(node)
            
            node.node_physical_memory               = statItem(record[2], "Bytes")
            node.node_db_memory_usage               = statItem(record[8], "Bytes")
            node.node_shared_buffer_size            = statItem(record[3], "Bytes")
            node.node_shared_buffer_hit_ratio       = statItem(record[4], "%")
            node.node_in_memory_sorts               = statItem(record[5],)
            node.node_in_disk_sorts                 = statItem(record[6],)
            node.node_in_memory_sort_ratio          = statItem(record[7], "%")  
        self.logger.debug("collect NodeMemoryStat finished.")
 
    def collectNodeIOStat(self):
        sql = "select o_stat_collect_time as %s, o_node_name, " % (self.__baselineFlag)
        sql += "o_number_of_files, o_physical_reads, o_physical_writes, o_read_time, "
        sql += "o_write_time from pmk.get_node_io_stat('all', null, null);"
        
        self.logger.debug("Start collect Node IO statistics...")
        try:  
            records = self.execQueryCommand(sql, 7)
        except Exception, e:
            raise Exception("Collect Node IO statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Node IO statistics failed! Error: No records returned")
        
        for i in range(0, recordsCount):
            record = records[i]
            
            found = False
            for node in self.node_stat:
                if(node.nodename == record[1].strip()):
                    found = True
                    break
               
            if(not found):
                node = nodeStatistics(record[1].strip())
                self.node_stat.append(node)
            
            node.node_number_of_files               = statItem(record[2])
            node.node_physical_reads                = statItem(record[3])
            node.node_physical_writes               = statItem(record[4])
            node.node_read_time                     = statItem(record[5])
            node.node_write_time                    = statItem(record[6])
        self.logger.debug("collect NodeIOStat finished.")
            
    def collectSessionCpuStat(self):
        sql = "select o_session_start_time as %s, o_node_name, o_db_name, o_user_name, " % (self.__baselineFlag)
        sql += "o_session_cpu_time, o_mppdb_cpu_time, o_mppdb_cpu_time_perc "
        sql += "from pmk.get_session_cpu_stat(null, %d);" % self.__TopNSessions
        
        self.logger.debug("Start collect Session Cpu statistics...")
        try:  
            records = self.execQueryCommand(sql, 7)
        except Exception, e:
            raise Exception("Collect Session Cpu statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Session Cpu statistics failed! Error: No records returned")
        
        for i in range(0, recordsCount):
            record = records[i]
            
            sess = sessionStatistics(record[1].strip(), record[2].strip(), record[3].strip())            
            sess.session_cpu_time                          = statItem(record[4])
            sess.session_db_cpu_time                       = statItem(record[5])
            sess.session_cpu_percent                       = statItem(record[6], "%")
            
            self.session_cpu_stat.append(sess)
        self.logger.debug("collect SessionCpuStat finished.")    
            
    def collectSessionMemoryStat(self):
        sql = "select o_session_start_time as %s, o_node_name, o_db_name, o_user_name, " % (self.__baselineFlag)
        sql += "o_buffer_hits, o_session_buffer_hit_ratio, o_sorts_in_memory, "
        sql += "o_sorts_in_disk, o_session_memory_sort_ratio, "
        sql += "o_session_total_memory_size, o_session_used_memory_size from pmk.get_session_memory_stat(null, %d);" % self.__TopNSessions
        
        self.logger.debug("Start collect Session Memory statistics...")
        try:
            records = self.execQueryCommand(sql, 11)
        except Exception, e:
            raise Exception("Collect Session Memory statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Session Memory statistics failed! Error: No records returned")
            
        for i in range(0, recordsCount):
            record = records[i]
            
            sess = sessionStatistics(record[1].strip(), record[2].strip(), record[3].strip())            
            sess.session_buffer_reads                       = statItem(record[4])
            sess.session_buffer_hit_ratio                   = statItem(record[5])
            sess.session_in_memory_sorts                    = statItem(record[6])
            sess.session_in_disk_sorts                      = statItem(record[7])
            sess.session_in_memory_sorts_ratio              = statItem(record[8]) 
            sess.session_total_memory_size                  = statItem(record[9])
            sess.session_used_memory_size                   = statItem(record[10])
            
            self.session_mem_stat.append(sess)
        self.logger.debug("collect SessionMemoryStat finished.")    
            
    def collectSessionIOStat(self):
        sql = "select o_session_start_time as %s, o_node_name, o_db_name, o_user_name, " % (self.__baselineFlag)
        sql += "o_disk_reads, o_read_time "
        sql += "from pmk.get_session_io_stat(null, %d);" % self.__TopNSessions
        
        self.logger.debug("Start collect Session IO statistics...")
        try:
            records = self.execQueryCommand(sql, 6)
        except Exception, e:
            raise Exception("Collect Session IO statistics failed! Error: %s" % str(e))
        
        recordsCount = len(records)
        if(recordsCount == 0):
            raise Exception("Collect Session IO statistics failed! Error: No records returned")
            
        for i in range(0, recordsCount):
            record = records[i]
            
            sess = sessionStatistics(record[1].strip(), record[2].strip(), record[3].strip())            
            sess.session_physical_reads                     = statItem(record[4])
            sess.session_read_time                          = statItem(record[5]) 
            
            self.session_io_stat.append(sess)
        self.logger.debug("collect SessionIOStat finished.")    
        
    def displayOneStatItem(self, desc, value):
        if(value != None):
            self.writeOutput("    %-45s:    %s" % (desc, str(value)))
        else:
            self.writeOutput("    %-45s:    Not Available" % (desc))
            
    def summaryDisplay(self):
        self.writeOutput("Cluster statistics information:")
        self.displayOneStatItem("Host CPU busy time ratio",         self.cluster_stat.cluster_host_cpu_busy_time_perc)
        self.displayOneStatItem("MPPDB CPU time % in busy time",    self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time)
        self.displayOneStatItem("Shared Buffer Hit ratio",          self.cluster_stat.cluster_share_buffer_hit_ratio)
        self.displayOneStatItem("In-memory sort ratio",             self.cluster_stat.cluster_in_memory_sort_ratio)
        self.displayOneStatItem("Physical Reads",                   self.cluster_stat.cluster_io_stat_physical_reads)
        self.displayOneStatItem("Physical Writes",                  self.cluster_stat.cluster_io_stat_physical_writes)
        self.displayOneStatItem("DB size",                          self.cluster_stat.cluster_disk_usage_db_size)
        self.displayOneStatItem("Total Physical writes",            self.cluster_stat.cluster_disk_usage_tot_physical_writes)
        self.displayOneStatItem("Active SQL count",                 self.cluster_stat.cluster_activity_active_sql_count)
        self.displayOneStatItem("Session count",                    self.cluster_stat.cluster_activity_session_count)
     
    def detailDisplay(self):
        self.writeOutput("Cluster statistics information:")
        self.writeOutput("Host CPU usage rate:")
        self.displayOneStatItem("Host total CPU time",          self.cluster_stat.cluster_host_total_cpu_time)
        self.displayOneStatItem("Host CPU busy time",           self.cluster_stat.cluster_host_cpu_busy_time)
        self.displayOneStatItem("Host CPU iowait time",         self.cluster_stat.cluster_host_cpu_iowait_time)
        self.displayOneStatItem("Host CPU busy time ratio",     self.cluster_stat.cluster_host_cpu_busy_time_perc)
        self.displayOneStatItem("Host CPU iowait time ratio",   self.cluster_stat.cluster_host_cpu_iowait_time_perc)
        
        self.writeOutput("MPPDB CPU usage rate:")
        self.displayOneStatItem("MPPDB CPU time % in busy time",      self.cluster_stat.cluster_mppdb_cpu_time_in_busy_time)
        self.displayOneStatItem("MPPDB CPU time % in total time",     self.cluster_stat.cluster_mppdb_cpu_time_in_total_time)
        
        self.writeOutput("Shared buffer hit rate:")
        self.displayOneStatItem("Shared Buffer Reads",       self.cluster_stat.cluster_share_buffer_read)
        self.displayOneStatItem("Shared Buffer Hits",        self.cluster_stat.cluster_share_buffer_hit)
        self.displayOneStatItem("Shared Buffer Hit ratio",   self.cluster_stat.cluster_share_buffer_hit_ratio)
        
        self.writeOutput("In memory sort rate:")
        self.displayOneStatItem("In-memory sort count",      self.cluster_stat.cluster_in_memory_sort_count)
        self.displayOneStatItem("In-disk sort count",        self.cluster_stat.cluster_disk_sort_count)
        self.displayOneStatItem("In-memory sort ratio",      self.cluster_stat.cluster_in_memory_sort_ratio)
        
        self.writeOutput("I/O usage:")
        self.displayOneStatItem("Number of files",           self.cluster_stat.cluster_io_stat_number_of_files)
        self.displayOneStatItem("Physical Reads",            self.cluster_stat.cluster_io_stat_physical_reads)
        self.displayOneStatItem("Physical Writes",           self.cluster_stat.cluster_io_stat_physical_writes)
        self.displayOneStatItem("Read Time",                 self.cluster_stat.cluster_io_stat_read_time)
        self.displayOneStatItem("Write Time",                self.cluster_stat.cluster_io_stat_write_time)
        
        self.writeOutput("Disk usage:")
        self.displayOneStatItem("DB size",                   self.cluster_stat.cluster_disk_usage_db_size)
        self.displayOneStatItem("Total Physical writes",     self.cluster_stat.cluster_disk_usage_tot_physical_writes)
        self.displayOneStatItem("Average Physical write",    self.cluster_stat.cluster_disk_usage_avg_physical_write)
        self.displayOneStatItem("Maximum Physical write",    self.cluster_stat.cluster_disk_usage_max_physical_write)
        
        self.writeOutput("Activity statistics:")
        self.displayOneStatItem("Active SQL count",          self.cluster_stat.cluster_activity_active_sql_count)
        self.displayOneStatItem("Session count",             self.cluster_stat.cluster_activity_session_count)
        
        self.writeOutput("Node statistics information:")
        for node in self.node_stat:
            self.writeOutput("%s:" % node.nodename)
            self.displayOneStatItem("MPPDB CPU Time",                   node.node_mppdb_cpu_busy_time)
            self.displayOneStatItem("Host CPU Busy Time",               node.node_host_cpu_busy_time)
            self.displayOneStatItem("Host CPU Total Time",              node.node_host_cpu_total_time )
            self.displayOneStatItem("MPPDB CPU Time % in Busy Time",    node.node_mppdb_cpu_time_in_busy_time)
            self.displayOneStatItem("MPPDB CPU Time % in Total Time",   node.node_mppdb_cpu_time_in_total_time)
            
            self.displayOneStatItem("Physical memory",                  node.node_physical_memory)
            self.displayOneStatItem("DB Memory usage",                  node.node_db_memory_usage)
            self.displayOneStatItem("Shared buffer size",               node.node_shared_buffer_size)
            self.displayOneStatItem("Shared buffer hit ratio",          node.node_shared_buffer_hit_ratio)
            self.displayOneStatItem("Sorts in memory",                  node.node_in_memory_sorts)
            self.displayOneStatItem("Sorts in disk",                    node.node_in_disk_sorts)
            self.displayOneStatItem("In-memory sort ratio",             node.node_in_memory_sort_ratio)
            
            self.displayOneStatItem("Number of files",                  node.node_number_of_files)
            self.displayOneStatItem("Physical Reads",                   node.node_physical_reads)
            self.displayOneStatItem("Physical Writes",                  node.node_physical_writes)
            self.displayOneStatItem("Read Time",                        node.node_read_time)
            self.displayOneStatItem("Write Time",                       node.node_write_time)
        
        self.writeOutput("Session statistics information(Top %d):" % self.__TopNSessions)
        self.writeOutput("Session CPU statistics:")
        for i in range(0, len(self.session_cpu_stat)):
            sess = self.session_cpu_stat[i]
            self.writeOutput("%d %s-%s-%s:" % (i+1, sess.nodename, sess.dbname, sess.username))
            self.displayOneStatItem("Session CPU time",                  sess.session_cpu_time)
            self.displayOneStatItem("Database CPU time",                 sess.session_db_cpu_time)
            self.displayOneStatItem("Session CPU time %",                sess.session_cpu_percent)
       
        self.writeOutput("\nSession Memory statistics:")
        for i in range(0, len(self.session_mem_stat)):
            sess = self.session_mem_stat[i]
            self.writeOutput("%d %s-%s-%s:" % (i+1, sess.nodename, sess.dbname, sess.username))
            self.displayOneStatItem("Buffer Reads",                         sess.session_buffer_reads)
            self.displayOneStatItem("Shared Buffer Hit ratio",              sess.session_buffer_hit_ratio)
            self.displayOneStatItem("In Memory sorts",                      sess.session_in_memory_sorts)
            self.displayOneStatItem("In Disk sorts",                        sess.session_in_disk_sorts)
            self.displayOneStatItem("In Memory sorts ratio",                sess.session_in_memory_sorts_ratio)
            self.displayOneStatItem("Total Memory Size",                    sess.session_total_memory_size)
            self.displayOneStatItem("Used Memory Size",                     sess.session_used_memory_size)
            
        self.writeOutput("\nSession IO statistics:")   
        for i in range(0, len(self.session_io_stat)):
            sess = self.session_io_stat[i]
            self.writeOutput("%d %s-%s-%s:" % (i+1, sess.nodename, sess.dbname, sess.username))
            self.displayOneStatItem("Physical Reads",                   sess.session_physical_reads)
            self.displayOneStatItem("Read Time",                        sess.session_read_time)
        
if __name__ == '__main__':     
    
    import getopt
    sys.path.append(sys.path[0] + "/../../")
    from script.util.GaussLog import GaussLog
    
    def usage():
        """
Usage:
    python GaussStat.py -p installpath -u user -c ip:port [-f output] [-d] [-l log]
    
options:      
    -p                                install path
    -u                                database user name
    -c                                host information
    -d --detail                       show the detail info about performance check
    -l --logpath=logfile              the log file of operation
    -h --help                         show this help, then exit
        """
        print usage.__doc__ 
    
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "p:u:c:l:dh", ["logpath=", "detail", "help"])
    except Exception, e:
        GaussLog.exitWithError("Parameter input error for GaussStat: %s" % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError("Parameter input error for GaussStat: %s" % str(args[0]))
         
    installPath = ""
    user = ""
    logFile = ""
    localPort = []
    detail = False
         
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-p"):
            installPath = value.strip()
        elif (key == "-u"):
            user = value.strip()
        elif (key == "-c"):
            localPort = value.strip()
        elif (key == "-l" or key == "--logpath"):
            logFile = value.strip()
        elif (key == "-d" or key == "--detail"):
            detail = True
        else:
            GaussLog.exitWithError("Unknown parameter for GaussStat: %s" % key)
    
    if(not os.path.exists(installPath) or user == "" or localPort == ""):
        usage()
        GaussLog.exitWithError("indispensable paramter missed.")
        
    if(logFile == ""):
        logFile = "%s/om/gaussdb_local.log" % DefaultValue.getUserLogDirWithUser(user)
        
    logger = GaussLog(logFile, "GaussStat")
        
    try:  
        stat = GaussStat(installPath, user, localPort, logger, detail)
        stat.collect()
        stat.outPut()
    except Exception, e:
        logger.logExit("Can't get statistics, reason: %s" % str(e))
        
    logger.closeLog()
