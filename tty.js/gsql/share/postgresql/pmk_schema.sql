-- It starts a transaction during the PMK installationSTART TRANSACTION;
/*
 * This script is used to create a repository schema for PMK	
*/

CREATE SCHEMA pmk;

-- PMK Configuration table
CREATE TABLE pmk.pmk_configuration
(
	config_param_name               varchar(64)     NOT NULL
,	config_value                    text       		NOT NULL
,	PRIMARY KEY                     (config_param_name)
);
-- if config_value = -1, it is considered as infinite.

INSERT INTO pmk.pmk_configuration
          ( config_param_name, config_value )
VALUES    ( 'Collection Count', '10' )
	 ,	  ( 'Enable PMK', 'TRUE' );


-- Snapshot (statistics collection) information
CREATE TABLE pmk.pmk_snapshot
(
	snapshot_id						serial							-- Snapshot ID (Running number)
,	current_snapshot_time			timestamp with time zone		-- Time at the beginning of the snapshot
,	last_snapshot_time				timestamp with time zone		-- Time at the end of the snapshot; the actual time the snapshot was taken
,	creation_time					timestamp with time zone		-- Time the snapshot was created
,	PRIMARY KEY						(snapshot_id)		
);

ALTER SEQUENCE pmk.pmk_snapshot_snapshot_id_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CYCLE;
CREATE INDEX ix_pmk_snapshot_time ON pmk.pmk_snapshot (current_snapshot_time DESC);


-- Statistics for each node
CREATE TABLE pmk.pmk_snapshot_data_node_stat
(
	snapshot_id						int								-- Snapshot Id 
,   node_id							integer							-- node id from pgxc_node
,	node_name						text							-- node name from pgxc_node
,	node_host						text							-- node host from pgxc_node
,	last_startup_time				timestamp with time zone		-- last restart time of the node before snapshot starts
,	number_of_files            	    integer
,	physical_reads                  bigint
,	physical_reads_delta            bigint
,	physical_writes                 bigint
,	physical_writes_delta           bigint
,	read_time                       bigint
,	read_time_delta                 bigint
,	write_time                      bigint
,	write_time_delta                bigint
,	db_size                         bigint
,   active_sql_count                int
,   wait_sql_count                  int
,   session_count                   int
,   xact_commit                     bigint
,   xact_commit_delta               bigint
,   xact_rollback                   bigint
,   xact_rollback_delta             bigint
,   checkpoints_timed               bigint
,   checkpoints_timed_delta         bigint
,   checkpoints_req                 bigint
,   checkpoints_req_delta           bigint
,   checkpoint_write_time           double precision
,   checkpoint_write_time_delta     double precision
,	physical_memory                 bigint
,   db_memory_usage                 bigint
,	shared_buffer_size              bigint
,   session_memory_total_size       bigint
,   session_memory_used_size        bigint
,	blocks_read                     bigint
,	blocks_read_delta               bigint
,   blocks_hit                      bigint
,   blocks_hit_delta                bigint
,	work_memory_size     			bigint
,   sorts_in_memory                 bigint
,   sorts_in_memory_delta           bigint
,   sorts_in_disk                   bigint
,   sorts_in_disk_delta             bigint
,   busy_time                       numeric
,   busy_time_delta                 numeric
,   idle_time                       numeric
,   idle_time_delta                 numeric
,   iowait_time                     numeric
,   iowait_time_delta               numeric
,   db_cpu_time                     numeric
,   db_cpu_time_delta               numeric
,	PRIMARY KEY  					(snapshot_id, node_id)
);

CREATE INDEX ix_pmk_snapshot_dnode_stat_node_name ON pmk.pmk_snapshot_data_node_stat (UPPER(node_name), snapshot_id);


CREATE TABLE pmk.pmk_snapshot_coordinator_stat
(
	snapshot_id						int								-- Snapshot Id 
,   node_id							integer							-- node id from pgxc_node
,	node_name						text							-- node name from pgxc_node
,	node_host						text							-- node host from pgxc_node
,	last_startup_time				timestamp with time zone		-- last restart time of the node before snapshot starts
,	number_of_files            	    integer
,	physical_reads                  bigint
,	physical_reads_delta            bigint
,	physical_writes                 bigint
,	physical_writes_delta           bigint
,	read_time                       bigint
,	read_time_delta                 bigint
,	write_time                      bigint
,	write_time_delta                bigint
,	db_size                         bigint
,   active_sql_count                int
,   wait_sql_count                  int
,   session_count                   int
,   xact_commit                     bigint
,   xact_commit_delta               bigint
,   xact_rollback                   bigint
,   xact_rollback_delta             bigint
,   checkpoints_timed               bigint
,   checkpoints_timed_delta         bigint
,   checkpoints_req                 bigint
,   checkpoints_req_delta           bigint
,   checkpoint_write_time           double precision
,   checkpoint_write_time_delta     double precision
,	physical_memory                 bigint
,   db_memory_usage                 bigint
,	shared_buffer_size              bigint
,   session_memory_total_size       bigint
,   session_memory_used_size        bigint
,	blocks_read                     bigint
,	blocks_read_delta               bigint
,   blocks_hit                      bigint
,   blocks_hit_delta                bigint
,	work_memory_size     			bigint
,   sorts_in_memory                 bigint
,   sorts_in_memory_delta           bigint
,   sorts_in_disk                   bigint
,   sorts_in_disk_delta             bigint
,   busy_time                       numeric
,   busy_time_delta                 numeric
,   idle_time                       numeric
,   idle_time_delta                 numeric
,   iowait_time                     numeric
,   iowait_time_delta               numeric
,   db_cpu_time                     numeric
,   db_cpu_time_delta               numeric
,	PRIMARY KEY  					(snapshot_id, node_id)	
);

CREATE INDEX ix_pmk_snapshot_coord_stat_node_name ON pmk.pmk_snapshot_coordinator_stat (UPPER(node_name), snapshot_id);


-- Table to maintain PMK meta data
CREATE TABLE pmk.pmk_meta_data
(
	pmk_version                     varchar(128)
,	last_snapshot_id				int
,	last_snapshot_collect_time		timestamp with time zone
,	PRIMARY KEY                     (pmk_version)
);

INSERT INTO pmk.pmk_meta_data
          ( pmk_version, last_snapshot_id, last_snapshot_collect_time )
VALUES    ( 'Gauss MPPDB Tools V100R003C00B520', NULL, NULL );
/*
	This function is used to find the node type ( D for data node and C for coordinator) of the node 
	from which the command is executed and to return the error if the node type is data-node.
	It returns NULL if the node type is coordinator.
	
	It is called in all the interfaces to verify whether the interfaces are invoked from data-node.
*/

CREATE OR REPLACE FUNCTION pmk.check_node_type 
RETURNS TEXT 
AS
$$
DECLARE    l_node_type            CHAR(1);
BEGIN

    SELECT n.node_type
      INTO l_node_type
      FROM pgxc_node n, pg_settings s
     WHERE s.name        = 'pgxc_node_name' 
	   AND n.node_name   = s.setting;

    IF l_node_type = 'D'
	THEN
		RETURN 'ERROR:: PMK commands can not be executed from data node. Please execute it from coordinator.';
	ELSE
		RETURN NULL;
	END IF;

END;
$$
LANGUAGE plpgsql;

/*
	This function is used to check whether the PMK is enabled. 
	It returns the error if the PMK is not enabled.
	It returns NULL if the PMK is enabled.
	
	It is called in all the interfaces to verify whether the PMK is enabled or not.
*/

CREATE OR REPLACE FUNCTION pmk.check_pmk_enabled 
RETURNS TEXT 
AS
$$
DECLARE    l_pmk_enabled_i          TEXT;
BEGIN

    SELECT UPPER(config_value)
      INTO l_pmk_enabled_i
      FROM pmk.pmk_configuration
     WHERE config_param_name        = 'Enable PMK';

    IF l_pmk_enabled_i = 'FALSE'
	THEN
		RETURN 'ERROR:: PMK should be enabled to use the PMK features.';
	ELSE
		RETURN NULL;
	END IF;

END;
$$
LANGUAGE plpgsql;

--	This function is used to find the PMK version
--  If it is executed from a data-node, it throws the appropriate error.

CREATE OR REPLACE FUNCTION pmk.pmk_version ( ) 
RETURNS pmk.pmk_meta_data.pmk_version%TYPE 
AS
$$
DECLARE  l_pmk_version              pmk.pmk_meta_data.pmk_version%TYPE;
		 l_error_message			TEXT;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN 'f';
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN 'f';
	END IF;

	SELECT pmk_version
	  INTO l_pmk_version
	  FROM pmk.pmk_meta_data;

	RETURN l_pmk_version;

END;
$$
LANGUAGE plpgsql;

-- This function is used to configure the PMK configuration parameters
-- -1 indicates all the statistics collection should be retained.
-- Atleast one statistics collection should be retained in the database. 

CREATE OR REPLACE FUNCTION pmk.configure_parameter
                ( IN       i_config_param_name      pmk.pmk_configuration.config_param_name%TYPE
                , IN       i_config_value           pmk.pmk_configuration.config_value%TYPE
                ) 
RETURNS boolean 
AS
$$
DECLARE			l_collect_count_value				INT;
				l_config_value						TEXT;
				l_upper_config_param				TEXT;
				l_error_message						TEXT;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;

	l_upper_config_param	:= UPPER(TRIM(BOTH ' ' FROM i_config_param_name));

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	   AND l_upper_config_param <> 'ENABLE PMK'
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;
	
	IF i_config_param_name IS NULL 
	THEN
		l_error_message := 'ERROR:: Null should not be provided for configuration parameter name.';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;

	IF i_config_value IS NULL
	THEN
		l_error_message := 'ERROR:: Null should not be provided for configuration value.';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;

	IF l_upper_config_param = 'COLLECTION COUNT'
	THEN
		l_collect_count_value := i_config_value::int;

		IF l_collect_count_value < -1 
		THEN
			l_error_message := 'ERROR:: Configuration value "' || i_config_value || '" should not be less than -1.';
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN FALSE;

		ELSIF l_collect_count_value = 0 
		THEN
			l_error_message := 'ERROR:: 0 should not be provided since atleast one collection should be retained.';
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN FALSE;

		ELSE
			l_config_value := l_collect_count_value;
		END IF;

	ELSIF l_upper_config_param = 'ENABLE PMK'
	THEN
		l_config_value := UPPER(TRIM(BOTH ' ' FROM i_config_value));
		
		IF l_config_value NOT IN ('TRUE', 'FALSE')
		THEN
			l_error_message := 'ERROR:: Allowed values are TRUE or FALSE for the configuration parameter "Enable PMK".';
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN FALSE;

		END IF;
	END IF;

	SET allow_concurrent_tuple_update = ON;
	
	UPDATE	pmk.pmk_configuration
	SET		config_value	          = l_config_value
	WHERE	UPPER(config_param_name)  = l_upper_config_param;

	IF NOT FOUND THEN
		l_error_message := 'ERROR:: Invalid configuration parameter "' || i_config_param_name || '" provided for configuring PMK parameter ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;
	
	RETURN TRUE;

END;
$$
LANGUAGE plpgsql;


-- If ALL is provided, it returns the details of all the configuration parameters.
-- If a specific config parameter is provided, it returns the details of the configuration parameter.

CREATE OR REPLACE FUNCTION pmk.get_configuration_parameter
				( IN	i_config_param_name				TEXT ) 
RETURNS SETOF pmk.pmk_configuration 
AS
$$
DECLARE					l_upper_config_param			TEXT;
						l_error_message					TEXT;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_upper_config_param	:= UPPER(TRIM(BOTH ' ' FROM i_config_param_name));

	IF l_upper_config_param = 'ALL'
	THEN
	
		RETURN QUERY 
		SELECT config_param_name
			 , config_value
		  FROM pmk.pmk_configuration
		 ORDER BY config_param_name;
	
	ELSE

		RETURN QUERY 
		SELECT config_param_name
			 , config_value
		  FROM pmk.pmk_configuration
		 WHERE UPPER(config_param_name)		= l_upper_config_param;

	END IF;

END;
$$
LANGUAGE plpgsql;
 /* 
	This function is used to collect statistics for each node (including data node and coordinator)
*/

CREATE OR REPLACE FUNCTION pmk.find_perf_stat 
                ( IN		i_node_type						char(1)
				, OUT       o_number_of_files            	int
                , OUT 		o_physical_reads             	bigint
                , OUT       o_physical_writes            	bigint
                , OUT       o_read_time                  	bigint
                , OUT       o_write_time                 	bigint 
                , OUT       o_physical_memory           	bigint
                , OUT       o_shared_buffer_size         	bigint
                , OUT       o_session_memory_total_size	 	bigint
                , OUT       o_session_memory_used_size  	bigint
                , OUT       o_blocks_read                	bigint
                , OUT       o_blocks_hit                 	bigint
                , OUT       o_db_size                    	bigint
                , OUT       o_work_memory_size           	bigint
                , OUT       o_sorts_in_memory            	bigint
                , OUT       o_sorts_in_disk              	bigint
                , OUT       o_active_sql_count           	int
                , OUT       o_wait_sql_count             	int
                , OUT       o_session_count              	int
                , OUT       o_busy_time                  	numeric
                , OUT       o_idle_time                  	numeric
                , OUT       o_iowait_time                	numeric
                , OUT       o_db_cpu_time                	numeric
                , OUT       o_db_memory_usage            	bigint
                , OUT       o_node_startup_time          	timestamp with time zone
                , OUT       o_node_host_name             	text 
                , OUT       o_xact_commit                	bigint
                , OUT       o_xact_rollback              	bigint
                , OUT       o_checkpoints_timed          	bigint
                , OUT       o_checkpoints_req            	bigint
                , OUT       o_checkpoint_write_time      	double precision
				)
AS
$$
DECLARE
          l_block_size            int;
          l_record_chk            int;
BEGIN

    o_node_startup_time := pg_postmaster_start_time();
    o_node_host_name    := get_hostname();

    SELECT COUNT(*) AS number_of_files
         , SUM(phyrds) AS physical_reads
         , SUM(phywrts) AS physical_writes
         , SUM(readtim) AS read_time
         , SUM(writetim) AS write_time
      INTO o_number_of_files
	     , o_physical_reads
         , o_physical_writes
         , o_read_time
         , o_write_time
      FROM pv_file_stat;

    IF o_number_of_files = 0
    THEN
	        o_physical_reads   := 0;
	        o_physical_writes  := 0;
	        o_read_time        := 0;
	        o_write_time       := 0;
    END IF;

      WITH os_stat AS 
         (
           SELECT os.name AS statname
                , os.value AS statvalue
             FROM pv_os_run_info os
            WHERE os.name      IN ( 'PHYSICAL_MEMORY_BYTES', 'BUSY_TIME', 'IDLE_TIME', 'IOWAIT_TIME' )
         )
    SELECT (SELECT statvalue FROM os_stat WHERE statname = 'PHYSICAL_MEMORY_BYTES')
         , (SELECT statvalue FROM os_stat WHERE statname = 'BUSY_TIME') 
         , (SELECT statvalue FROM os_stat WHERE statname = 'IDLE_TIME')  
         , (SELECT statvalue FROM os_stat WHERE statname = 'IOWAIT_TIME') 
      INTO o_physical_memory
	     , o_busy_time
		 , o_idle_time
		 , o_iowait_time
      FROM DUAL;

    --  pv_db_time is not available; temporarily PMK extension is used.
    o_db_cpu_time     := total_cpu();
    o_db_memory_usage := total_memory()*1024;

      WITH config_value AS 
         ( SELECT name
                , setting::bigint AS config_value
             FROM pg_settings 
            WHERE name   IN ( 'block_size', 'shared_buffers', 'work_mem' )
          )
          , config_value1 AS
          ( SELECT (SELECT config_value FROM config_value WHERE name = 'block_size') AS block_size 
                 , (SELECT config_value FROM config_value WHERE name = 'shared_buffers') AS shared_buffers 
                 , (SELECT config_value FROM config_value WHERE name = 'work_mem') AS work_mem 
              FROM DUAL
          )
    SELECT block_size
         , (shared_buffers * block_size)::bigint
         , (work_mem * 1024)::bigint
      INTO l_block_size
         , o_shared_buffer_size
         , o_work_memory_size
      FROM config_value1;

	/* Commented since these statistics are not used for node and cluster reports
	*/
    o_session_memory_total_size  := 0;
    o_session_memory_used_size   := 0;

	SELECT SUM(blks_read)::bigint
		 , SUM(blks_hit)::bigint
		 , SUM(xact_commit)::bigint
		 , SUM(xact_rollback)::bigint
	  INTO o_blocks_read
		 , o_blocks_hit
		 , o_xact_commit
		 , o_xact_rollback
	  FROM pg_stat_database;

	IF i_node_type = 'D'
	THEN
		SELECT SUM(pg_database_size(oid))::bigint
		  INTO o_db_size
		  FROM pg_database;
	ELSE
		o_db_size := 0;
	END IF;
	
      WITH session_state AS 
         ( SELECT state
                , waiting
             FROM pg_stat_activity 
         )
		 , active_session AS
         ( SELECT state
                , waiting
             FROM session_state
			WHERE state IN ('active', 'fastpath function call')
         )
    SELECT ( SELECT COUNT(*) FROM active_session )
         , ( SELECT COUNT(*) FROM active_session WHERE waiting = TRUE ) 
         , ( SELECT COUNT(*) FROM session_state )  
      INTO o_active_sql_count
         , o_wait_sql_count
         , o_session_count
      FROM DUAL;

    -- Currently, the below statistics are calculated from pv_session_stat (which is not accurate) since pv_db_stat is not available
      WITH sort_state AS 
         ( SELECT statname
                , SUM(value)::bigint AS sorts_cnt
             FROM pv_session_stat
            WHERE statname IN ('n_sort_in_memory', 'n_sort_in_disk') 			   
            GROUP BY statname
         )
    SELECT (SELECT sorts_cnt FROM sort_state WHERE statname = 'n_sort_in_memory') 
         , (SELECT sorts_cnt FROM sort_state WHERE statname = 'n_sort_in_disk')  
      INTO o_sorts_in_memory
         , o_sorts_in_disk
      FROM DUAL;

    SELECT SUM(checkpoints_timed)::bigint
         , SUM(checkpoints_req)::bigint
    	 , SUM(checkpoint_write_time)::bigint
      INTO o_checkpoints_timed
         , o_checkpoints_req
    	 , o_checkpoint_write_time
      FROM pg_stat_bgwriter;

END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pmk.find_node_stat 
			    ( IN        i_node_name     			 name 
				, IN        i_node_type     			 name 
                , OUT       o_number_of_files            int
                , OUT 		o_physical_reads             bigint
                , OUT       o_physical_writes            bigint
                , OUT       o_read_time                  bigint
                , OUT       o_write_time                 bigint 
                , OUT       o_physical_memory            bigint
                , OUT       o_shared_buffer_size         bigint
                , OUT       o_session_memory_total_size	 bigint
                , OUT       o_session_memory_used_size   bigint
                , OUT       o_blocks_read                bigint
                , OUT       o_blocks_hit                 bigint
                , OUT       o_db_size                    bigint
                , OUT       o_work_memory_size           bigint
                , OUT       o_sorts_in_memory            bigint
                , OUT       o_sorts_in_disk              bigint
                , OUT       o_active_sql_count           int
                , OUT       o_wait_sql_count             int
                , OUT       o_session_count              int
                , OUT       o_busy_time                  numeric
                , OUT       o_idle_time                  numeric
                , OUT       o_iowait_time                numeric
                , OUT       o_db_cpu_time                numeric
                , OUT       o_db_memory_usage            bigint
                , OUT       o_node_startup_time          timestamp with time zone
                , OUT       o_node_host_name             text 
                , OUT       o_xact_commit                bigint
                , OUT       o_xact_rollback              bigint
                , OUT       o_checkpoints_timed          bigint
                , OUT       o_checkpoints_req            bigint
                , OUT       o_checkpoint_write_time      double precision
				)
AS
$$
BEGIN

	EXECUTE 'EXECUTE DIRECT ON ( ' || i_node_name || ' ) ''SELECT o_number_of_files
															 	, o_physical_reads
															 	, o_physical_writes
															 	, o_read_time
															 	, o_write_time
															 	, o_physical_memory
															 	, o_shared_buffer_size
															 	, o_session_memory_total_size
															 	, o_session_memory_used_size
															 	, o_blocks_read
															 	, o_blocks_hit
															 	, o_db_size
															 	, o_work_memory_size
															 	, o_sorts_in_memory
															 	, o_sorts_in_disk
															 	, o_active_sql_count
															 	, o_wait_sql_count
															 	, o_session_count
															 	, o_busy_time
															 	, o_idle_time
															 	, o_iowait_time
															 	, o_db_cpu_time
															 	, o_db_memory_usage
															 	, o_node_startup_time
															 	, o_node_host_name
															 	, o_xact_commit
															 	, o_xact_rollback
															 	, o_checkpoints_timed
															 	, o_checkpoints_req
															 	, o_checkpoint_write_time
															 FROM pmk.find_perf_stat(''''' || i_node_type || ''''')'' '
															 INTO o_number_of_files
																, o_physical_reads
																, o_physical_writes
																, o_read_time
																, o_write_time
																, o_physical_memory
																, o_shared_buffer_size
																, o_session_memory_total_size
																, o_session_memory_used_size
																, o_blocks_read
																, o_blocks_hit
																, o_db_size
																, o_work_memory_size
																, o_sorts_in_memory
																, o_sorts_in_disk
																, o_active_sql_count
																, o_wait_sql_count
																, o_session_count
																, o_busy_time
																, o_idle_time
																, o_iowait_time
																, o_db_cpu_time
																, o_db_memory_usage
																, o_node_startup_time
																, o_node_host_name
																, o_xact_commit
																, o_xact_rollback
																, o_checkpoints_timed
																, o_checkpoints_req
																, o_checkpoint_write_time;

END;
$$
LANGUAGE plpgsql;

/* 
	This function is used to find the "delta values" of node statistics and 
	create into PMK table (pmk_snapshot_data_node_stat and pmk_snapshot_coordinator_stat).
*/

CREATE OR REPLACE FUNCTION pmk.load_datanode_stat 
                ( IN        i_snapshot_id				      int
				, IN		i_last_snapshot_id				  int
				, IN		i_pmk_last_collect_start_time     timestamp with time zone
				, IN        i_node_id                         int
				, IN        i_node_name                       text
				, IN        i_number_of_files                 int
				, IN        i_physical_reads                  bigint
				, IN        i_physical_writes                 bigint
				, IN        i_read_time                       bigint
				, IN        i_write_time                      bigint
				, IN        i_physical_memory                 bigint
				, IN        i_shared_buffer_size              bigint
				, IN        i_session_memory_total_size	      bigint
				, IN        i_session_memory_used_size        bigint
				, IN        i_blocks_read                     bigint
				, IN        i_blocks_hit                      bigint
				, IN        i_db_size                         bigint
				, IN        i_work_memory_size                bigint
				, IN        i_sorts_in_memory                 bigint
				, IN        i_sorts_in_disk                   bigint
				, IN        i_active_sql_count                int
				, IN        i_wait_sql_count                  int
				, IN        i_session_count                   int
				, IN        i_busy_time                       numeric
				, IN        i_idle_time                       numeric
				, IN        i_iowait_time                     numeric
				, IN        i_db_cpu_time                     numeric
				, IN        i_db_memory_usage                 bigint
				, IN        i_node_startup_time               timestamp with time zone
				, IN        i_node_host_name                  text
				, IN        i_xact_commit                     bigint
				, IN        i_xact_rollback                   bigint
				, IN        i_checkpoints_timed               bigint
				, IN        i_checkpoints_req                 bigint
				, IN        i_checkpoint_write_time           double precision
				)
RETURNS void
AS
$$
DECLARE l_physical_reads_delta            	bigint;
        l_physical_writes_delta           	bigint;
        l_read_time_delta                 	bigint;
        l_write_time_delta               	bigint;
        l_blocks_read_delta               	bigint;
        l_blocks_hit_delta                	bigint;
        l_sorts_in_memory_delta           	bigint;
        l_sorts_in_disk_delta             	bigint;
        l_busy_time_delta                	numeric;
        l_idle_time_delta                 	numeric;
        l_iowait_time_delta               	numeric;
        l_db_cpu_time_delta               	numeric;
        l_xact_commit_delta               	bigint;
        l_xact_rollback_delta             	bigint;
        l_checkpoints_timed_delta         	bigint;
        l_checkpoints_req_delta           	bigint;
        l_checkpoint_write_time_delta     	double precision;
BEGIN

	IF i_pmk_last_collect_start_time 	< i_node_startup_time
	   OR i_pmk_last_collect_start_time IS NULL
    THEN
		l_physical_reads_delta     	    := i_physical_reads;
		l_physical_writes_delta    	    := i_physical_writes;
		l_read_time_delta          	    := i_read_time;
		l_write_time_delta         	    := i_write_time;
		l_xact_commit_delta         	:= i_xact_commit;
		l_xact_rollback_delta       	:= i_xact_rollback;
		l_checkpoints_timed_delta    	:= i_checkpoints_timed;
		l_checkpoints_req_delta       	:= i_checkpoints_req;
		l_checkpoint_write_time_delta 	:= i_checkpoint_write_time;

		l_blocks_read_delta      		:= i_blocks_read;
		l_blocks_hit_delta       		:= i_blocks_hit;

		l_busy_time_delta   			:= i_busy_time;
		l_idle_time_delta   			:= i_idle_time;
		l_iowait_time_delta 			:= i_iowait_time;
		l_db_cpu_time_delta 			:= i_db_cpu_time;
	
	ELSE
	   -- MAX is specified here to address the scenario that a node is added after the last stat collect and it is started before the last stat collect. 
	   -- If MAX is not specified, it would throw "no data found" in this scenario
	   SELECT ( i_physical_reads     	- COALESCE(MAX(pns.physical_reads), 0) )
			, ( i_physical_writes    	- COALESCE(MAX(pns.physical_writes), 0) )
			, ( i_read_time         	- COALESCE(MAX(pns.read_time), 0) )
			, ( i_write_time          	- COALESCE(MAX(pns.write_time), 0) ) 
			, ( i_xact_commit          	- COALESCE(MAX(pns.xact_commit), 0) )
			, ( i_xact_rollback        	- COALESCE(MAX(pns.xact_rollback), 0) )
			, ( i_checkpoints_timed     - COALESCE(MAX(pns.checkpoints_timed), 0) )
			, ( i_checkpoints_req       - COALESCE(MAX(pns.checkpoints_req), 0) )
			, ( i_checkpoint_write_time - COALESCE(MAX(pns.checkpoint_write_time), 0) )
			, ( i_blocks_read     		- COALESCE(MAX(pns.blocks_read), 0) )
			, ( i_blocks_hit      		- COALESCE(MAX(pns.blocks_hit), 0) )
			, ( i_busy_time   			- COALESCE(MAX(pns.busy_time), 0) )
			, ( i_idle_time   			- COALESCE(MAX(pns.idle_time), 0) )
			, ( i_iowait_time 			- COALESCE(MAX(pns.iowait_time), 0) )
			, ( i_db_cpu_time 			- COALESCE(MAX(pns.db_cpu_time), 0) ) 
		 INTO l_physical_reads_delta
			, l_physical_writes_delta
			, l_read_time_delta
			, l_write_time_delta
			, l_xact_commit_delta
			, l_xact_rollback_delta
			, l_checkpoints_timed_delta
			, l_checkpoints_req_delta
			, l_checkpoint_write_time_delta
			, l_blocks_read_delta
			, l_blocks_hit_delta
			, l_busy_time_delta
			, l_idle_time_delta
			, l_iowait_time_delta
			, l_db_cpu_time_delta
		 FROM pmk.pmk_snapshot_data_node_stat pns
		WHERE pns.snapshot_id		 	 = i_last_snapshot_id
		  AND pns.node_id				 = i_node_id
		LIMIT 1;						 -- added for performance

	END IF;

	-- Currently, the below statistics are calculated from pv_session_stat (which is not accurate) since pv_db_stat is not available
	-- These statistics are cumulative from instance startup.
	l_sorts_in_memory_delta  := i_sorts_in_memory;
	l_sorts_in_disk_delta    := i_sorts_in_disk;

	INSERT INTO pmk.pmk_snapshot_data_node_stat 
			  ( snapshot_id
			  , node_id, node_name
			  ,	node_host
			  ,	last_startup_time
			  , number_of_files
			  , physical_reads, physical_reads_delta
			  , physical_writes, physical_writes_delta
			  , read_time, read_time_delta
			  , write_time, write_time_delta
			  , db_size
			  , active_sql_count, wait_sql_count
			  , session_count
			  , xact_commit, xact_commit_delta
			  , xact_rollback, xact_rollback_delta
			  , checkpoints_timed, checkpoints_timed_delta
			  , checkpoints_req, checkpoints_req_delta
			  , checkpoint_write_time, checkpoint_write_time_delta
			  , physical_memory
			  , db_memory_usage
			  , shared_buffer_size       
			  , session_memory_total_size
			  , session_memory_used_size 
			  , blocks_read, blocks_read_delta
			  , blocks_hit, blocks_hit_delta
			  , work_memory_size
			  , sorts_in_memory, sorts_in_memory_delta    
			  , sorts_in_disk, sorts_in_disk_delta
			  , busy_time, busy_time_delta
			  , idle_time, idle_time_delta
			  , iowait_time, iowait_time_delta
			  , db_cpu_time, db_cpu_time_delta 
			  )   
	   VALUES ( i_snapshot_id
			  , i_node_id, i_node_name
			  , i_node_host_name
			  , i_node_startup_time
			  , i_number_of_files
			  , i_physical_reads, l_physical_reads_delta
			  , i_physical_writes, l_physical_writes_delta
			  , i_read_time, l_read_time_delta
			  , i_write_time, l_write_time_delta
			  , i_db_size
			  , i_active_sql_count, i_wait_sql_count
			  , i_session_count
			  , i_xact_commit, l_xact_commit_delta
			  , i_xact_rollback, l_xact_rollback_delta
			  , i_checkpoints_timed, l_checkpoints_timed_delta
			  , i_checkpoints_req, l_checkpoints_req_delta
			  , i_checkpoint_write_time, l_checkpoint_write_time_delta
			  , i_physical_memory
			  , i_db_memory_usage
			  , i_shared_buffer_size
			  , i_session_memory_total_size
			  , i_session_memory_used_size
			  , i_blocks_read, l_blocks_read_delta
			  , i_blocks_hit, l_blocks_hit_delta
			  , i_work_memory_size
			  , i_sorts_in_memory, l_sorts_in_memory_delta
			  , i_sorts_in_disk, l_sorts_in_disk_delta
			  , i_busy_time, l_busy_time_delta
			  , i_idle_time, l_idle_time_delta
			  , i_iowait_time, l_iowait_time_delta
			  , i_db_cpu_time, l_db_cpu_time_delta
			  );

END;
$$
LANGUAGE plpgsql;
/* 
	This function is used to find the "delta values" of coordinator statistics and 
	create one or more entries (in multiple data nodes) into PMK table (pmk_snapshot_coordinator_stat).
*/

CREATE OR REPLACE FUNCTION pmk.load_coordinator_stat
                ( IN        i_snapshot_id				      int
				, IN		i_last_snapshot_id				  int
				, IN		i_pmk_last_collect_start_time     timestamp with time zone
				, IN        i_node_id                         int
				, IN        i_node_name                       text
				, IN        i_number_of_files                 int
				, IN        i_physical_reads                  bigint
				, IN        i_physical_writes                 bigint
				, IN        i_read_time                       bigint
				, IN        i_write_time                      bigint
				, IN        i_physical_memory                 bigint
				, IN        i_shared_buffer_size              bigint
				, IN        i_session_memory_total_size	      bigint
				, IN        i_session_memory_used_size        bigint
				, IN        i_blocks_read                     bigint
				, IN        i_blocks_hit                      bigint
				, IN        i_db_size                         bigint
				, IN        i_work_memory_size                bigint
				, IN        i_sorts_in_memory                 bigint
				, IN        i_sorts_in_disk                   bigint
				, IN        i_active_sql_count                int
				, IN        i_wait_sql_count                  int
				, IN        i_session_count                   int
				, IN        i_busy_time                       numeric
				, IN        i_idle_time                       numeric
				, IN        i_iowait_time                     numeric
				, IN        i_db_cpu_time                     numeric
				, IN        i_db_memory_usage                 bigint
				, IN        i_node_startup_time               timestamp with time zone
				, IN        i_node_host_name                  text
				, IN        i_xact_commit                     bigint
				, IN        i_xact_rollback                   bigint
				, IN        i_checkpoints_timed               bigint
				, IN        i_checkpoints_req                 bigint
				, IN        i_checkpoint_write_time           double precision
				)
RETURNS void
AS
$$
DECLARE l_physical_reads_delta            	bigint;
        l_physical_writes_delta           	bigint;
        l_read_time_delta                 	bigint;
        l_write_time_delta               	bigint;
        l_blocks_read_delta               	bigint;
        l_blocks_hit_delta                	bigint;
        l_sorts_in_memory_delta           	bigint;
        l_sorts_in_disk_delta             	bigint;
        l_busy_time_delta                	numeric;
        l_idle_time_delta                 	numeric;
        l_iowait_time_delta               	numeric;
        l_db_cpu_time_delta               	numeric;
        l_xact_commit_delta               	bigint;
        l_xact_rollback_delta             	bigint;
        l_checkpoints_timed_delta         	bigint;
        l_checkpoints_req_delta           	bigint;
        l_checkpoint_write_time_delta     	double precision;
BEGIN

	IF i_pmk_last_collect_start_time 	< i_node_startup_time
	   OR i_pmk_last_collect_start_time IS NULL
    THEN
		l_physical_reads_delta     	    := i_physical_reads;
		l_physical_writes_delta    	    := i_physical_writes;
		l_read_time_delta          	    := i_read_time;
		l_write_time_delta         	    := i_write_time;
		l_xact_commit_delta         	:= i_xact_commit;
		l_xact_rollback_delta       	:= i_xact_rollback;
		l_checkpoints_timed_delta    	:= i_checkpoints_timed;
		l_checkpoints_req_delta       	:= i_checkpoints_req;
		l_checkpoint_write_time_delta 	:= i_checkpoint_write_time;

		l_blocks_read_delta      		:= i_blocks_read;
		l_blocks_hit_delta       		:= i_blocks_hit;

		l_busy_time_delta   			:= i_busy_time;
		l_idle_time_delta   			:= i_idle_time;
		l_iowait_time_delta 			:= i_iowait_time;
		l_db_cpu_time_delta 			:= i_db_cpu_time;
	
	ELSE
	   -- MAX is specified here to address the scenario that a node is added after the last stat collect and it is started before the last stat collect. 
	   -- If MAX is not specified, it would throw "no data found" in this scenario
	   SELECT ( i_physical_reads     	- COALESCE(MAX(pns.physical_reads), 0) )
			, ( i_physical_writes    	- COALESCE(MAX(pns.physical_writes), 0) )
			, ( i_read_time         	- COALESCE(MAX(pns.read_time), 0) )
			, ( i_write_time          	- COALESCE(MAX(pns.write_time), 0) ) 
			, ( i_xact_commit          	- COALESCE(MAX(pns.xact_commit), 0) )
			, ( i_xact_rollback        	- COALESCE(MAX(pns.xact_rollback), 0) )
			, ( i_checkpoints_timed     - COALESCE(MAX(pns.checkpoints_timed), 0) )
			, ( i_checkpoints_req       - COALESCE(MAX(pns.checkpoints_req), 0) )
			, ( i_checkpoint_write_time - COALESCE(MAX(pns.checkpoint_write_time), 0) )
			, ( i_blocks_read     		- COALESCE(MAX(pns.blocks_read), 0) )
			, ( i_blocks_hit      		- COALESCE(MAX(pns.blocks_hit), 0) )
			, ( i_busy_time   			- COALESCE(MAX(pns.busy_time), 0) )
			, ( i_idle_time   			- COALESCE(MAX(pns.idle_time), 0) )
			, ( i_iowait_time 			- COALESCE(MAX(pns.iowait_time), 0) )
			, ( i_db_cpu_time 			- COALESCE(MAX(pns.db_cpu_time), 0) ) 
		 INTO l_physical_reads_delta
			, l_physical_writes_delta
			, l_read_time_delta
			, l_write_time_delta
			, l_xact_commit_delta
			, l_xact_rollback_delta
			, l_checkpoints_timed_delta
			, l_checkpoints_req_delta
			, l_checkpoint_write_time_delta
			, l_blocks_read_delta
			, l_blocks_hit_delta
			, l_busy_time_delta
			, l_idle_time_delta
			, l_iowait_time_delta
			, l_db_cpu_time_delta
		 FROM pmk.pmk_snapshot_coordinator_stat pns
		WHERE pns.snapshot_id		 	 = i_last_snapshot_id
		  AND pns.node_id				 = i_node_id
		LIMIT 1;						 -- added for performance

	END IF;

	-- Currently, the below statistics are calculated from pv_session_stat (which is not accurate) since pv_db_stat is not available
	-- These statistics are cumulative from instance startup.
	l_sorts_in_memory_delta  := i_sorts_in_memory;
	l_sorts_in_disk_delta    := i_sorts_in_disk;

	INSERT INTO pmk.pmk_snapshot_coordinator_stat 
			  ( snapshot_id
			  , node_id, node_name
			  ,	node_host
			  ,	last_startup_time
			  , number_of_files
			  , physical_reads, physical_reads_delta
			  , physical_writes, physical_writes_delta
			  , read_time, read_time_delta
			  , write_time, write_time_delta
			  , db_size
			  , active_sql_count, wait_sql_count
			  , session_count
			  , xact_commit, xact_commit_delta
			  , xact_rollback, xact_rollback_delta
			  , checkpoints_timed, checkpoints_timed_delta
			  , checkpoints_req, checkpoints_req_delta
			  , checkpoint_write_time, checkpoint_write_time_delta
			  , physical_memory
			  , db_memory_usage
			  , shared_buffer_size       
			  , session_memory_total_size
			  , session_memory_used_size 
			  , blocks_read, blocks_read_delta
			  , blocks_hit, blocks_hit_delta
			  , work_memory_size
			  , sorts_in_memory, sorts_in_memory_delta    
			  , sorts_in_disk, sorts_in_disk_delta
			  , busy_time, busy_time_delta
			  , idle_time, idle_time_delta
			  , iowait_time, iowait_time_delta
			  , db_cpu_time, db_cpu_time_delta 
			  )   
	   VALUES ( i_snapshot_id
			  , i_node_id, i_node_name
			  , i_node_host_name
			  , i_node_startup_time
			  , i_number_of_files
			  , i_physical_reads, l_physical_reads_delta
			  , i_physical_writes, l_physical_writes_delta
			  , i_read_time, l_read_time_delta
			  , i_write_time, l_write_time_delta
			  , i_db_size
			  , i_active_sql_count, i_wait_sql_count
			  , i_session_count
			  , i_xact_commit, l_xact_commit_delta
			  , i_xact_rollback, l_xact_rollback_delta
			  , i_checkpoints_timed, l_checkpoints_timed_delta
			  , i_checkpoints_req, l_checkpoints_req_delta
			  , i_checkpoint_write_time, l_checkpoint_write_time_delta
			  , i_physical_memory
			  , i_db_memory_usage
			  , i_shared_buffer_size
			  , i_session_memory_total_size
			  , i_session_memory_used_size
			  , i_blocks_read, l_blocks_read_delta
			  , i_blocks_hit, l_blocks_hit_delta
			  , i_work_memory_size
			  , i_sorts_in_memory, l_sorts_in_memory_delta
			  , i_sorts_in_disk, l_sorts_in_disk_delta
			  , i_busy_time, l_busy_time_delta
			  , i_idle_time, l_idle_time_delta
			  , i_iowait_time, l_iowait_time_delta
			  , i_db_cpu_time, l_db_cpu_time_delta
			  );

END;
$$
LANGUAGE plpgsql;
/* 
	This function is used to find the performance statistics of each node (including data node and coordinator) and
	create the statistics into PMK tables (pmk_snapshot_data_node_stat for data node and pmk_snapshot_coordinator_stat for coordinator).
*/

CREATE OR REPLACE FUNCTION pmk.load_node_stat 
                ( IN        i_pmk_curr_collect_start_time     TIMESTAMP WITH TIME ZONE
				, IN		i_pmk_last_collect_start_time	  TIMESTAMP WITH TIME ZONE
				, IN		i_last_snapshot_id				  INT
				)
RETURNS void
AS
$$
DECLARE l_snapshot_id				 	 	INT;
		l_query_str                       	TEXT;
        l_node_stat_cur                   	RECORD;
BEGIN

	INSERT INTO pmk_snapshot
			  ( current_snapshot_time
			  , last_snapshot_time
			  , creation_time )
	   VALUES ( i_pmk_curr_collect_start_time
			  , i_pmk_last_collect_start_time
			  , current_timestamp )
	  RETURNING snapshot_id 
	       INTO l_snapshot_id;
	
	FOR i IN ( SELECT oid AS node_id
	                , node_name
					, node_type
                 FROM pgxc_node
			    ORDER BY node_type DESC, oid )
	LOOP

		IF l_query_str IS NULL THEN
			l_query_str	:= 'SELECT ''' || i.node_name || ''' AS node_name
                                 , ''' || i.node_type || ''' AS node_type
								 ,   ' || i.node_id   ||   ' AS node_id
                                 , ns.*
                              FROM pmk.find_node_stat (''' || i.node_name || ''', ''' || i.node_type || ''') ns';

		ELSE
			l_query_str	:= l_query_str || ' 
                UNION ALL 
               SELECT ''' || i.node_name || ''' AS node_name
                    , ''' || i.node_type || ''' AS node_type
					,   ' || i.node_id   ||   ' AS node_id
                    , ns.*
                 FROM pmk.find_node_stat (''' || i.node_name || ''', ''' || i.node_type || ''') ns';
		END IF;

	END LOOP;

	FOR l_node_stat_cur IN EXECUTE l_query_str
	LOOP
		IF l_node_stat_cur.node_type = 'D'
		THEN
			-- Loading data-node statistics
			PERFORM pmk.load_datanode_stat ( l_snapshot_id
										   , i_last_snapshot_id
										   , i_pmk_last_collect_start_time
										   , l_node_stat_cur.node_id
										   , l_node_stat_cur.node_name
										   , l_node_stat_cur.o_number_of_files
										   , l_node_stat_cur.o_physical_reads
										   , l_node_stat_cur.o_physical_writes
										   , l_node_stat_cur.o_read_time
										   , l_node_stat_cur.o_write_time
										   , l_node_stat_cur.o_physical_memory
										   , l_node_stat_cur.o_shared_buffer_size
										   , l_node_stat_cur.o_session_memory_total_size
										   , l_node_stat_cur.o_session_memory_used_size
										   , l_node_stat_cur.o_blocks_read
										   , l_node_stat_cur.o_blocks_hit
										   , l_node_stat_cur.o_db_size
										   , l_node_stat_cur.o_work_memory_size
										   , l_node_stat_cur.o_sorts_in_memory
										   , l_node_stat_cur.o_sorts_in_disk
										   , l_node_stat_cur.o_active_sql_count
										   , l_node_stat_cur.o_wait_sql_count
										   , l_node_stat_cur.o_session_count
										   , l_node_stat_cur.o_busy_time
										   , l_node_stat_cur.o_idle_time
										   , l_node_stat_cur.o_iowait_time
										   , l_node_stat_cur.o_db_cpu_time
										   , l_node_stat_cur.o_db_memory_usage
										   , l_node_stat_cur.o_node_startup_time
										   , l_node_stat_cur.o_node_host_name
										   , l_node_stat_cur.o_xact_commit
										   , l_node_stat_cur.o_xact_rollback
										   , l_node_stat_cur.o_checkpoints_timed
										   , l_node_stat_cur.o_checkpoints_req
										   , l_node_stat_cur.o_checkpoint_write_time
										   );
		ELSE
			-- Loading coordinator statistics
			PERFORM pmk.load_coordinator_stat ( l_snapshot_id
											  , i_last_snapshot_id
											  , i_pmk_last_collect_start_time
											  , l_node_stat_cur.node_id
											  , l_node_stat_cur.node_name
											  , l_node_stat_cur.o_number_of_files
											  , l_node_stat_cur.o_physical_reads
											  , l_node_stat_cur.o_physical_writes
											  , l_node_stat_cur.o_read_time
											  , l_node_stat_cur.o_write_time
											  , l_node_stat_cur.o_physical_memory
											  , l_node_stat_cur.o_shared_buffer_size
											  , l_node_stat_cur.o_session_memory_total_size
											  , l_node_stat_cur.o_session_memory_used_size
											  , l_node_stat_cur.o_blocks_read
											  , l_node_stat_cur.o_blocks_hit
											  , l_node_stat_cur.o_db_size
											  , l_node_stat_cur.o_work_memory_size
											  , l_node_stat_cur.o_sorts_in_memory
											  , l_node_stat_cur.o_sorts_in_disk
											  , l_node_stat_cur.o_active_sql_count
											  , l_node_stat_cur.o_wait_sql_count
											  , l_node_stat_cur.o_session_count
											  , l_node_stat_cur.o_busy_time
											  , l_node_stat_cur.o_idle_time
											  , l_node_stat_cur.o_iowait_time
											  , l_node_stat_cur.o_db_cpu_time
											  , l_node_stat_cur.o_db_memory_usage
											  , l_node_stat_cur.o_node_startup_time
											  , l_node_stat_cur.o_node_host_name
											  , l_node_stat_cur.o_xact_commit
											  , l_node_stat_cur.o_xact_rollback
											  , l_node_stat_cur.o_checkpoints_timed
											  , l_node_stat_cur.o_checkpoints_req
											  , l_node_stat_cur.o_checkpoint_write_time
											  );
		END IF;
	
	END LOOP;
	
	-- setting current_snapshot_id and snapshot_time as last_snapshot_id and last_snapshot_collect_time
	UPDATE pmk.pmk_meta_data 
       SET last_snapshot_id   			= l_snapshot_id
		 , last_snapshot_collect_time	= i_pmk_curr_collect_start_time;

END;
$$
LANGUAGE plpgsql;

--	This function is used to delete the statistics snapshots based on "collection count" config param

CREATE OR REPLACE FUNCTION pmk.delete_expired_snapshots ( ) 
RETURNS void 
AS
$$
DECLARE l_collection_count                	INT;
		l_retention_snapshot_id				INT;
BEGIN

	-- Deleting node statistics based on  "collection count" config param
	SELECT config_value
	  INTO l_collection_count
	  FROM pmk.pmk_configuration
	 WHERE config_param_name = 'Collection Count';

	IF l_collection_count > -1
	THEN
		IF l_collection_count = 0
		THEN
			l_collection_count := 1;
		END IF;

		SELECT MIN(snapshot_id)
		  INTO l_retention_snapshot_id	
          FROM ( SELECT snapshot_id
                   FROM pmk.pmk_snapshot
                  ORDER BY snapshot_id DESC
                  LIMIT l_collection_count );

		DELETE FROM pmk.pmk_snapshot_data_node_stat
		 WHERE snapshot_id		< l_retention_snapshot_id;

		DELETE FROM pmk.pmk_snapshot_coordinator_stat
		 WHERE snapshot_id		< l_retention_snapshot_id;

		DELETE FROM pmk.pmk_snapshot
		 WHERE snapshot_id		< l_retention_snapshot_id;
		 
	END IF;
     
END;
$$
LANGUAGE plpgsql;

/* 
	This function is the interface to end user for collecting statistics. 
	It internally calls the "load_node_stat" function for each node (including data node and coordinator) 
	to find and create the statistics into PMK tables.
	Besides, it deletes the statistics from PMK tables based on the "Collection Count" configuration parameter.
*/

CREATE OR REPLACE FUNCTION pmk.collect_stat ( ) 
RETURNS boolean
AS
$$
DECLARE l_pmk_curr_collect_start_time     	TIMESTAMP WITH TIME ZONE; 
		l_pmk_last_collect_start_time	  	TIMESTAMP WITH TIME ZONE; 
		l_error_message					  	TEXT;
		l_last_snapshot_id				  	INT;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;
	END IF;

	-- If multiple statistics-collections are executed in the same coordinator
    IF ( SELECT COUNT(*) FROM pg_stat_activity
                 WHERE query  LIKE 'select% pmk.collect_stat%'
                   AND state  =  'active'
				 LIMIT 2
       ) > 1
    THEN
        l_error_message := 'ERROR:: Collect statistics is executed by another session.';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE; 
	END IF;

	-- FOR UPDATE clause is used to avoid multiple statistics-collections from different coordinators
	SELECT last_snapshot_id
		 , last_snapshot_collect_time
	  INTO l_last_snapshot_id
		 , l_pmk_last_collect_start_time
	  FROM pmk.pmk_meta_data
	   FOR UPDATE NOWAIT;

    l_pmk_curr_collect_start_time := date_trunc('second', current_timestamp);

	IF l_pmk_curr_collect_start_time < l_pmk_last_collect_start_time
	THEN
        l_error_message := 'ERROR:: There is a change in system time of Gauss MPPDB host. PMK does not support the scenarios related to system time change.';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;

	ELSIF l_pmk_curr_collect_start_time = l_pmk_last_collect_start_time
	THEN
        l_error_message := 'ERROR:: Multiple statistics-collections can not be done within a second.';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN FALSE;

    END IF;

	-- Find the statistics for each node and load into the PMK tables
	PERFORM pmk.load_node_stat ( l_pmk_curr_collect_start_time
							   , l_pmk_last_collect_start_time
							   , l_last_snapshot_id );

	-- Delete statistics based on "collection count" config param
	PERFORM pmk.delete_expired_snapshots ( );

    RETURN TRUE;

END;
$$
LANGUAGE plpgsql;

/* 
 If start time is greater than the last stat-collect time, it throws an error.
 If end time is null, it assign the last statistics collection time to the end time.
 If both start time and end time are null, it assign the last statistics collection time to both the parameters
 If start time is null and end time is not null, it throws an error.
 If start time is greater than end time, it throws an error.
*/
CREATE OR REPLACE FUNCTION pmk.check_start_end_dates
                ( INOUT        io_start_pmk_time              timestamp with time zone
                , INOUT        io_end_pmk_time                timestamp with time zone
				, OUT		   o_error_message				  text
                )
AS
$$
DECLARE                    l_last_collect_time                 timestamp with time zone;
BEGIN

	SELECT last_snapshot_collect_time
	  INTO l_last_collect_time
	  FROM pmk.pmk_meta_data;

	IF io_start_pmk_time > l_last_collect_time
	THEN
		o_error_message := 'ERROR:: The from-time provided is greater than the last statistics-collection time(' || l_last_collect_time || '). Invalid value(s) provided for the input time-range';
		RETURN;
	END IF;

    IF io_end_pmk_time IS NULL
    THEN
        io_end_pmk_time := l_last_collect_time;

        IF io_start_pmk_time IS NULL
        THEN
            io_start_pmk_time := io_end_pmk_time;
        END IF;
    ELSE
        IF (io_start_pmk_time IS NULL) OR 
           (io_start_pmk_time > io_end_pmk_time)
        THEN
            o_error_message := 'ERROR:: Invalid value(s) provided for the input time-range';
			RETURN;
        END IF;
    END IF;

END;
$$
LANGUAGE plpgsql;

-- Host CPU statistics at cluster level

CREATE OR REPLACE FUNCTION pmk.get_cluster_host_cpu_stat 
                ( IN        i_start_pmk_time              timestamp with time zone
                , IN        i_end_pmk_time                timestamp with time zone
                , OUT       o_stat_collect_time           timestamp
                , OUT       o_avg_cpu_total_time          numeric(21, 3)
                , OUT       o_avg_cpu_busy_time           numeric(21, 3)
                , OUT       o_avg_cpu_iowait_time         numeric(21, 3)
                , OUT       o_cpu_busy_perc			      numeric(5, 2)
                , OUT       o_cpu_io_wait_perc			  numeric(5, 2)
                , OUT       o_min_cpu_busy_perc			  numeric(5, 2)
                , OUT       o_max_cpu_busy_perc			  numeric(5, 2)
                , OUT       o_min_cpu_iowait_perc		  numeric(5, 2)
                , OUT       o_max_cpu_iowait_perc		  numeric(5, 2)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message); 
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster host CPU statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message); 
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	    , os_cpu_stat AS 
		( SELECT s.pmk_curr_collect_start_time
			   , node_host
			   , node_name
			   , (busy_time_delta * 10) AS cpu_busy_time
			   , (idle_time_delta * 10) AS cpu_idle_time
			   , (iowait_time_delta * 10) AS cpu_iowait_time
			  FROM pmk.pmk_snapshot_data_node_stat dns, snap s
			 WHERE dns.snapshot_id			= s.snapshot_id
			 UNION ALL
		  SELECT s.pmk_curr_collect_start_time
			   , node_host
			   , node_name
			   , (busy_time_delta * 10) AS cpu_busy_time
			   , (idle_time_delta * 10) AS cpu_idle_time
			   , (iowait_time_delta * 10) AS cpu_iowait_time
			  FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
			 WHERE dns.snapshot_id			= s.snapshot_id
		)
		, os_cpu_stat1 AS 
		( SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
			   , node_host
			   , cpu_busy_time
			   , cpu_idle_time
			   , cpu_iowait_time
			   , (cpu_busy_time+cpu_idle_time+cpu_iowait_time)::numeric AS cpu_total_time
			FROM ( SELECT pmk_curr_collect_start_time
						, node_host
						, cpu_busy_time
						, cpu_idle_time
						, cpu_iowait_time
						, rank() OVER (PARTITION BY pmk_curr_collect_start_time, node_host ORDER BY cpu_busy_time DESC, node_name) AS node_cpu_busy_order
					 FROM os_cpu_stat
				 )
		   WHERE node_cpu_busy_order = 1
		)
		SELECT hcs.stat_collect_time
		     , AVG(hcs.cpu_total_time)::numeric(21, 3) AS avg_cpu_total_time
			 , AVG(hcs.cpu_busy_time)::numeric(21, 3) AS avg_cpu_busy_time
			 , AVG(hcs.cpu_iowait_time)::numeric(21, 3) AS avg_cpu_iowait_time
			 , ( (SUM(cpu_busy_time) * 100.0) / NULLIF(SUM(cpu_total_time), 0) )::numeric(5, 2) AS cpu_busy_perc
             , ( (SUM(cpu_iowait_time) * 100.0) / NULLIF(SUM(cpu_total_time), 0) )::numeric(5, 2) AS cpu_io_wait_perc
	         , MIN(hcs.cpu_busy_time_perc)::numeric(5, 2) AS min_cpu_busy_perc
	         , MAX(hcs.cpu_busy_time_perc)::numeric(5, 2) AS max_cpu_busy_perc
	         , MIN(hcs.cpu_iowait_time_perc)::numeric(5, 2) AS min_cpu_iowait_perc
	         , MAX(hcs.cpu_iowait_time_perc)::numeric(5, 2) AS max_cpu_iowait_perc
		  FROM ( SELECT node_host
					  , stat_collect_time
					  , cpu_total_time
					  , cpu_busy_time
					  , cpu_iowait_time
					  , ( (cpu_busy_time * 100.0) / NULLIF(cpu_total_time, 0) )::numeric(5, 2) AS cpu_busy_time_perc
					  , ( (cpu_iowait_time * 100.0) / NULLIF(cpu_total_time, 0) )::numeric(5, 2) AS cpu_iowait_time_perc
				   FROM os_cpu_stat1 ) hcs
		 GROUP BY hcs.stat_collect_time
		 ORDER BY hcs.stat_collect_time;

END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pmk.get_cluster_mppdb_cpu_stat
                ( IN        i_start_pmk_time                          timestamp with time zone
                , IN        i_end_pmk_time                            timestamp with time zone
                , OUT       o_stat_collect_time           			  timestamp
                , OUT       o_avg_mppdb_cpu_time			          numeric(21, 3)
                , OUT       o_avg_host_cpu_busy_time		          numeric(21, 3)
                , OUT       o_avg_host_cpu_total_time		          numeric(21, 3)
                , OUT       o_mppdb_cpu_time_perc_wrt_busy_time		  numeric(5, 2)
                , OUT       o_mppdb_cpu_time_perc_wrt_total_time	  numeric(5, 2)
                , OUT       o_min_mppdb_cpu_time_perc_wrt_busy_time   numeric(5, 2)
                , OUT       o_max_mppdb_cpu_time_perc_wrt_busy_time   numeric(5, 2)
				, OUT		o_min_mppdb_cpu_time_perc_wrt_total_time  numeric(5, 2)
				, OUT		o_max_mppdb_cpu_time_perc_wrt_total_time  numeric(5, 2)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  			  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;
	
	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster MPPDB CPU statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
		 , cpu_stat AS
		 ( SELECT s.snapshot_id
				, s.pmk_curr_collect_start_time
				, dns.node_name
				, dns.node_host
				, (dns.busy_time_delta * 10) AS host_cpu_busy_time
				, (dns.idle_time_delta * 10) AS host_cpu_idle_time
				, (dns.iowait_time_delta * 10) AS host_cpu_iowait_time
				, (dns.db_cpu_time_delta * 10) AS mppdb_cpu_time
			 FROM pmk.pmk_snapshot_data_node_stat dns, snap s
			WHERE dns.snapshot_id			= s.snapshot_id
			UNION ALL
		   SELECT s.snapshot_id
				, s.pmk_curr_collect_start_time
				, dns.node_name
				, dns.node_host
				, (dns.busy_time_delta * 10) AS host_cpu_busy_time
				, (dns.idle_time_delta * 10) AS host_cpu_idle_time
				, (dns.iowait_time_delta * 10) AS host_cpu_iowait_time
				, (dns.db_cpu_time_delta * 10) AS mppdb_cpu_time
			 FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
			WHERE dns.snapshot_id			= s.snapshot_id
		 )
		, host_cpu_stat AS 
		( SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
			   , node_host
			   , host_cpu_busy_time
			   , host_cpu_idle_time
			   , host_cpu_iowait_time
			   , (host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time)::numeric AS host_cpu_total_time
			FROM ( SELECT pmk_curr_collect_start_time
						, node_host
						, host_cpu_busy_time
						, host_cpu_idle_time
						, host_cpu_iowait_time
						, rank() OVER (PARTITION BY snapshot_id, node_host 
										   ORDER BY host_cpu_busy_time DESC, node_name) AS node_cpu_busy_order
					 FROM cpu_stat
				 )
		   WHERE node_cpu_busy_order = 1
		)
		, host_cpu_stat_summary AS 
		( SELECT stat_collect_time
			   , AVG(host_cpu_busy_time)::numeric(21, 3) AS avg_host_cpu_busy_time
			   , AVG(host_cpu_total_time)::numeric(21, 3) AS avg_host_cpu_total_time
			   , SUM(host_cpu_busy_time)::numeric(21, 3) AS tot_host_cpu_busy_time
			   , SUM(host_cpu_total_time)::numeric(21, 3) AS tot_host_cpu_total_time
			FROM host_cpu_stat
		   GROUP BY stat_collect_time
		)
		, mppdb_cpu_stat0 AS
		( SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
			   , node_name
			   , mppdb_cpu_time
			   , host_cpu_busy_time
			   , (host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time)::numeric AS host_cpu_total_time
			FROM cpu_stat
		)
		, mppdb_cpu_stat AS
		( SELECT stat_collect_time
			   , node_name
			   , mppdb_cpu_time
			   , ( (mppdb_cpu_time * 100.0) / NULLIF(host_cpu_busy_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_busy_time
			   , ( (mppdb_cpu_time * 100.0) / NULLIF(host_cpu_total_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_total_time
			FROM mppdb_cpu_stat0
		)
		, mppdb_cpu_stat_summary AS
		( SELECT stat_collect_time
			   , AVG(mppdb_cpu_time)::numeric(21, 3) AS avg_mppdb_cpu_time
			   , SUM(mppdb_cpu_time)::numeric(21, 3) AS tot_mppdb_cpu_time
			   , MIN(mppdb_cpu_time_perc_wrt_busy_time)::numeric(5, 2) AS min_mppdb_cpu_time_perc_wrt_busy_time
			   , MAX(mppdb_cpu_time_perc_wrt_busy_time)::numeric(5, 2) AS max_mppdb_cpu_time_perc_wrt_busy_time
			   , MIN(mppdb_cpu_time_perc_wrt_total_time)::numeric(5, 2) AS min_mppdb_cpu_time_perc_wrt_total_time
			   , MAX(mppdb_cpu_time_perc_wrt_total_time)::numeric(5, 2) AS max_mppdb_cpu_time_perc_wrt_total_time
			FROM mppdb_cpu_stat
		   GROUP BY stat_collect_time
		)
	SELECT mcs.stat_collect_time
		 , mcs.avg_mppdb_cpu_time
		 , hcs.avg_host_cpu_busy_time
		 , hcs.avg_host_cpu_total_time
		 , CASE WHEN mcs.tot_mppdb_cpu_time < hcs.tot_host_cpu_busy_time
					 THEN ( (mcs.tot_mppdb_cpu_time * 100.0) / NULLIF(hcs.tot_host_cpu_busy_time, 0) )::numeric(5, 2) 
				ELSE 100.00
			END AS mppdb_cpu_time_perc_wrt_busy_time
		 , CASE WHEN mcs.tot_mppdb_cpu_time < hcs.tot_host_cpu_total_time
					 THEN ( (mcs.tot_mppdb_cpu_time * 100.0) / NULLIF(hcs.tot_host_cpu_total_time, 0) )::numeric(5, 2)
				ELSE 100.00
			END AS mppdb_cpu_time_perc_wrt_total_time
		 , mcs.min_mppdb_cpu_time_perc_wrt_busy_time
		 , mcs.max_mppdb_cpu_time_perc_wrt_busy_time
		 , mcs.min_mppdb_cpu_time_perc_wrt_total_time
		 , mcs.max_mppdb_cpu_time_perc_wrt_total_time
	  FROM mppdb_cpu_stat_summary mcs
	     , host_cpu_stat_summary hcs
	 WHERE mcs.stat_collect_time	= hcs.stat_collect_time
	 ORDER BY mcs.stat_collect_time;
	  
END;
$$
LANGUAGE plpgsql;

-- Shared buffer statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_shared_buffer_stat
                ( IN        i_start_pmk_time                  timestamp with time zone
                , IN        i_end_pmk_time                    timestamp with time zone
                , OUT       o_stat_collect_time           	  timestamp
                , OUT       o_shared_buffer_hit_ratio		  numeric(5, 2)
                , OUT       o_min_shared_buffer_hit_ratio     numeric(5, 2)
                , OUT       o_max_shared_buffer_hit_ratio     numeric(5, 2)
                , OUT       o_total_blocks_read               bigint
                , OUT       o_total_blocks_hit                bigint
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  	  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;
	
	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster shared buffer statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , ( (total_blocks_hit * 100.0) / NULLIF(total_blocks_read+total_blocks_hit, 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
		 , min_shared_buffer_hit_ratio
		 , max_shared_buffer_hit_ratio
		 , total_blocks_read
		 , total_blocks_hit
	  FROM ( SELECT pmk_curr_collect_start_time
				  , SUM(blocks_read)::bigint AS total_blocks_read
				  , SUM(blocks_hit)::bigint AS total_blocks_hit
				  , MIN(shared_buffer_hit_ratio)::numeric(5, 2) AS min_shared_buffer_hit_ratio
				  , MAX(shared_buffer_hit_ratio)::numeric(5, 2) AS max_shared_buffer_hit_ratio
			   FROM ( SELECT s.pmk_curr_collect_start_time
						   , node_name
						   , blocks_read_delta AS blocks_read
						   , blocks_hit_delta AS blocks_hit
						   , ( (blocks_hit_delta * 100.0) / NULLIF((blocks_read_delta + blocks_hit_delta), 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
						FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					   WHERE dns.snapshot_id			= s.snapshot_id
					   UNION ALL
					  SELECT s.pmk_curr_collect_start_time
						   , node_name
						   , blocks_read_delta AS blocks_read
						   , blocks_hit_delta AS blocks_hit
						   , ( (blocks_hit_delta * 100.0) / NULLIF((blocks_read_delta + blocks_hit_delta), 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
						FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
					   WHERE dns.snapshot_id			= s.snapshot_id
					)
			  GROUP BY pmk_curr_collect_start_time
			)
	  ORDER BY stat_collect_time;

END;
$$
LANGUAGE plpgsql;
-- Sort statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_memory_sort_stat
                ( IN        i_start_pmk_time                  timestamp with time zone
                , IN        i_end_pmk_time                    timestamp with time zone
                , OUT       o_stat_collect_time           	  timestamp
                , OUT       o_memory_sort_ratio		          numeric(5, 2)
                , OUT       o_min_memory_sort_ratio           numeric(5, 2)
                , OUT       o_max_memory_sort_ratio           numeric(5, 2)
                , OUT       o_total_memory_sorts              bigint
                , OUT       o_total_disk_sorts                bigint
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  	  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster memory sort statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , ( (total_memory_sorts * 100.0) / NULLIF(total_disk_sorts+total_memory_sorts, 0) )::numeric(5, 2) AS memory_sort_ratio
		 , min_memory_sort_ratio
		 , max_memory_sort_ratio
		 , total_memory_sorts
		 , total_disk_sorts
	  FROM ( SELECT pmk_curr_collect_start_time
				  , SUM(memory_sorts)::bigint AS total_memory_sorts
				  , SUM(disk_sorts)::bigint AS total_disk_sorts
				  , MIN(memory_sort_ratio)::numeric(5, 2) AS min_memory_sort_ratio
				  , MAX(memory_sort_ratio)::numeric(5, 2) AS max_memory_sort_ratio
			   FROM ( SELECT s.pmk_curr_collect_start_time
						   , node_name
						   , sorts_in_memory_delta AS memory_sorts
						   , sorts_in_disk_delta AS disk_sorts
						   , ( (sorts_in_memory_delta * 100.0) / NULLIF((sorts_in_disk_delta + sorts_in_memory_delta), 0) )::numeric(5, 2) AS memory_sort_ratio
						FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					   WHERE dns.snapshot_id			= s.snapshot_id
					   UNION ALL
					  SELECT s.pmk_curr_collect_start_time
						   , node_name
						   , sorts_in_memory_delta AS memory_sorts
						   , sorts_in_disk_delta AS disk_sorts
						   , ( (sorts_in_memory_delta * 100.0) / NULLIF((sorts_in_disk_delta + sorts_in_memory_delta), 0) )::numeric(5, 2) AS memory_sort_ratio
						FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
					   WHERE dns.snapshot_id			= s.snapshot_id
					)
			  GROUP BY pmk_curr_collect_start_time
			)
	 ORDER BY stat_collect_time;
		
END;
$$
LANGUAGE plpgsql;

-- I/O statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_io_stat 
                ( IN        i_start_pmk_time           	  timestamp with time zone
                , IN        i_end_pmk_time             	  timestamp with time zone
                , OUT       o_stat_collect_time        	  timestamp
                , OUT       o_number_of_files          	  int
                , OUT       o_physical_reads           	  bigint
                , OUT       o_physical_writes          	  bigint
                , OUT       o_read_time                	  bigint
                , OUT       o_write_time               	  bigint
                , OUT       o_avg_read_per_sec         	  numeric(20,2)
                , OUT       o_avg_read_time            	  numeric(20,3)
                , OUT       o_avg_write_per_sec        	  numeric(20,2)
                , OUT       o_avg_write_time           	  numeric(20,3)
				, OUT       o_min_node_read_per_sec	   	  numeric(20,2)
			    , OUT       o_max_node_read_per_sec	   	  numeric(20,2)
			    , OUT       o_min_node_read_time	   	  numeric(20,3)
			    , OUT       o_max_node_read_time	   	  numeric(20,3)
			    , OUT       o_min_node_write_per_sec   	  numeric(20,2)
			    , OUT       o_max_node_write_per_sec   	  numeric(20,2)
			    , OUT       o_min_node_write_time	   	  numeric(20,3)
			    , OUT       o_max_node_write_time	   	  numeric(20,3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster I/O statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , number_of_files
		 , physical_reads
		 , physical_writes
		 , read_time
		 , write_time
		 , ( physical_reads * 1000000.0 / NULLIF(read_time, 0) )::numeric(20,2) AS avg_read_per_sec
		 , ( read_time * 1.0 / NULLIF(physical_reads, 0) )::numeric(20,3) AS avg_read_time
		 , ( physical_writes * 1000000.0 / NULLIF(write_time, 0) )::numeric(20,2) AS avg_write_per_sec
		 , ( write_time * 1.0 / NULLIF(physical_writes, 0) )::numeric(20,3) AS avg_write_time
		 , min_node_read_per_sec
		 , max_node_read_per_sec
		 , min_node_read_time
		 , max_node_read_time
		 , min_node_write_per_sec
		 , max_node_write_per_sec
		 , min_node_write_time
		 , max_node_write_time
	  FROM ( SELECT pmk_curr_collect_start_time
				 , SUM(number_of_files)::int AS number_of_files
				 , SUM(physical_reads_delta)::bigint AS physical_reads
				 , SUM(physical_writes_delta)::bigint AS physical_writes
				 , SUM(read_time_delta)::bigint AS read_time
				 , SUM(write_time_delta)::bigint AS write_time
				 , MIN(node_read_per_sec) AS min_node_read_per_sec
				 , MAX(node_read_per_sec) AS max_node_read_per_sec
				 , MIN(node_read_time) AS min_node_read_time
				 , MAX(node_read_time) AS max_node_read_time
				 , MIN(node_write_per_sec) AS min_node_write_per_sec
				 , MAX(node_write_per_sec) AS max_node_write_per_sec
				 , MIN(node_write_time) AS min_node_write_time
				 , MAX(node_write_time) AS max_node_write_time
			  FROM ( SELECT s.pmk_curr_collect_start_time
						  , node_name
						  , number_of_files
						  , physical_reads_delta
						  , physical_writes_delta
						  , read_time_delta
						  , write_time_delta
						  , ( physical_reads_delta * 1000000.0 / NULLIF(read_time_delta, 0) )::numeric(20,2) AS node_read_per_sec
						  , ( read_time_delta * 1.0 / NULLIF(physical_reads_delta, 0) )::numeric(20,3) AS node_read_time
						  , ( physical_writes_delta * 1000000.0 / NULLIF(write_time_delta, 0) )::numeric(20,2) AS node_write_per_sec
						  , ( write_time_delta * 1.0 / NULLIF(physical_writes_delta, 0) )::numeric(20,3) AS node_write_time
					   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					  UNION ALL
					 SELECT s.pmk_curr_collect_start_time
						  , node_name
						  , number_of_files
						  , physical_reads_delta
						  , physical_writes_delta
						  , read_time_delta
						  , write_time_delta
						  , ( physical_reads_delta * 1000000.0 / NULLIF(read_time_delta, 0) )::numeric(20,2) AS node_read_per_sec
						  , ( read_time_delta * 1.0 / NULLIF(physical_reads_delta, 0) )::numeric(20,3) AS node_read_time
						  , ( physical_writes_delta * 1000000.0 / NULLIF(write_time_delta, 0) )::numeric(20,2) AS node_write_per_sec
						  , ( write_time_delta * 1.0 / NULLIF(physical_writes_delta, 0) )::numeric(20,3) AS node_write_time
					   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
				   )
			 GROUP BY pmk_curr_collect_start_time
			)
	ORDER BY stat_collect_time;

END;
$$
LANGUAGE plpgsql;
-- Disk usage statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_disk_usage_stat 
                ( IN        i_start_pmk_time            	timestamp with time zone
                , IN        i_end_pmk_time              	timestamp with time zone
                , OUT       o_stat_collect_time         	timestamp
                , OUT       o_tot_datanode_db_size      	text
				, OUT		o_max_datanode_db_size			text
                , OUT       o_tot_physical_writes       	bigint
                , OUT       o_max_node_physical_writes  	bigint
				, OUT		o_max_node_write_per_sec		numeric(20,2)
                , OUT       o_avg_write_per_sec         	numeric(20,2)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				    text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of cluster disk usage statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	     , disk_stat AS
	     (
			SELECT s.pmk_curr_collect_start_time
				 , db_size
				 , physical_writes_delta
				 , write_time_delta
				 , ( physical_writes_delta * 1000000.0 / NULLIF(write_time_delta, 0) )::numeric(20,2) AS node_write_per_sec
			  FROM pmk.pmk_snapshot_data_node_stat dns, snap s
			 WHERE dns.snapshot_id			= s.snapshot_id
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , pg_size_pretty(tot_datanode_db_size) AS tot_datanode_db_size
		 , pg_size_pretty(max_datanode_db_size) AS max_datanode_db_size
		 , tot_physical_writes
		 , max_node_physical_writes
		 , max_node_write_per_sec
		 , ( tot_physical_writes * 1000000.0 / NULLIF(tot_write_time, 0) )::numeric(20,2) AS avg_write_per_sec
	  FROM ( SELECT pmk_curr_collect_start_time
				  , SUM(db_size)::bigint AS tot_datanode_db_size
				  , MAX(db_size)::bigint AS max_datanode_db_size
				  , SUM(physical_writes_delta)::bigint AS tot_physical_writes
				  , SUM(write_time_delta)::bigint AS tot_write_time
				  , MAX(physical_writes_delta)::bigint AS max_node_physical_writes
				  , MAX(node_write_per_sec) AS max_node_write_per_sec
			   FROM disk_stat
			 GROUP BY pmk_curr_collect_start_time
		   )
	 ORDER BY pmk_curr_collect_start_time;

END;
$$
LANGUAGE plpgsql;
-- Active SQL count statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_active_sql_count 
                ( IN        i_start_pmk_time         	  timestamp with time zone
                , IN        i_end_pmk_time           	  timestamp with time zone
                , OUT       o_stat_collect_time      	  timestamp
                , OUT       o_tot_active_sql_count   	  int
                , OUT       o_avg_active_sql_count    	  numeric(9, 2)
                , OUT       o_min_active_sql_count     	  int
                , OUT       o_max_active_sql_count     	  int
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of active SQL count statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , tot_active_sql_count
		 , avg_active_sql_count
		 , min_active_sql_count
		 , max_active_sql_count
	  FROM ( SELECT s.pmk_curr_collect_start_time
				  , SUM(active_sql_count)::int AS tot_active_sql_count
				  , ROUND(AVG(active_sql_count), 2)::numeric(9, 2) AS avg_active_sql_count
				  , MIN(active_sql_count)::int AS min_active_sql_count
				  , MAX(active_sql_count)::int AS max_active_sql_count
			   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
			  WHERE dns.snapshot_id			= s.snapshot_id
			  GROUP BY s.pmk_curr_collect_start_time
			)
	 ORDER BY stat_collect_time;

END;
$$
LANGUAGE plpgsql;
-- Connected session count statistics at cluster levelCREATE OR REPLACE FUNCTION pmk.get_cluster_session_count 
                ( IN        i_start_pmk_time           	  timestamp with time zone
                , IN        i_end_pmk_time             	  timestamp with time zone
                , OUT       o_stat_collect_time        	  timestamp
                , OUT       o_tot_session_count        	  int
                , OUT       o_avg_session_count        	  numeric(9, 2)
                , OUT       o_min_session_count        	  int
                , OUT       o_max_session_count        	  int
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_error_message				  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of session count statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	RETURN QUERY 
	  WITH snap AS
	     ( SELECT snapshot_id
			    , current_snapshot_time AS pmk_curr_collect_start_time
		     FROM pmk.pmk_snapshot
		    WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
		 )
	SELECT pmk_curr_collect_start_time::timestamp AS stat_collect_time
		 , tot_session_count
		 , avg_session_count
		 , min_session_count
		 , max_session_count
	  FROM ( SELECT s.pmk_curr_collect_start_time
				  , SUM(session_count)::int AS tot_session_count
				  , ROUND(AVG(session_count), 2)::numeric(9, 2) AS avg_session_count
				  , MIN(session_count)::int AS min_session_count
				  , MAX(session_count)::int AS max_session_count
			   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
			  WHERE dns.snapshot_id			= s.snapshot_id
			  GROUP BY s.pmk_curr_collect_start_time
			)
	 ORDER BY stat_collect_time;

END;
$$
LANGUAGE plpgsql;
-- CPU statistics at node levelCREATE OR REPLACE FUNCTION pmk.get_node_cpu_stat 
                ( IN        i_node_name                          text
                , IN        i_start_pmk_time                     timestamp with time zone
                , IN        i_end_pmk_time                       timestamp with time zone
				, OUT		o_node_type							 char(1)
                , OUT       o_node_name                          text
                , OUT       o_node_host                          text
                , OUT       o_stat_collect_time                  timestamp
                , OUT       o_mppdb_cpu_time                     bigint
                , OUT       o_host_cpu_busy_time                 bigint
                , OUT       o_host_cpu_total_time                bigint
                , OUT       o_mppdb_cpu_time_perc_wrt_busy_time  numeric(5, 2)
                , OUT       o_mppdb_cpu_time_perc_wrt_total_time numeric(5, 2)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_node_type							 char(1);
							l_node_name                  		 text;
							l_error_message				  		 text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    l_node_name   := UPPER(i_node_name);

	IF l_node_name <> 'ALL'
	THEN
		SELECT MAX(node_type)
		  INTO l_node_type
		  FROM pgxc_node
		 WHERE UPPER(node_name)   = l_node_name
		 LIMIT 1;
	
		IF l_node_type IS NULL
		THEN
			l_error_message := 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of node (MPPDB instance) CPU statistics ...';
			
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN;
		END IF;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of node (MPPDB instance) CPU statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	IF l_node_name = 'ALL'
	THEN
		RETURN QUERY 
		  WITH snap AS
			 ( SELECT snapshot_id
					, current_snapshot_time AS pmk_curr_collect_start_time
				 FROM pmk.pmk_snapshot
				WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
			 )
		SELECT node_type
			 , node_name
		     , node_host
			 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
			 , mppdb_cpu_time
			 , host_cpu_busy_time
			 , (host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time)::bigint AS host_cpu_total_time
			 , ( (mppdb_cpu_time * 100.0) / NULLIF(host_cpu_busy_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_busy_time
			 , ( (mppdb_cpu_time * 100.0) / NULLIF((host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time), 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_total_time
		  FROM ( SELECT 'D'::char(1) AS node_type
					  , node_name
		              , node_host
					  , s.pmk_curr_collect_start_time
					  , (busy_time_delta * 10)::bigint AS host_cpu_busy_time
					  , (idle_time_delta * 10)::bigint AS host_cpu_idle_time
					  , (iowait_time_delta * 10)::bigint AS host_cpu_iowait_time
					  , (db_cpu_time_delta * 10)::bigint AS mppdb_cpu_time
				   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
				  UNION ALL
				 SELECT 'C'::char(1) AS node_type
					  , node_name
					  , node_host
				 	  , s.pmk_curr_collect_start_time
					  , (busy_time_delta * 10)::bigint AS host_cpu_busy_time
					  , (idle_time_delta * 10)::bigint AS host_cpu_idle_time
					  , (iowait_time_delta * 10)::bigint AS host_cpu_iowait_time
					  , (db_cpu_time_delta * 10)::bigint AS mppdb_cpu_time
				   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
				)
		ORDER BY node_type, node_name, stat_collect_time;

	ELSE

		IF l_node_type = 'D'
		THEN
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'D'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , mppdb_cpu_time
				 , host_cpu_busy_time
				 , (host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time)::bigint AS host_cpu_total_time
				 , ( (mppdb_cpu_time * 100.0) / NULLIF(host_cpu_busy_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_busy_time
				 , ( (mppdb_cpu_time * 100.0) / NULLIF((host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time), 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_total_time
			  FROM ( SELECT node_name
			              , node_host
						  , s.pmk_curr_collect_start_time
						  , (busy_time_delta * 10)::bigint AS host_cpu_busy_time
						  , (idle_time_delta * 10)::bigint AS host_cpu_idle_time
						  , (iowait_time_delta * 10)::bigint AS host_cpu_iowait_time
						  , (db_cpu_time_delta * 10)::bigint AS mppdb_cpu_time
					   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
					)
			ORDER BY node_name, stat_collect_time;
			
		ELSE
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'C'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , mppdb_cpu_time
				 , host_cpu_busy_time
				 , (host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time)::bigint AS host_cpu_total_time
				 , ( (mppdb_cpu_time * 100.0) / NULLIF(host_cpu_busy_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_busy_time
				 , ( (mppdb_cpu_time * 100.0) / NULLIF((host_cpu_busy_time+host_cpu_idle_time+host_cpu_iowait_time), 0) )::numeric(5, 2) AS mppdb_cpu_time_perc_wrt_total_time
			  FROM ( SELECT node_name
			              , node_host
						  , s.pmk_curr_collect_start_time
						  , (busy_time_delta * 10)::bigint AS host_cpu_busy_time
						  , (idle_time_delta * 10)::bigint AS host_cpu_idle_time
						  , (iowait_time_delta * 10)::bigint AS host_cpu_iowait_time
						  , (db_cpu_time_delta * 10)::bigint AS mppdb_cpu_time
					   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
					)
			ORDER BY node_name, stat_collect_time;

		END IF; -- end of l_node_type = 'D'
	END IF;		-- end of l_node_name = 'ALL'

END;
$$
LANGUAGE plpgsql;
-- Memory statistics at node levelCREATE OR REPLACE FUNCTION pmk.get_node_memory_stat 
                ( IN        i_node_name                       text
                , IN        i_start_pmk_time                  timestamp with time zone
                , IN        i_end_pmk_time                    timestamp with time zone
				, OUT		o_node_type						  char(1)
                , OUT       o_node_name                       text
                , OUT       o_node_host                       text
                , OUT       o_stat_collect_time               timestamp
                , OUT       o_physical_memory                 bigint
                , OUT       o_db_memory_usage                 bigint
                , OUT       o_shared_buffer_size              bigint
                , OUT       o_blocks_read                     bigint
                , OUT       o_blocks_hit                      bigint
                , OUT       o_shared_buffer_hit_ratio         numeric(5, 2)
                , OUT       o_work_memory_size     			  bigint
                , OUT       o_sorts_in_memory                 bigint
                , OUT       o_sorts_in_disk                   bigint
                , OUT       o_in_memory_sort_ratio            numeric(5, 2)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_node_type						  char(1);
							l_node_name                  	  text;
							l_error_message				  	  text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    l_node_name   := UPPER(i_node_name);

	IF l_node_name <> 'ALL'
	THEN
		SELECT MAX(node_type)
		  INTO l_node_type
		  FROM pgxc_node
		 WHERE UPPER(node_name)   = l_node_name
		 LIMIT 1;
	
		IF l_node_type IS NULL
		THEN
			l_error_message := 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of node (MPPDB instance) memory statistics ...';
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN;
		END IF;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of node (MPPDB instance) memory statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	IF l_node_name = 'ALL'
	THEN

		RETURN QUERY 
		  WITH snap AS
			 ( SELECT snapshot_id
					, current_snapshot_time AS pmk_curr_collect_start_time
				 FROM pmk.pmk_snapshot
				WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
			 )
		SELECT node_type
			 , node_name
		     , node_host
			 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
			 , physical_memory
			 , db_memory_usage
			 , shared_buffer_size
			 , blocks_read
			 , blocks_hit
			 , ( (blocks_hit * 100.0) / NULLIF((blocks_read + blocks_hit), 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
			 , work_memory_size
			 , sorts_in_memory
			 , sorts_in_disk
			 , ( (sorts_in_memory * 100.0) / NULLIF((sorts_in_disk + sorts_in_memory), 0) )::numeric(5, 2) AS in_memory_sort_ratio
		  FROM ( SELECT 'D'::char(1) AS node_type
					  , node_name
		              , node_host
					  , s.pmk_curr_collect_start_time
					  , physical_memory
					  , db_memory_usage
					  , shared_buffer_size
					  , blocks_read_delta AS blocks_read
					  , blocks_hit_delta AS blocks_hit
					  , work_memory_size
					  , sorts_in_memory_delta AS sorts_in_memory
					  , sorts_in_disk_delta AS sorts_in_disk
				   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
				  UNION ALL
				 SELECT 'C'::char(1) AS node_type
					  , node_name
		              , node_host
					  , s.pmk_curr_collect_start_time
					  , physical_memory
					  , db_memory_usage
					  , shared_buffer_size
					  , blocks_read_delta AS blocks_read
					  , blocks_hit_delta AS blocks_hit
					  , work_memory_size
					  , sorts_in_memory_delta AS sorts_in_memory
					  , sorts_in_disk_delta AS sorts_in_disk
				   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
				)
		ORDER BY node_type, node_name, stat_collect_time;
	ELSE
		IF l_node_type = 'D'
		THEN
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'D'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , physical_memory
				 , db_memory_usage
				 , shared_buffer_size
				 , blocks_read
				 , blocks_hit
				 , ( (blocks_hit * 100.0) / NULLIF((blocks_read + blocks_hit), 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
				 , work_memory_size
				 , sorts_in_memory
				 , sorts_in_disk
				 , ( (sorts_in_memory * 100.0) / NULLIF((sorts_in_disk + sorts_in_memory), 0) )::numeric(5, 2) AS in_memory_sort_ratio
			  FROM ( SELECT node_name
						  , node_host
						  , s.pmk_curr_collect_start_time
						  , physical_memory
						  , db_memory_usage
						  , shared_buffer_size
						  , blocks_read_delta AS blocks_read
						  , blocks_hit_delta AS blocks_hit
						  , work_memory_size
						  , sorts_in_memory_delta AS sorts_in_memory
						  , sorts_in_disk_delta AS sorts_in_disk
					   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
					)
			ORDER BY node_name, stat_collect_time;

		ELSE
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'C'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , physical_memory
				 , db_memory_usage
				 , shared_buffer_size
				 , blocks_read
				 , blocks_hit
				 , ( (blocks_hit * 100.0) / NULLIF((blocks_read + blocks_hit), 0) )::numeric(5, 2) AS shared_buffer_hit_ratio
				 , work_memory_size
				 , sorts_in_memory
				 , sorts_in_disk
				 , ( (sorts_in_memory * 100.0) / NULLIF((sorts_in_disk + sorts_in_memory), 0) )::numeric(5, 2) AS in_memory_sort_ratio
			  FROM ( SELECT node_name
						  , node_host
						  , s.pmk_curr_collect_start_time
						  , physical_memory
						  , db_memory_usage
						  , shared_buffer_size
						  , blocks_read_delta AS blocks_read
						  , blocks_hit_delta AS blocks_hit
						  , work_memory_size
						  , sorts_in_memory_delta AS sorts_in_memory
						  , sorts_in_disk_delta AS sorts_in_disk
					   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
				      WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
					)
			ORDER BY node_name, stat_collect_time;

		END IF; -- end of l_node_type = 'D'
	END IF;		-- end of l_node_name = 'ALL'

END;
$$
LANGUAGE plpgsql;

-- I/O statistics at node levelCREATE OR REPLACE FUNCTION pmk.get_node_io_stat 
                ( IN        i_node_name           	        text
                , IN        i_start_pmk_time      	        timestamp with time zone
                , IN        i_end_pmk_time        	        timestamp with time zone
				, OUT		o_node_type						char(1)
                , OUT       o_node_name                     text
                , OUT       o_node_host                     text
                , OUT       o_stat_collect_time             timestamp
                , OUT       o_number_of_files     	        int
                , OUT       o_physical_reads      	        bigint
                , OUT       o_physical_writes    	        bigint
                , OUT       o_read_time          	        bigint
                , OUT       o_write_time          	     	bigint
                , OUT       o_avg_read_per_sec     		    numeric(20,2)
                , OUT       o_avg_read_time       		    numeric(20,3)
                , OUT       o_avg_write_per_sec    	    	numeric(20,2)
                , OUT       o_avg_write_time      	     	numeric(20,3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_node_type						char(1);
							l_node_name                  	text;
							l_error_message				  	text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    l_node_name   := UPPER(i_node_name);

	IF l_node_name <> 'ALL'
	THEN
		SELECT MAX(node_type)
		  INTO l_node_type
		  FROM pgxc_node
		 WHERE UPPER(node_name)   = l_node_name
		 LIMIT 1;
	
		IF l_node_type IS NULL
		THEN
			l_error_message := 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of node (MPPDB instance) I/O statistics ...';
			DBMS_OUTPUT.PUT_LINE(l_error_message);
			RETURN;
		END IF;
	END IF;

	-- Verifying the input start and end times 
	pmk.check_start_end_dates(i_start_pmk_time, i_end_pmk_time, l_error_message);

	IF  l_error_message IS NOT NULL
	THEN
		l_error_message := l_error_message || ' during generation of node (MPPDB instance) I/O statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	IF l_node_name = 'ALL'
	THEN

		RETURN QUERY 
		  WITH snap AS
			 ( SELECT snapshot_id
					, current_snapshot_time AS pmk_curr_collect_start_time
				 FROM pmk.pmk_snapshot
				WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
			 )
		SELECT node_type
			 , node_name
		     , node_host
			 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
			 , number_of_files
			 , physical_reads
			 , physical_writes
			 , read_time
			 , write_time
			 , ( physical_reads * 1000000.0 / NULLIF(read_time, 0) )::numeric(20,2) AS avg_read_per_sec
			 , ( read_time * 1.0 / NULLIF(physical_reads, 0) )::numeric(20,3) AS avg_read_time
			 , ( physical_writes * 1000000.0 / NULLIF(write_time, 0) )::numeric(20,2) AS avg_write_per_sec
			 , ( write_time * 1.0 / NULLIF(physical_writes, 0) )::numeric(20,3) AS avg_write_time
		  FROM ( SELECT 'D'::char(1) AS node_type
					  , node_name
		              , node_host
					  , s.pmk_curr_collect_start_time
					  , number_of_files
					  , physical_reads_delta AS physical_reads
					  , physical_writes_delta AS physical_writes
					  , read_time_delta AS read_time
					  , write_time_delta AS write_time
				   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
				  UNION ALL
				 SELECT 'C'::char(1) AS node_type
					  , node_name
		              , node_host
					  , s.pmk_curr_collect_start_time
					  , number_of_files
					  , physical_reads_delta AS physical_reads
					  , physical_writes_delta AS physical_writes
					  , read_time_delta AS read_time
					  , write_time_delta AS write_time
				   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
				  WHERE dns.snapshot_id			= s.snapshot_id
			   )
		ORDER BY node_type, node_name, stat_collect_time;

	ELSE

		IF l_node_type = 'D'
		THEN
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'D'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , number_of_files
				 , physical_reads
				 , physical_writes
				 , read_time
				 , write_time
				 , ( physical_reads * 1000000.0 / NULLIF(read_time, 0) )::numeric(20,2) AS avg_read_per_sec
				 , ( read_time * 1.0 / NULLIF(physical_reads, 0) )::numeric(20,3) AS avg_read_time
				 , ( physical_writes * 1000000.0 / NULLIF(write_time, 0) )::numeric(20,2) AS avg_write_per_sec
				 , ( write_time * 1.0 / NULLIF(physical_writes, 0) )::numeric(20,3) AS avg_write_time
			  FROM ( SELECT node_name
						  , node_host
						  , pmk_curr_collect_start_time
						  , number_of_files
						  , physical_reads_delta AS physical_reads
						  , physical_writes_delta AS physical_writes
						  , read_time_delta AS read_time
						  , write_time_delta AS write_time
					   FROM pmk.pmk_snapshot_data_node_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
				   )
			ORDER BY node_name, stat_collect_time;
			
		ELSE
			RETURN QUERY 
			  WITH snap AS
				 ( SELECT snapshot_id
						, current_snapshot_time AS pmk_curr_collect_start_time
					 FROM pmk.pmk_snapshot
					WHERE current_snapshot_time		BETWEEN i_start_pmk_time AND i_end_pmk_time
				 )
			SELECT 'C'::char(1) AS node_type
				 , node_name
			     , node_host
				 , pmk_curr_collect_start_time::timestamp AS stat_collect_time
				 , number_of_files
				 , physical_reads
				 , physical_writes
				 , read_time
				 , write_time
				 , ( physical_reads * 1000000.0 / NULLIF(read_time, 0) )::numeric(20,2) AS avg_read_per_sec
				 , ( read_time * 1.0 / NULLIF(physical_reads, 0) )::numeric(20,3) AS avg_read_time
				 , ( physical_writes * 1000000.0 / NULLIF(write_time, 0) )::numeric(20,2) AS avg_write_per_sec
				 , ( write_time * 1.0 / NULLIF(physical_writes, 0) )::numeric(20,3) AS avg_write_time
			  FROM ( SELECT node_name
						  , node_host
						  , pmk_curr_collect_start_time
						  , number_of_files
						  , physical_reads_delta AS physical_reads
						  , physical_writes_delta AS physical_writes
						  , read_time_delta AS read_time
						  , write_time_delta AS write_time
					   FROM pmk.pmk_snapshot_coordinator_stat dns, snap s
					  WHERE dns.snapshot_id			= s.snapshot_id
					    AND UPPER(node_name)        = l_node_name
				   )
			ORDER BY node_name, stat_collect_time;

		END IF; -- end of l_node_type = 'D'
	END IF;		-- end of l_node_name = 'ALL'

END;
$$
LANGUAGE plpgsql;
-- This function is used to find TOP N sessions, which take more CPU time-- But this function returns Top N sessions from each node.
-- The outer function (get_session_cpu_stat) return the Top N sessions as the final result.

CREATE OR REPLACE FUNCTION pmk.get_session_cpu_stat0
                ( IN        i_node_name                  text
                , IN        i_top_n_sessions             smallint
                , OUT       o_node_name                  name
                , OUT       o_db_name                    name
                , OUT       o_user_name                  name
                , OUT       o_client_hostname            text
                , OUT       o_session_start_time         timestamp
                , OUT       o_xact_start_time            timestamp
                , OUT       o_waiting                    boolean
                , OUT       o_state                      text
                , OUT       o_query                      text
                , OUT       o_session_cpu_time           bigint
                , OUT       o_mppdb_cpu_time             bigint 
                , OUT       o_mppdb_cpu_time_perc        numeric(5, 2) 
				, OUT		o_avg_sql_exec_time		     numeric(15, 3)
                ) 
RETURNS SETOF RECORD 
AS
$$
DECLARE					    l_node_query                 text;
						    l_execute_query              text;
BEGIN

    FOR i IN ( SELECT node_name
                 FROM pgxc_node nl 
                WHERE UPPER(nl.node_name) = COALESCE(NULLIF(i_node_name, 'ALL'), UPPER(nl.node_name))
             )
	LOOP

        l_node_query := 'WITH sess_time_stat0 AS
							( SELECT sessid, stat_name
								   , (value/1000.0)::numeric AS stat_value			-- converting to millisecond
                                FROM pv_session_time 
                               WHERE stat_name          IN ( ''''CPU_TIME'''', ''''EXECUTION_TIME'''')
							)  
							, sess_time_stat AS
							( SELECT DISTINCT stso.sessid
								   , (SELECT stsi.stat_value FROM sess_time_stat0 stsi WHERE stsi.sessid = stso.sessid AND stsi.stat_name = ''''CPU_TIME'''') AS session_cpu_time
								   , (SELECT stsi.stat_value FROM sess_time_stat0 stsi WHERE stsi.sessid = stso.sessid AND stsi.stat_name = ''''EXECUTION_TIME'''') AS session_sql_time
							    FROM sess_time_stat0 stso
							) 
							, mppdb_cpu_time AS
							( SELECT (total_cpu()*10.0)::bigint AS mppdb_cpu_time 	-- converting to millisecond
                               FROM DUAL
							)
							, sess_cpu_stat AS
							( SELECT ''''' || i.node_name  || '''''::name AS node_name
								   , a.datname::name AS db_name
								   , a.usename::name AS user_name
								   , a.client_hostname
								   , date_trunc(''''second'''', a.backend_start)::timestamp AS session_start_time
								   , date_trunc(''''second'''', a.xact_start)::timestamp AS xact_start_time
								   , a.waiting
								   , a.state, a.query
								   , ROUND(st.session_cpu_time)::bigint AS session_cpu_time
								   , m.mppdb_cpu_time
								   , ( (st.session_cpu_time * 100.0) / NULLIF(m.mppdb_cpu_time, 0) )::numeric(5, 2) AS mppdb_cpu_time_perc
								   , st.sessid
								   , st.session_sql_time
							    FROM pg_stat_activity a
								   , sess_time_stat st
								   , mppdb_cpu_time m
							   WHERE a.state	          IN (''''active'''', ''''fastpath function call'''')
								 AND a.pid             	  =  sessionid2pid(st.sessid::cstring)
							   ORDER BY st.session_cpu_time DESC
									  , mppdb_cpu_time_perc DESC
							   LIMIT ' || i_top_n_sessions || '
						    )
							SELECT scs.node_name
								 , scs.db_name
								 , scs.user_name
								 , scs.client_hostname
								 , scs.session_start_time
								 , scs.xact_start_time
								 , scs.waiting
								 , scs.state
								 , scs.query
								 , scs.session_cpu_time
								 , scs.mppdb_cpu_time
								 , scs.mppdb_cpu_time_perc
								 , ( scs.session_sql_time / NULLIF(ss.value, 0) )::numeric(15, 3) AS avg_sql_exec_time
							  FROM sess_cpu_stat scs
								 , pv_session_stat ss
							 WHERE ss.sessid		  			=  scs.sessid
							   AND ss.statname		  			=  ''''n_sql''''';

        l_execute_query := 'EXECUTE DIRECT ON ( ' || i.node_name || ' ) ''' || l_node_query || '''';

		RETURN QUERY
		EXECUTE l_execute_query;

    END LOOP;

END;
$$
LANGUAGE plpgsql;

-- This function is used to find TOP n sessions, which take more CPU timeCREATE OR REPLACE FUNCTION pmk.get_session_cpu_stat
                ( IN        i_node_name                  text
                , IN        i_top_n_sessions             smallint
                , OUT       o_node_name                  name
                , OUT       o_db_name                    name
                , OUT       o_user_name                  name
                , OUT       o_client_hostname            text
                , OUT       o_session_start_time         timestamp
                , OUT       o_xact_start_time            timestamp
                , OUT       o_waiting                    boolean
                , OUT       o_state                      text
                , OUT       o_query                      text
                , OUT       o_session_cpu_time           bigint
                , OUT       o_mppdb_cpu_time             bigint
                , OUT       o_mppdb_cpu_time_perc        numeric(5, 2)
				, OUT		o_avg_sql_exec_time			 numeric(15, 3)
                ) 
RETURNS SETOF RECORD 
AS
$$
DECLARE                     l_node_name                  text;
                            l_error_message				 text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    IF i_top_n_sessions < 0
    THEN
        l_error_message		:= 'ERROR:: Negative value provided for Top N Sessions during generation of session CPU statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
    END IF;

    l_node_name   := UPPER(i_node_name);

    IF l_node_name <> 'ALL'
       AND NOT EXISTS ( SELECT 1 FROM pgxc_node
                         WHERE UPPER(node_name)   = l_node_name )
    THEN
        l_error_message		:= 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of session CPU statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
    END IF;

    RETURN QUERY
    SELECT scs.o_node_name
         , scs.o_db_name
         , scs.o_user_name
         , scs.o_client_hostname
         , scs.o_session_start_time
         , scs.o_xact_start_time
         , scs.o_waiting
         , scs.o_state
         , scs.o_query
         , scs.o_session_cpu_time
         , scs.o_mppdb_cpu_time
         , scs.o_mppdb_cpu_time_perc
		 , scs.o_avg_sql_exec_time
      FROM pmk.get_session_cpu_stat0(l_node_name, i_top_n_sessions) scs
     ORDER BY scs.o_session_cpu_time DESC
            , scs.o_mppdb_cpu_time_perc DESC
		    , scs.o_node_name
	 LIMIT i_top_n_sessions;

END;
$$
LANGUAGE plpgsql;
-- This function is used to find TOP N sessions, which are sorted by physical_reads DESC and sorts_in_memory DESC
-- But this function returns Top N sessions from each node.
-- The outer function (get_session_memory_stat) return the Top N sessions as the final result.

CREATE OR REPLACE FUNCTION pmk.get_session_memory_stat0 
                ( IN        i_node_name                    text
                , IN        i_top_n_sessions               smallint
                , OUT       o_node_name                    name
                , OUT       o_db_name                      name
                , OUT       o_user_name                    name
                , OUT       o_client_hostname              text
                , OUT       o_session_start_time           timestamp
                , OUT       o_xact_start_time              timestamp
                , OUT       o_waiting                      boolean
                , OUT       o_state                        text
                , OUT       o_query                        text
                , OUT       o_session_total_memory_size    bigint
                , OUT       o_session_used_memory_size     bigint
                , OUT       o_buffer_hits	               bigint
                , OUT       o_disk_reads	               bigint
                , OUT       o_session_buffer_hit_ratio     numeric(5, 2)
                , OUT       o_sorts_in_memory              bigint
                , OUT       o_sorts_in_disk                bigint
                , OUT       o_session_memory_sort_ratio    numeric(5, 2)
				, OUT		o_avg_sql_exec_time			   numeric(15, 3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE					    l_node_query                   text;
						    l_execute_query                text;
BEGIN

    FOR i IN ( SELECT node_name
                 FROM pgxc_node nl 
                WHERE UPPER(nl.node_name) = COALESCE(NULLIF(i_node_name, 'ALL'), UPPER(nl.node_name))
             )
	LOOP

        l_node_query := 'WITH sess_memory_usage AS
							( SELECT sessid
                                   , SUM(totalsize)::bigint AS totalsize
                                   , SUM(usedsize)::bigint AS usedsize
                                FROM pv_session_memory_detail
                               GROUP BY sessid
							) 
							, sess_stat0 AS 
							( SELECT ss.sessid
                                   , ss.statname AS statname
                                   , ss.value AS statvalue
                                FROM sess_memory_usage st, pv_session_stat ss
                               WHERE ss.sessid        = st.sessid
                                 AND ss.statname      IN ( ''''n_blocks_fetched''''
														 , ''''n_shared_blocks_read'''', ''''n_local_blocks_read''''
                                                         , ''''n_sort_in_disk'''', ''''n_sort_in_memory'''' 
														 , ''''n_sql'''' )
							)
							, sess_stat1 AS 
							( SELECT oss.sessid
								  , oss.totalsize
								  , oss.usedsize
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_blocks_fetched'''') AS total_reads
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_shared_blocks_read'''') AS disk_to_shared_buffer
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_local_blocks_read'''') AS disk_to_local_buffer
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_sort_in_disk'''') AS sorts_in_disk
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_sort_in_memory'''') AS sorts_in_memory 
								  , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = oss.sessid AND iss.statname = ''''n_sql'''') AS sql_count 
							   FROM sess_memory_usage oss
							)
							, sess_stat AS
							( SELECT ss.sessid
                                   , ss.totalsize
                                   , ss.usedsize
								   , ss.total_reads
                                   , (ss.disk_to_shared_buffer + ss.disk_to_local_buffer) AS disk_reads
								   , (ss.total_reads - (ss.disk_to_shared_buffer+ss.disk_to_local_buffer)) AS buffer_hits
								   , sorts_in_disk
								   , sorts_in_memory
								   , sql_count
                                FROM sess_stat1 ss
							)
							, sess_memory_stat AS
							( SELECT ''''' || i.node_name  || '''''::name AS node_name
								   , a.datname::name AS db_name
								   , a.usename::name AS user_name
								   , a.client_hostname
								   , date_trunc(''''second'''', a.backend_start)::timestamp AS session_start_time
								   , date_trunc(''''second'''', a.xact_start)::timestamp AS xact_start_time
								   , a.waiting
								   , a.state, a.query
								   , st.totalsize AS session_total_memory_size
								   , st.usedsize AS session_used_memory_size
								   , st.buffer_hits, st.disk_reads
								   , ( (st.buffer_hits * 100.0) / NULLIF(st.total_reads, 0) )::numeric(5, 2) AS session_buffer_hit_ratio
								   , st.sorts_in_memory, st.sorts_in_disk
								   , ( (st.sorts_in_memory * 100.0) / NULLIF(st.sorts_in_memory + st.sorts_in_disk, 0) )::numeric(5, 2) AS session_memory_sort_ratio
								   , st.sessid
								   , st.sql_count
							    FROM pg_stat_activity a
								   , sess_stat st
							   WHERE a.state	          IN (''''active'''', ''''fastpath function call'''')
								 AND a.pid              = sessionid2pid(st.sessid::cstring)
							   ORDER BY st.totalsize DESC
									  , st.usedsize DESC
							   LIMIT ' || i_top_n_sessions || '
						    )
							SELECT sms.node_name
								 , sms.db_name
								 , sms.user_name
								 , sms.client_hostname
								 , sms.session_start_time
								 , sms.xact_start_time
								 , sms.waiting
								 , sms.state
								 , sms.query
								 , sms.session_total_memory_size
								 , sms.session_used_memory_size
								 , sms.buffer_hits
								 , sms.disk_reads
								 , sms.session_buffer_hit_ratio
								 , sms.sorts_in_memory
								 , sms.sorts_in_disk
								 , sms.session_memory_sort_ratio
								 , ( ss.value / (NULLIF(sms.sql_count, 0) * 1000.0) )::numeric(15, 3) AS avg_sql_exec_time
							  FROM sess_memory_stat sms
								 , pv_session_time ss
							 WHERE ss.sessid		  			=  sms.sessid
							   AND ss.stat_name		  			=  ''''EXECUTION_TIME''''';

        l_execute_query := 'EXECUTE DIRECT ON ( ' || i.node_name || ' ) ''' || l_node_query || '''';

		RETURN QUERY
		EXECUTE l_execute_query;

    END LOOP;

END;
$$
LANGUAGE plpgsql;

-- This function is used to find TOP n sessions, which use more memory (physical_reads DESC and sorts_in_memory DESC)CREATE OR REPLACE FUNCTION pmk.get_session_memory_stat 
                ( IN        i_node_name                    text
                , IN        i_top_n_sessions               smallint
                , OUT       o_node_name                    name
                , OUT       o_db_name                      name
                , OUT       o_user_name                    name
                , OUT       o_client_hostname              text
                , OUT       o_session_start_time           timestamp
                , OUT       o_xact_start_time              timestamp
                , OUT       o_waiting                      boolean
                , OUT       o_state                        text
                , OUT       o_query                        text
                , OUT       o_session_total_memory_size    bigint
                , OUT       o_session_used_memory_size     bigint
                , OUT       o_buffer_hits	               bigint
                , OUT       o_disk_reads	               bigint
                , OUT       o_session_buffer_hit_ratio     numeric(5, 2)
                , OUT       o_sorts_in_memory              bigint
                , OUT       o_sorts_in_disk                bigint
                , OUT       o_session_memory_sort_ratio    numeric(5, 2)
				, OUT		o_avg_sql_exec_time			   numeric(15, 3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_node_name                    text;
                            l_error_message				   text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    IF i_top_n_sessions < 0
    THEN
        l_error_message		:= 'ERROR:: Negative value provided for Top N Sessions during generation of session memory statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
    END IF;

    l_node_name   := UPPER(i_node_name);

    IF l_node_name <> 'ALL'
       AND NOT EXISTS ( SELECT 1 FROM pgxc_node
                         WHERE UPPER(node_name)   = l_node_name )
    THEN
        l_error_message		:= 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of session memory statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
    END IF;

    RETURN QUERY
    SELECT scs.o_node_name
         , scs.o_db_name
         , scs.o_user_name
         , scs.o_client_hostname
         , scs.o_session_start_time
         , scs.o_xact_start_time
         , scs.o_waiting
         , scs.o_state
         , scs.o_query
         , scs.o_session_total_memory_size
         , scs.o_session_used_memory_size
         , scs.o_buffer_hits
         , scs.o_disk_reads
         , scs.o_session_buffer_hit_ratio
         , scs.o_sorts_in_memory
         , scs.o_sorts_in_disk
         , scs.o_session_memory_sort_ratio
		 , scs.o_avg_sql_exec_time
      FROM pmk.get_session_memory_stat0(l_node_name, i_top_n_sessions) scs
     ORDER BY scs.o_session_total_memory_size DESC
            , scs.o_session_used_memory_size DESC
			, scs.o_node_name
	 LIMIT i_top_n_sessions;
	 
END;
$$
LANGUAGE plpgsql;

-- This function is used to find TOP N sessions, which do more physical I/O operations.
-- But this function returns Top N sessions from each node.
-- The outer function (get_session_io_stat) return the Top N sessions as the final result.

CREATE OR REPLACE FUNCTION pmk.get_session_io_stat0
                ( IN        i_node_name                  text
                , IN        i_top_n_sessions             smallint
                , OUT       o_node_name                  name
                , OUT       o_db_name                    name
                , OUT       o_user_name                  name
                , OUT       o_client_hostname            text
                , OUT       o_session_start_time         timestamp
                , OUT       o_xact_start_time            timestamp
                , OUT       o_waiting                    boolean
                , OUT       o_state                      text
                , OUT       o_query                      text
                , OUT       o_disk_reads	             bigint
                , OUT       o_read_time                  bigint
                , OUT       o_avg_read_per_sec           numeric(20, 2)
                , OUT       o_avg_read_time              numeric(20, 3)
				, OUT		o_avg_sql_exec_time			 numeric(15, 3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE					    l_node_query                 text;
						    l_execute_query              text;
BEGIN

    FOR i IN ( SELECT node_name
                 FROM pgxc_node nl 
                WHERE UPPER(nl.node_name) = COALESCE(NULLIF(i_node_name, 'ALL'), UPPER(nl.node_name))
             )
	LOOP

        l_node_query := 'WITH sess_stat0 AS 
                            ( SELECT ss.sessid
                                   , ss.statname AS statname
                                   , ss.value AS statvalue
                                FROM pv_session_stat ss
                               WHERE ss.statname      IN ( ''''n_shared_blocks_read'''', ''''n_local_blocks_read''''
														 , ''''n_blocks_read_time'''', ''''n_sql'''' )
                            )
						    , sess_stat1 AS 
						    ( SELECT DISTINCT ss.sessid
								   , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = ss.sessid AND iss.statname = ''''n_shared_blocks_read'''') AS disk_to_shared_buffer
								   , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = ss.sessid AND iss.statname = ''''n_local_blocks_read'''') AS disk_to_local_buffer
								   , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = ss.sessid AND iss.statname = ''''n_blocks_read_time'''') AS read_time
								   , (SELECT statvalue FROM sess_stat0 iss WHERE iss.sessid = ss.sessid AND iss.statname = ''''n_sql'''') AS sql_count 
							    FROM sess_stat0 ss
						    )
						    , sess_stat AS 
						    ( SELECT ss.sessid
                                   , (ss.disk_to_shared_buffer + ss.disk_to_local_buffer) AS disk_reads
								   , ss.read_time
								   , ss.sql_count
                                FROM sess_stat1 ss
							)		
						 	, sess_io_stat AS
							( SELECT ''''' || i.node_name  || '''''::name AS node_name
								   , a.datname::name AS db_name
								   , a.usename::name AS user_name
								   , a.client_hostname
								   , date_trunc(''''second'''', a.backend_start)::timestamp AS session_start_time
								   , date_trunc(''''second'''', a.xact_start)::timestamp AS xact_start_time
								   , a.waiting
								   , a.state, a.query
								   , st.disk_reads
								   , st.read_time
								   , ( st.disk_reads * 1000000.0 / NULLIF(st.read_time, 0) )::numeric(20,2) AS avg_read_per_sec
								   , ( st.read_time * 1.0 / NULLIF(st.disk_reads, 0) )::numeric(20,3) AS avg_read_time
								   , st.sessid
								   , st.sql_count
							    FROM pg_stat_activity a
								   , sess_stat st
							   WHERE a.state	          IN (''''active'''', ''''fastpath function call'''')
								 AND a.pid              = sessionid2pid(st.sessid::cstring)
							   ORDER BY st.disk_reads DESC
									  , st.read_time DESC
							   LIMIT ' || i_top_n_sessions || '
						    )
							SELECT sios.node_name
								 , sios.db_name
								 , sios.user_name
								 , sios.client_hostname
								 , sios.session_start_time
								 , sios.xact_start_time
								 , sios.waiting
								 , sios.state
								 , sios.query
								 , sios.disk_reads
								 , sios.read_time
								 , sios.avg_read_per_sec
								 , sios.avg_read_time
								 , ( ss.value / (NULLIF(sios.sql_count, 0) * 1000.0) )::numeric(15, 3) AS avg_sql_exec_time
							  FROM sess_io_stat sios
								 , pv_session_time ss
							 WHERE ss.sessid		  			=  sios.sessid
							   AND ss.stat_name		  			=  ''''EXECUTION_TIME''''';

        l_execute_query := 'EXECUTE DIRECT ON ( ' || i.node_name || ' ) ''' || l_node_query || '''';

		RETURN QUERY
		EXECUTE l_execute_query;

    END LOOP;

END;
$$
LANGUAGE plpgsql;

-- This function is used to find TOP n sessions, which do more physical I/O operations

CREATE OR REPLACE FUNCTION pmk.get_session_io_stat 
                ( IN        i_node_name                  text
                , IN        i_top_n_sessions             smallint
                , OUT       o_node_name                  name
                , OUT       o_db_name                    name
                , OUT       o_user_name                  name
                , OUT       o_client_hostname            text
                , OUT       o_session_start_time         timestamp
                , OUT       o_xact_start_time            timestamp
                , OUT       o_waiting                    boolean
                , OUT       o_state                      text
                , OUT       o_query                      text
                , OUT       o_disk_reads	             bigint
                , OUT       o_read_time                  bigint
                , OUT       o_avg_read_per_sec           numeric(20, 2)
                , OUT       o_avg_read_time              numeric(20, 3)
				, OUT		o_avg_sql_exec_time			 numeric(15, 3)
                ) 
RETURNS SETOF record 
AS
$$
DECLARE                     l_node_name                  text;
                            l_error_message				 text;
BEGIN

	l_error_message := pmk.check_node_type();
	
    IF  l_error_message IS NOT NULL
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
	END IF;

	l_error_message := pmk.check_pmk_enabled();

    IF l_error_message IS NOT NULL 
	THEN
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN;
	END IF;

    IF i_top_n_sessions < 0
    THEN
        l_error_message		:= 'ERROR:: Negative value provided for Top N Sessions during generation of session I/O statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
    END IF;

    l_node_name   := UPPER(i_node_name);

    IF l_node_name <> 'ALL'
       AND NOT EXISTS ( SELECT 1 FROM pgxc_node
                         WHERE UPPER(node_name)   = l_node_name )
    THEN
        l_error_message		:= 'ERROR:: Invalid node name ("' || i_node_name || '") provided during generation of session I/O statistics ...';
		DBMS_OUTPUT.PUT_LINE(l_error_message);
		RETURN; 
    END IF;

    RETURN QUERY
    SELECT scs.o_node_name
         , scs.o_db_name
         , scs.o_user_name
         , scs.o_client_hostname
         , scs.o_session_start_time
         , scs.o_xact_start_time
         , scs.o_waiting
         , scs.o_state
         , scs.o_query
         , scs.o_disk_reads
         , scs.o_read_time
         , scs.o_avg_read_per_sec
         , scs.o_avg_read_time
		 , scs.o_avg_sql_exec_time
      FROM pmk.get_session_io_stat0(l_node_name, i_top_n_sessions) scs
     ORDER BY scs.o_disk_reads DESC
            , scs.o_read_time DESC
			, scs.o_node_name
	 LIMIT i_top_n_sessions;

END;
$$
LANGUAGE plpgsql;

-- It ends the transaction started in the begining of PMK installationCOMMIT;
