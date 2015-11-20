DECLARE
	result                BOOL;
BEGIN
	FOR i in (select * from pg_stat_activity where query like 'select case (select pgxc_lock_for_backup()) when true then (select pg_sleep(%)) end;')
	LOOP
		RAISE INFO 'datid: %, datname: %, pid: %', i.datid, i.datname, i.pid;
		RAISE INFO 'usesysid: %, usename: %, application_name: %', i.usesysid, i.usename, i.application_name;
		RAISE INFO 'client_addr: %, client_hostname: %, client_port: %', i.client_addr, i.client_hostname, i.client_port;
		RAISE INFO 'backend_start: %, xact_start: %', i.backend_start, i.xact_start;
		RAISE INFO 'query_start: %, state_change: %', i.query_start, i.state_change;
		RAISE INFO 'waiting: %, state: %', i.waiting, i.state;
		RAISE INFO 'query: %', i.query;
		result := false;
		select pg_cancel_backend(i.pid) into result;
		RAISE INFO 'cancel command result: %', result;
	END LOOP;
END;
/