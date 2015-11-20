/*
 * PostgreSQL System Views
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/backend/catalog/system_views.sql
 */
CREATE SCHEMA SYS;                                                                                  
CREATE OR REPLACE VIEW PG_CATALOG.DUAL AS (SELECT 'X'::TEXT AS DUMMY);                              
GRANT SELECT ON TABLE DUAL TO PUBLIC;      
CREATE VIEW pg_roles AS
    SELECT
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcatupdate,
        rolcanlogin,
        rolreplication,
        rolauditadmin,
        rolsystemadmin,
        rolconnlimit,
        '********'::text as rolpassword,
        rolvaliduntil,
        setconfig as rolconfig,
        pg_authid.oid
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0);

CREATE VIEW pg_shadow AS
    SELECT
        rolname AS usename,
        pg_authid.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        rolpassword AS passwd,
        rolvaliduntil::abstime AS valuntil,
        setconfig AS useconfig
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    WHERE rolcanlogin;

REVOKE ALL on pg_shadow FROM public;

CREATE VIEW pg_group AS
    SELECT
        rolname AS groname,
        oid AS grosysid,
        ARRAY(SELECT member FROM pg_auth_members WHERE roleid = oid) AS grolist
    FROM pg_authid
    WHERE NOT rolcanlogin;

CREATE VIEW pg_user AS
    SELECT
        usename,
        usesysid,
        usecreatedb,
        usesuper,
        usecatupd,
        userepl,
        '********'::text as passwd,
        valuntil,
        useconfig
    FROM pg_shadow;

CREATE VIEW pg_rules AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        R.rulename AS rulename,
        pg_get_ruledef(R.oid) AS definition
    FROM (pg_rewrite R JOIN pg_class C ON (C.oid = R.ev_class))
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE R.rulename != '_RETURN';

CREATE VIEW pg_views AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS viewname,
        pg_get_userbyid(C.relowner) AS viewowner,
        pg_get_viewdef(C.oid) AS definition
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'v';

CREATE VIEW pg_tables AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        pg_get_userbyid(C.relowner) AS tableowner,
        T.spcname AS tablespace,
        C.relhasindex AS hasindexes,
        C.relhasrules AS hasrules,
        C.relhastriggers AS hastriggers
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
    WHERE C.relkind = 'r';

CREATE VIEW pg_indexes AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        I.relname AS indexname,
        T.spcname AS tablespace,
        pg_get_indexdef(I.oid) AS indexdef
    FROM pg_index X JOIN pg_class C ON (C.oid = X.indrelid)
         JOIN pg_class I ON (I.oid = X.indexrelid)
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = I.reltablespace)
    WHERE C.relkind = 'r' AND I.relkind = 'i';

CREATE VIEW pg_stats AS
    SELECT
        nspname AS schemaname,
        relname AS tablename,
        attname AS attname,
        stainherit AS inherited,
        stanullfrac AS null_frac,
        stawidth AS avg_width,
        stadistinct AS n_distinct,
        CASE
            WHEN stakind1 = 1 THEN stavalues1
            WHEN stakind2 = 1 THEN stavalues2
            WHEN stakind3 = 1 THEN stavalues3
            WHEN stakind4 = 1 THEN stavalues4
            WHEN stakind5 = 1 THEN stavalues5
        END AS most_common_vals,
        CASE
            WHEN stakind1 = 1 THEN stanumbers1
            WHEN stakind2 = 1 THEN stanumbers2
            WHEN stakind3 = 1 THEN stanumbers3
            WHEN stakind4 = 1 THEN stanumbers4
            WHEN stakind5 = 1 THEN stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN stakind1 = 2 THEN stavalues1
            WHEN stakind2 = 2 THEN stavalues2
            WHEN stakind3 = 2 THEN stavalues3
            WHEN stakind4 = 2 THEN stavalues4
            WHEN stakind5 = 2 THEN stavalues5
        END AS histogram_bounds,
        CASE
            WHEN stakind1 = 3 THEN stanumbers1[1]
            WHEN stakind2 = 3 THEN stanumbers2[1]
            WHEN stakind3 = 3 THEN stanumbers3[1]
            WHEN stakind4 = 3 THEN stanumbers4[1]
            WHEN stakind5 = 3 THEN stanumbers5[1]
        END AS correlation,
        CASE
            WHEN stakind1 = 4 THEN stavalues1
            WHEN stakind2 = 4 THEN stavalues2
            WHEN stakind3 = 4 THEN stavalues3
            WHEN stakind4 = 4 THEN stavalues4
            WHEN stakind5 = 4 THEN stavalues5
        END AS most_common_elems,
        CASE
            WHEN stakind1 = 4 THEN stanumbers1
            WHEN stakind2 = 4 THEN stanumbers2
            WHEN stakind3 = 4 THEN stanumbers3
            WHEN stakind4 = 4 THEN stanumbers4
            WHEN stakind5 = 4 THEN stanumbers5
        END AS most_common_elem_freqs,
        CASE
            WHEN stakind1 = 5 THEN stanumbers1
            WHEN stakind2 = 5 THEN stanumbers2
            WHEN stakind3 = 5 THEN stanumbers3
            WHEN stakind4 = 5 THEN stanumbers4
            WHEN stakind5 = 5 THEN stanumbers5
        END AS elem_count_histogram
    FROM pg_statistic s JOIN pg_class c ON (c.oid = s.starelid AND s.starelkind='c')
         JOIN pg_attribute a ON (c.oid = attrelid AND attnum = s.staattnum)
         LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE NOT attisdropped AND has_column_privilege(c.oid, a.attnum, 'select');

REVOKE ALL on pg_statistic FROM public;

-- views about partitioned table, partitioned index, table partition and index partition

-- DBA_PART_TABLES
-- DBA_TAB_PARTITIONS
-- DBA_PART_INDEXES
-- DBA_IND_PARTITIONS

-- USER_PART_TABLES
-- USER_TAB_PARTITIONS
-- USER_PART_INDEXES
-- USER_IND_PARTITIONS

CREATE OR REPLACE VIEW DBA_PART_TABLES AS

	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64)) AS TABLE_NAME ,

	----------------------------------------------------------------------
		CASE
			when p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE,
	----------------------------------------------------------------------

		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = c.oid AND ps.parttype = 'p') AS PARTITION_COUNT,

	----------------------------------------------------------------------
		CASE
			WHEN c.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE c.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,

	----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
	FROM
		PG_CLASS c , PG_PARTITION p , pg_authid a , pg_namespace n
	WHERE
		c.parttype = 'p'  AND  -- it is a partitioned table in pg_class
		c.relkind = 'r'  AND  	-- normal table ,it can be ignore, all partitioned table is  normal table
		c.oid = p.parentid	AND  -- join condition
		p.parttype = 'r'   	AND		-- it is a partitioned table in pg_partition
		c.relowner = a.oid	AND		-- the owner of table
		c.relnamespace = n.oid		-- namespace
	ORDER BY TABLE_OWNER,SCHEMA,PARTITIONING_TYPE, PARTITION_COUNT,DEF_TABLESPACE_NAME , PARTITIONING_KEY_COUNT;

CREATE OR REPLACE VIEW USER_PART_TABLES AS
	SELECT
		*
	FROM DBA_PART_TABLES
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW DBA_TAB_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64))  AS TABLE_NAME,
	----------------------------------------------------------------------
		CAST(p.relname AS varchar2(64)) AS PARTITION_NAME,
	----------------------------------------------------------------------
		array_to_string(p.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
	----------------------------------------------------------------------
		CASE
			WHEN p.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE (SELECT spc.spcname FROM pg_tablespace spc WHERE p.reltablespace = spc.oid)
		END
		AS TABLESPACE_NAME
	----------------------------------------------------------------------
	FROM
		pg_class c , pg_partition p , pg_authid a,  pg_namespace n
	----------------------------------------------------------------------
	WHERE
		p.parentid = c.oid AND
	----------------------------------------------------------------------
		p.parttype = 'p' AND
		c.relowner = a.oid AND
		c.relnamespace = n.oid

	----------------------------------------------------------------------
	ORDER BY TABLE_OWNER,SCHEMA,TABLE_NAME, PARTITION_NAME, HIGH_VALUE, TABLESPACE_NAME;

CREATE OR REPLACE VIEW USER_TAB_PARTITIONS AS
	SELECT
		*
	FROM DBA_TAB_PARTITIONS
	WHERE TABLE_OWNER	= CURRENT_USER;
	
CREATE OR REPLACE VIEW DBA_PART_INDEXES AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME                 ,
		----------------------------------------------------------------------
		CAST(ct.relname AS varchar2(64)) AS TABLE_NAME                 ,
		----------------------------------------------------------------------
		CASE
			WHEN p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE      ,
		----------------------------------------------------------------------
		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = ct.oid AND ps.parttype = 'p')
		AS PARTITION_COUNT    ,
		----------------------------------------------------------------------
		CASE
			WHEN ci.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE ci.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
		----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
	FROM
		pg_class ct , --table
		pg_class ci,  --index
		pg_index i,	  -- i.INDEXRELID is index     i.INDRELID is table
		pg_partition p,  -- partition attribute of table
		pg_authid a,
		pg_namespace n
	WHERE
		ci.parttype = 'p' AND
		ci.relkind = 'i'	AND   --find all the local partitioned index

		ci.oid = i.INDEXRELID 	AND --find table be indexed
		ct.oid = i.INDRELID		AND

		ct.oid = p.parentid		AND -- find the attribute of partitioned table
		p.parttype = 'r' 		AND

		ci.relowner = a.oid		AND
		ci.relnamespace = n.oid
	ORDER BY TABLE_NAME ,SCHEMA, INDEX_NAME, DEF_TABLESPACE_NAME,INDEX_OWNER;

CREATE OR REPLACE VIEW USER_PART_INDEXES AS
	SELECT
		*
	FROM DBA_PART_INDEXES
	WHERE INDEX_OWNER	= CURRENT_USER;
	
CREATE OR REPLACE VIEW DBA_IND_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME,
		----------------------------------------------------------------------
		CAST(pi.relname AS varchar2(64)) AS PARTITION_NAME,
		----------------------------------------------------------------------
		pi.indisusable AS INDEX_PARTITION_USABLE,
		----------------------------------------------------------------------
		array_to_string(pt.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
		----------------------------------------------------------------------
		CASE
			WHEN pi.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE pi.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME
		----------------------------------------------------------------------
	FROM
		pg_class ci , 	--the local partitioned index
		pg_partition pi,-- the partition index
		pg_partition pt,-- the partition indexed by partition index
		pg_authid a,
		pg_namespace n

	WHERE
		ci.parttype  = 'p'  AND -- the partitioned relation
		ci.relkind   = 'i'  AND -- choose index of partitioned relation
		ci.oid = pi.parentid 	AND --the index partitons
		pi.indextblid = pt.oid  AND-- partition indexed by partition index
		ci.relowner = a.oid	AND
		ci.relnamespace = n.oid
	ORDER BY INDEX_OWNER,SCHEMA,INDEX_NAME, PARTITION_NAME, HIGH_VALUE, DEF_TABLESPACE_NAME;

CREATE OR REPLACE VIEW USER_IND_PARTITIONS AS
	SELECT
		*
	FROM DBA_IND_PARTITIONS
	WHERE INDEX_OWNER	= CURRENT_USER;

-- under SYS. schema 
CREATE OR REPLACE VIEW SYS.DBA_PART_TABLES AS

	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64)) AS TABLE_NAME ,

	----------------------------------------------------------------------
		CASE
			when p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE,
	----------------------------------------------------------------------

		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = c.oid AND ps.parttype = 'p') AS PARTITION_COUNT,

	----------------------------------------------------------------------
		CASE
			WHEN c.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE c.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,

	----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
	FROM
		PG_CLASS c , PG_PARTITION p , pg_authid a , pg_namespace n
	WHERE
		c.parttype = 'p'  AND  -- it is a partitioned table in pg_class
		c.relkind = 'r'  AND  	-- normal table ,it can be ignore, all partitioned table is  normal table
		c.oid = p.parentid	AND  -- join condition
		p.parttype = 'r'   	AND		-- it is a partitioned table in pg_partition
		c.relowner = a.oid	AND		-- the owner of table
		c.relnamespace = n.oid		-- namespace
	ORDER BY TABLE_OWNER,SCHEMA,PARTITIONING_TYPE, PARTITION_COUNT,DEF_TABLESPACE_NAME , PARTITIONING_KEY_COUNT;

CREATE OR REPLACE VIEW SYS.USER_PART_TABLES AS
	SELECT
		*
	FROM DBA_PART_TABLES
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW SYS.DBA_TAB_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64))  AS TABLE_NAME,
	----------------------------------------------------------------------
		CAST(p.relname AS varchar2(64)) AS PARTITION_NAME,
	----------------------------------------------------------------------
		array_to_string(p.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
	----------------------------------------------------------------------
		CASE
			WHEN p.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE (SELECT spc.spcname FROM pg_tablespace spc WHERE p.reltablespace = spc.oid)
		END
		AS TABLESPACE_NAME
	----------------------------------------------------------------------
	FROM
		pg_class c , pg_partition p , pg_authid a,  pg_namespace n
	----------------------------------------------------------------------
	WHERE
		p.parentid = c.oid AND
	----------------------------------------------------------------------
		p.parttype = 'p' AND
		c.relowner = a.oid AND
		c.relnamespace = n.oid

	----------------------------------------------------------------------
	ORDER BY TABLE_OWNER,SCHEMA,TABLE_NAME, PARTITION_NAME, HIGH_VALUE, TABLESPACE_NAME;

CREATE OR REPLACE VIEW SYS.USER_TAB_PARTITIONS AS
	SELECT
		*
	FROM DBA_TAB_PARTITIONS
	WHERE TABLE_OWNER	= CURRENT_USER;
	
CREATE OR REPLACE VIEW SYS.DBA_PART_INDEXES AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME                 ,
		----------------------------------------------------------------------
		CAST(ct.relname AS varchar2(64)) AS TABLE_NAME                 ,
		----------------------------------------------------------------------
		CASE
			WHEN p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE      ,
		----------------------------------------------------------------------
		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = ct.oid AND ps.parttype = 'p')
		AS PARTITION_COUNT    ,
		----------------------------------------------------------------------
		CASE
			WHEN ci.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE ci.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
		----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
	FROM
		pg_class ct , --table
		pg_class ci,  --index
		pg_index i,	  -- i.INDEXRELID is index     i.INDRELID is table
		pg_partition p,  -- partition attribute of table
		pg_authid a,
		pg_namespace n
	WHERE
		ci.parttype = 'p' AND
		ci.relkind = 'i'	AND   --find all the local partitioned index

		ci.oid = i.INDEXRELID 	AND --find table be indexed
		ct.oid = i.INDRELID		AND

		ct.oid = p.parentid		AND -- find the attribute of partitioned table
		p.parttype = 'r' 		AND

		ci.relowner = a.oid		AND
		ci.relnamespace = n.oid
	ORDER BY TABLE_NAME ,SCHEMA, INDEX_NAME, DEF_TABLESPACE_NAME,INDEX_OWNER;

CREATE OR REPLACE VIEW SYS.USER_PART_INDEXES AS
	SELECT
		*
	FROM DBA_PART_INDEXES
	WHERE INDEX_OWNER	= CURRENT_USER;
	
CREATE OR REPLACE VIEW SYS.DBA_IND_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME,
		----------------------------------------------------------------------
		CAST(pi.relname AS varchar2(64)) AS PARTITION_NAME,
		----------------------------------------------------------------------
		pi.indisusable AS INDEX_PARTITION_USABLE,
		----------------------------------------------------------------------
		array_to_string(pt.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
		----------------------------------------------------------------------
		CASE
			WHEN pi.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::TEXT
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE pi.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME
		----------------------------------------------------------------------
	FROM
		pg_class ci , 	--the local partitioned index
		pg_partition pi,-- the partition index
		pg_partition pt,-- the partition indexed by partition index
		pg_authid a,
		pg_namespace n

	WHERE
		ci.parttype  = 'p'  AND -- the partitioned relation
		ci.relkind   = 'i'  AND -- choose index of partitioned relation
		ci.oid = pi.parentid 	AND --the index partitons
		pi.indextblid = pt.oid  AND-- partition indexed by partition index
		ci.relowner = a.oid	AND
		ci.relnamespace = n.oid
	ORDER BY INDEX_OWNER,SCHEMA,INDEX_NAME, PARTITION_NAME, HIGH_VALUE, DEF_TABLESPACE_NAME;

CREATE OR REPLACE VIEW SYS.USER_IND_PARTITIONS AS
	SELECT
		*
	FROM DBA_IND_PARTITIONS
	WHERE INDEX_OWNER	= CURRENT_USER;


CREATE VIEW pg_locks AS
    SELECT * FROM pg_lock_status() AS L;

CREATE VIEW pg_cursors AS
    SELECT * FROM pg_cursor() AS C;

CREATE VIEW pg_available_extensions AS
    SELECT E.name, E.default_version, X.extversion AS installed_version,
           E.comment
      FROM pg_available_extensions() AS E
           LEFT JOIN pg_extension AS X ON E.name = X.extname;

CREATE VIEW pg_available_extension_versions AS
    SELECT E.name, E.version, (X.extname IS NOT NULL) AS installed,
           E.superuser, E.relocatable, E.schema, E.requires, E.comment
      FROM pg_available_extension_versions() AS E
           LEFT JOIN pg_extension AS X
             ON E.name = X.extname AND E.version = X.extversion;

CREATE VIEW pg_prepared_xacts AS
    SELECT P.transaction, P.gid, P.prepared,
           U.rolname AS owner, D.datname AS database
    FROM pg_prepared_xact() AS P
         LEFT JOIN pg_authid U ON P.ownerid = U.oid
         LEFT JOIN pg_database D ON P.dbid = D.oid;

CREATE VIEW pg_prepared_statements AS
    SELECT * FROM pg_prepared_statement() AS P;

CREATE VIEW pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'S' THEN 'sequence'::text
		 WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
	     END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'column'::text AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
	     END || '.' || att.attname AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_attribute att
	     ON rel.oid = att.attrelid AND l.objsubid = att.attnum
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid != 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN pro.proisagg = true THEN 'aggregate'::text
	     WHEN pro.proisagg = false THEN 'function'::text
	END AS objtype,
	pro.pronamespace AS objnamespace,
	CASE WHEN pg_function_is_visible(pro.oid)
	     THEN quote_ident(pro.proname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(pro.proname)
	END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_proc pro ON l.classoid = pro.tableoid AND l.objoid = pro.oid
	JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN typ.typtype = 'd' THEN 'domain'::text
	ELSE 'type'::text END AS objtype,
	typ.typnamespace AS objnamespace,
	CASE WHEN pg_type_is_visible(typ.oid)
	THEN quote_ident(typ.typname)
	ELSE quote_ident(nsp.nspname) || '.' || quote_ident(typ.typname)
	END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_type typ ON l.classoid = typ.tableoid AND l.objoid = typ.oid
	JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'large object'::text AS objtype,
	NULL::oid AS objnamespace,
	l.objoid::text AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
WHERE
	l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'language'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(lan.lanname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_language lan ON l.classoid = lan.tableoid AND l.objoid = lan.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'schema'::text AS objtype,
	nsp.oid AS objnamespace,
	quote_ident(nsp.nspname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'database'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(dat.datname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid = dat.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'tablespace'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(spc.spcname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid = spc.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'role'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(rol.rolname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;

CREATE VIEW pg_settings AS
    SELECT * FROM pg_show_all_settings() AS A;

CREATE RULE pg_settings_u AS
    ON UPDATE TO pg_settings
    WHERE new.name = old.name DO
    SELECT set_config(old.name, new.setting, 'f');

CREATE RULE pg_settings_n AS
    ON UPDATE TO pg_settings
    DO INSTEAD NOTHING;

GRANT SELECT, UPDATE ON pg_settings TO PUBLIC;

CREATE VIEW pg_timezone_abbrevs AS
    SELECT * FROM pg_timezone_abbrevs();

CREATE VIEW pg_timezone_names AS
    SELECT * FROM pg_timezone_names();
    
CREATE VIEW pg_control_group_config AS
    SELECT * FROM pg_control_group_config();

-- Statistics views

CREATE VIEW pg_stat_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_stat_get_numscans(C.oid) AS seq_scan,
            pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
            sum(pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
            sum(pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
            pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
            pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
            pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
            pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
            pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
            pg_stat_get_live_tuples(C.oid) AS n_live_tup,
            pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
            pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
            pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
            pg_stat_get_last_analyze_time(C.oid) as last_analyze,
            pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
            pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
            pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
            pg_stat_get_analyze_count(C.oid) AS analyze_count,
            pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE VIEW pg_stat_xact_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_stat_get_xact_numscans(C.oid) AS seq_scan,
            pg_stat_get_xact_tuples_returned(C.oid) AS seq_tup_read,
            sum(pg_stat_get_xact_numscans(I.indexrelid))::bigint AS idx_scan,
            sum(pg_stat_get_xact_tuples_fetched(I.indexrelid))::bigint +
            pg_stat_get_xact_tuples_fetched(C.oid) AS idx_tup_fetch,
            pg_stat_get_xact_tuples_inserted(C.oid) AS n_tup_ins,
            pg_stat_get_xact_tuples_updated(C.oid) AS n_tup_upd,
            pg_stat_get_xact_tuples_deleted(C.oid) AS n_tup_del,
            pg_stat_get_xact_tuples_hot_updated(C.oid) AS n_tup_hot_upd
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE VIEW pg_stat_sys_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_xact_sys_tables AS
    SELECT * FROM pg_stat_xact_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_user_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_stat_xact_user_tables AS
    SELECT * FROM pg_stat_xact_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_stat_get_blocks_fetched(C.oid) -
                    pg_stat_get_blocks_hit(C.oid) AS heap_blks_read,
            pg_stat_get_blocks_hit(C.oid) AS heap_blks_hit,
            sum(pg_stat_get_blocks_fetched(I.indexrelid) -
                    pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_read,
            sum(pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_hit,
            pg_stat_get_blocks_fetched(T.oid) -
                    pg_stat_get_blocks_hit(T.oid) AS toast_blks_read,
            pg_stat_get_blocks_hit(T.oid) AS toast_blks_hit,
            pg_stat_get_blocks_fetched(X.oid) -
                    pg_stat_get_blocks_hit(X.oid) AS tidx_blks_read,
            pg_stat_get_blocks_hit(X.oid) AS tidx_blks_hit
    FROM pg_class C LEFT JOIN
            pg_index I ON C.oid = I.indrelid LEFT JOIN
            pg_class T ON C.reltoastrelid = T.oid LEFT JOIN
            pg_class X ON T.reltoastidxid = X.oid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
    GROUP BY C.oid, N.nspname, C.relname, T.oid, X.oid;

CREATE VIEW pg_statio_sys_tables AS
    SELECT * FROM pg_statio_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_tables AS
    SELECT * FROM pg_statio_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_stat_all_indexes AS
    SELECT
            C.oid AS relid,
            I.oid AS indexrelid,
            N.nspname AS schemaname,
            C.relname AS relname,
            I.relname AS indexrelname,
            pg_stat_get_numscans(I.oid) AS idx_scan,
            pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
            pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
    FROM pg_class C JOIN
            pg_index X ON C.oid = X.indrelid JOIN
            pg_class I ON I.oid = X.indexrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't');

CREATE VIEW pg_stat_sys_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_stat_user_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_indexes AS
    SELECT
            C.oid AS relid,
            I.oid AS indexrelid,
            N.nspname AS schemaname,
            C.relname AS relname,
            I.relname AS indexrelname,
            pg_stat_get_blocks_fetched(I.oid) -
                    pg_stat_get_blocks_hit(I.oid) AS idx_blks_read,
            pg_stat_get_blocks_hit(I.oid) AS idx_blks_hit
    FROM pg_class C JOIN
            pg_index X ON C.oid = X.indrelid JOIN
            pg_class I ON I.oid = X.indexrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't');

CREATE VIEW pg_statio_sys_indexes AS
    SELECT * FROM pg_statio_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_indexes AS
    SELECT * FROM pg_statio_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_statio_all_sequences AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_stat_get_blocks_fetched(C.oid) -
                    pg_stat_get_blocks_hit(C.oid) AS blks_read,
            pg_stat_get_blocks_hit(C.oid) AS blks_hit
    FROM pg_class C
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S';

CREATE VIEW pg_statio_sys_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW pg_statio_user_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue, 
            S.state,
            S.query
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid;

CREATE VIEW pv_os_run_info AS SELECT * FROM pv_os_run_info();
CREATE VIEW pv_session_memory_detail AS SELECT * FROM pv_session_memory_detail();
CREATE VIEW pv_session_time AS SELECT * FROM pv_session_time();
CREATE VIEW pv_redo_stat AS SELECT * FROM pg_stat_get_redo_stat();
CREATE VIEW pv_session_stat AS SELECT * FROM pv_session_stat();
CREATE VIEW pv_file_stat AS SELECT * FROM pg_stat_get_file_stat();

CREATE VIEW pg_stat_replication AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
            S.pid = W.sender_pid;
            
CREATE VIEW pg_replication_slots AS
    SELECT
            L.slot_name,
            L.slot_type,
            L.datoid,
            D.datname AS database,
            L.active,
            L.xmin,
            L.restart_lsn,
            L.dummy_standby
    FROM pg_get_replication_slots() AS L
            LEFT JOIN pg_database D ON (L.datoid = D.oid);


CREATE VIEW pg_stat_database AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_numbackends(D.oid) AS numbackends,
            pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
            pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
            pg_stat_get_db_blocks_fetched(D.oid) -
                    pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
            pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
            pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
            pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
            pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
            pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
            pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
            pg_stat_get_db_conflict_all(D.oid) AS conflicts,
            pg_stat_get_db_temp_files(D.oid) AS temp_files,
            pg_stat_get_db_temp_bytes(D.oid) AS temp_bytes,
            pg_stat_get_db_deadlocks(D.oid) AS deadlocks,
            pg_stat_get_db_blk_read_time(D.oid) AS blk_read_time,
            pg_stat_get_db_blk_write_time(D.oid) AS blk_write_time,
            pg_stat_get_mem_mbytes_reserved(D.oid) as mem_mbytes_reserved, 
            pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
    FROM pg_database D;

CREATE VIEW pg_stat_database_conflicts AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
            pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
            pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
            pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
            pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
    FROM pg_database D;

CREATE VIEW pg_stat_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_stat_get_function_calls(P.oid) AS calls,
            pg_stat_get_function_total_time(P.oid) AS total_time,
            pg_stat_get_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_stat_get_function_calls(P.oid) IS NOT NULL;

CREATE VIEW pg_stat_xact_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_stat_get_xact_function_calls(P.oid) AS calls,
            pg_stat_get_xact_function_total_time(P.oid) AS total_time,
            pg_stat_get_xact_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_stat_get_xact_function_calls(P.oid) IS NOT NULL;

CREATE VIEW pg_stat_bgwriter AS
    SELECT
        pg_stat_get_bgwriter_timed_checkpoints() AS checkpoints_timed,
        pg_stat_get_bgwriter_requested_checkpoints() AS checkpoints_req,
        pg_stat_get_checkpoint_write_time() AS checkpoint_write_time,
        pg_stat_get_checkpoint_sync_time() AS checkpoint_sync_time,
        pg_stat_get_bgwriter_buf_written_checkpoints() AS buffers_checkpoint,
        pg_stat_get_bgwriter_buf_written_clean() AS buffers_clean,
        pg_stat_get_bgwriter_maxwritten_clean() AS maxwritten_clean,
        pg_stat_get_buf_written_backend() AS buffers_backend,
        pg_stat_get_buf_fsync_backend() AS buffers_backend_fsync,
        pg_stat_get_buf_alloc() AS buffers_alloc,
        pg_stat_get_bgwriter_stat_reset_time() AS stats_reset;

CREATE VIEW pg_user_mappings AS
    SELECT
        U.oid       AS umid,
        S.oid       AS srvid,
        S.srvname   AS srvname,
        U.umuser    AS umuser,
        CASE WHEN U.umuser = 0 THEN
            'public'
        ELSE
            A.rolname
        END AS usename,
        CASE WHEN pg_has_role(S.srvowner, 'USAGE') OR has_server_privilege(S.oid, 'USAGE') THEN
            U.umoptions
        ELSE
            NULL
        END AS umoptions
    FROM pg_user_mapping U
         LEFT JOIN pg_authid A ON (A.oid = U.umuser) JOIN
        pg_foreign_server S ON (U.umserver = S.oid);
/*  adapt oracle's V$SESSION: display session information for each current session  */
CREATE VIEW V$SESSION AS
	SELECT
		sa.pid AS SID,
		0::integer AS SERIAL#,
		sa.usesysid AS USER#,
		ad.rolname AS USERNAME
	FROM pg_stat_get_activity(NULL) AS sa
	LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
	WHERE sa.application_name <> 'JobScheduler';


/*  adapt oracle's V$SESSION_LONGOPS: displays the status of various operations */
	CREATE VIEW V$SESSION_LONGOPS AS
		SELECT
			sa.pid AS SID,
			0::integer AS SERIAL#,
			NULL::integer AS SOFAR,
			NULL::integer AS TOTALWORK
		FROM pg_stat_activity sa
		WHERE sa.application_name <> 'JobScheduler';


REVOKE ALL on pg_user_mapping FROM public;

CREATE OR REPLACE VIEW PG_CATALOG.DUAL AS (SELECT 'X'::TEXT AS DUMMY);
GRANT SELECT ON TABLE DUAL TO PUBLIC;

-- these functions are added for supporting default format transformation
CREATE OR REPLACE FUNCTION to_char(NUMERIC)
RETURNS VARCHAR2
AS $$ SELECT CAST(numeric_out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(INT2)
RETURNS VARCHAR2
AS $$ SELECT CAST(int2out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(INT4)
RETURNS VARCHAR2
AS $$  SELECT CAST(int4out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(INT8)
RETURNS VARCHAR2
AS $$ SELECT CAST(int8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(FLOAT4)
RETURNS VARCHAR2
AS $$ SELECT CAST(float4out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(FLOAT8)
RETURNS VARCHAR2
AS $$ SELECT CAST(float8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_char(TEXT)
RETURNS TEXT
AS $$ SELECT $1 $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_number(TEXT)
RETURNS NUMERIC
AS $$ SELECT numeric_in(textout($1), 0::Oid, -1) $$
LANGUAGE SQL  STRICT IMMUTABLE;

create schema dbms_output;

create or replace function dbms_output.enable (integer default 20000)
returns void
as '$libdir/plpgsql','dbms_output_enable'
language c strict volatile;

create or replace function dbms_output.put (text)
returns void
as '$libdir/plpgsql','dbms_output_put'
language c strict volatile;

create or replace function dbms_output.put_line	(text)
returns	void
as '$libdir/plpgsql','dbms_output_put_line'
language c strict volatile;

grant usage	on schema dbms_output to public;

create schema dbms_lob;

create or replace function dbms_lob.getlength (blob)
returns	integer
as '$libdir/plpgsql','dbms_lob_getlength'
language c strict volatile;
/* Added dbms_lob.getlength for text type argument other than blob */
create or replace function dbms_lob.getlength (text)
returns	integer
as '$libdir/plpgsql','dbms_lob_getlength_char'
language c strict volatile;

create or replace function dbms_lob.read (blob,integer,integer,out raw)
returns	raw
as '$libdir/plpgsql','dbms_lob_read'
language c strict volatile;

create or replace function dbms_lob.write (inout blob,integer,integer,raw)
as '$libdir/plpgsql','dbms_lob_write'
language c strict volatile;

create or replace function dbms_lob.writeappend (inout blob,integer,raw)
as '$libdir/plpgsql','dbms_lob_writeappend'
language c strict volatile;

create or replace function dbms_lob.open (blob)
returns void 
as '$libdir/plpgsql','dbms_lob_open'
language c strict volatile;

create or replace function dbms_lob.close (blob)
returns void 
as '$libdir/plpgsql','dbms_lob_close'
language c strict volatile;


create or replace function dbms_lob.erase (inout blob, inout integer, integer default 1)
returns record
as '$libdir/plpgsql','dbms_lob_erase'
language c strict volatile;


create or replace function dbms_lob.copy (inout blob, blob, integer, integer default 1,integer default 1)
as '$libdir/plpgsql','dbms_lob_copy'
language c strict volatile;


grant usage	on schema dbms_lob to public;

create schema dbms_random;

create or replace function dbms_random.seed	(integer)
returns	integer
as '$libdir/plpgsql','dbms_random_seed'
language c strict volatile;

create or replace function dbms_random.value (number,number)
returns	number
as '$libdir/plpgsql','dbms_random_value'
language c strict volatile;

grant usage	on schema dbms_random to public;

create schema utl_raw;

create or replace function utl_raw.length(raw)
returns	integer
as '$libdir/plpgsql','utl_raw_length'
language c strict volatile;

create or replace function utl_raw.cast_to_raw(varchar2)
returns	raw
as '$libdir/plpgsql','utl_raw_cast_to_raw'
language c strict volatile;

create or replace function utl_raw.cast_from_binary_integer	
(n in integer,
endianess in integer default 1)
returns	raw
as '$libdir/plpgsql','utl_raw_cast_from_binary_integer'
language c strict volatile;

create or replace function utl_raw.cast_to_binary_integer 
(r in raw,
endianess in integer default 1)
returns	integer
as '$libdir/plpgsql','utl_raw_cast_to_binary_integer'
language c strict volatile;

grant usage	on schema utl_raw to public;

CREATE CAST (VARCHAR2 AS RAW) WITH FUNCTION hextoraw(text) AS IMPLICIT;


--
-- We have a few function definitions in here, too.
-- At some point there might be enough to justify breaking them out into
-- a separate "system_functions.sql" file.
--

-- Tsearch debug function.  Defined here because it'd be pretty unwieldy
-- to put it into pg_proc.h

CREATE FUNCTION ts_debug(IN config regconfig, IN document text,
    OUT alias text,
    OUT description text,
    OUT token text,
    OUT dictionaries regdictionary[],
    OUT dictionary regdictionary,
    OUT lexemes text[])
RETURNS SETOF record AS
$$
SELECT
    tt.alias AS alias,
    tt.description AS description,
    parse.token AS token,
    ARRAY ( SELECT m.mapdict::pg_catalog.regdictionary
            FROM pg_catalog.pg_ts_config_map AS m
            WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
            ORDER BY m.mapseqno )
    AS dictionaries,
    ( SELECT mapdict::pg_catalog.regdictionary
      FROM pg_catalog.pg_ts_config_map AS m
      WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
      ORDER BY pg_catalog.ts_lexize(mapdict, parse.token) IS NULL, m.mapseqno
      LIMIT 1
    ) AS dictionary,
    ( SELECT pg_catalog.ts_lexize(mapdict, parse.token)
      FROM pg_catalog.pg_ts_config_map AS m
      WHERE m.mapcfg = $1 AND m.maptokentype = parse.tokid
      ORDER BY pg_catalog.ts_lexize(mapdict, parse.token) IS NULL, m.mapseqno
      LIMIT 1
    ) AS lexemes
FROM pg_catalog.ts_parse(
        (SELECT cfgparser FROM pg_catalog.pg_ts_config WHERE oid = $1 ), $2
    ) AS parse,
     pg_catalog.ts_token_type(
        (SELECT cfgparser FROM pg_catalog.pg_ts_config WHERE oid = $1 )
    ) AS tt
WHERE tt.tokid = parse.tokid
$$
LANGUAGE SQL STRICT STABLE;

COMMENT ON FUNCTION ts_debug(regconfig,text) IS
    'debug function for text search configuration';

CREATE FUNCTION ts_debug(IN document text,
    OUT alias text,
    OUT description text,
    OUT token text,
    OUT dictionaries regdictionary[],
    OUT dictionary regdictionary,
    OUT lexemes text[])
RETURNS SETOF record AS
$$
    SELECT * FROM pg_catalog.ts_debug( pg_catalog.get_current_ts_config(), $1);
$$
LANGUAGE SQL STRICT STABLE;

COMMENT ON FUNCTION ts_debug(text) IS
    'debug function for current text search configuration';

--
-- Redeclare built-in functions that need default values attached to their
-- arguments.  It's impractical to set those up directly in pg_proc.h because
-- of the complexity and platform-dependency of the expression tree
-- representation.  (Note that internal functions still have to have entries
-- in pg_proc.h; we are merely causing their proargnames and proargdefaults
-- to get filled in.)
--

CREATE OR REPLACE FUNCTION
  pg_start_backup(label text, fast boolean DEFAULT false)
  RETURNS text STRICT VOLATILE LANGUAGE internal AS 'pg_start_backup';
CREATE OR REPLACE FUNCTION TO_TEXT(INT1)
RETURNS TEXT
AS $$ select CAST(int1out($1) AS VARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE;
CREATE OR REPLACE FUNCTION TO_TEXT(INT2)
RETURNS TEXT
AS $$ select CAST(int2out($1) AS VARCHAR)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_TEXT(INT4)
RETURNS TEXT
AS $$  select CAST(int4out($1) AS VARCHAR)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_TEXT(INT8)
RETURNS TEXT
AS $$ select CAST(int8out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_TEXT(FLOAT4)
RETURNS TEXT
AS $$ select CAST(float4out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_TEXT(FLOAT8)
RETURNS TEXT
AS $$ select CAST(float8out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_TEXT(NUMERIC)
RETURNS TEXT
AS $$ SELECT CAST(numeric_out($1) AS VARCHAR) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(NUMERIC)
RETURNS VARCHAR2
AS $$ SELECT CAST(numeric_out($1) AS VARCHAR2)    $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(INT2)
RETURNS VARCHAR2
AS $$ select CAST(int2out($1) AS VARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(INT4)
RETURNS VARCHAR2
AS $$  select CAST(int4out($1) AS VARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(INT8)
RETURNS VARCHAR2
AS $$ select CAST(int8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(FLOAT4)
RETURNS VARCHAR2
AS $$ select CAST(float4out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(FLOAT8)
RETURNS VARCHAR2
AS $$ select CAST(float8out($1) AS VARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_CHAR(TEXT)
RETURNS TEXT
AS $$ select $1 $$
LANGUAGE SQL  STRICT IMMUTABLE;

create or replace function to_number(text)
returns numeric
AS $$ select numeric_in(textout($1), 0::Oid, -1) $$
LANGUAGE SQL  STRICT IMMUTABLE;

/*text to num*/
create or replace function int1(text)
returns int1
as $$ select cast(to_number($1) as int1)$$
language sql IMMUTABLE strict;
create or replace function int2(text)
returns int2
as $$ select cast(to_number($1) as int2)$$
language sql IMMUTABLE strict;

create or replace function int4(text)
returns int4
as $$ select cast(to_number($1) as int4) $$
language sql IMMUTABLE strict;

create or replace function int8(text)
returns int8
as $$ select cast(to_number($1) as int8) $$
language sql IMMUTABLE strict;

create or replace function float4(text)
returns float4
as $$ select cast(to_number($1) as float4) $$
language sql IMMUTABLE strict;

create or replace function float8(text)
returns float8
as $$ select cast(to_number($1) as float8) $$
language sql IMMUTABLE strict;

/*character to numeric*/
CREATE OR REPLACE FUNCTION TO_NUMERIC(CHAR)
RETURNS NUMERIC
AS $$ SELECT TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_NUMERIC(VARCHAR)
RETURNS NUMERIC
AS $$ SELECT TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT;

/*character to int*/
CREATE OR REPLACE FUNCTION TO_INTEGER(VARCHAR)
RETURNS INTEGER
AS $$ SELECT int4in(varcharout($1)) $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_INTEGER(CHAR)
RETURNS INTEGER
AS $$ SELECT int4in(bpcharout($1)) $$
LANGUAGE SQL IMMUTABLE STRICT;


CREATE CAST (TEXT AS RAW) WITH FUNCTION hextoraw(TEXT);
CREATE CAST (RAW AS TEXT) WITH FUNCTION rawtohex(raw) AS IMPLICIT;

CREATE CAST (BLOB AS RAW) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (RAW AS BLOB) WITHOUT FUNCTION AS IMPLICIT;

/*character to int8*/
CREATE OR REPLACE FUNCTION TO_BIGINT(VARCHAR)
RETURNS BIGINT
AS $$ SELECT int8in(varcharout($1))$$
LANGUAGE SQL IMMUTABLE STRICT;

/*float8 to numeric*/
CREATE OR REPLACE FUNCTION TO_NUMERIC(double precision)
RETURNS NUMERIC
AS $$ SELECT TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT;

/*date to char(n)*/
CREATE OR REPLACE FUNCTION TO_TEXT(TIMESTAMP WITHOUT TIME ZONE)
RETURNS TEXT
AS $$  select CAST(timestamp_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_TEXT(TIMESTAMP WITH TIME ZONE)
RETURNS TEXT
AS $$  select CAST(timestamptz_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION "sysdate"() RETURNS timestamp AS $$ 
	SELECT CURRENT_TIMESTAMP::timestamp(0); 
$$
LANGUAGE SQL;

/* clob to char,varchar */
CREATE OR REPLACE FUNCTION TO_CHAR(CLOB)
RETURNS BPCHAR
AS $$  select CAST(textout($1::text) AS BPCHAR)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_VARCHAR2(CLOB)
RETURNS VARCHAR2
AS $$  select CAST(textout($1::text) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (CLOB AS BPCHAR) WITH FUNCTION TO_CHAR(CLOB) AS IMPLICIT;
CREATE CAST (CLOB AS VARCHAR2) WITH FUNCTION TO_VARCHAR2(CLOB) AS IMPLICIT;

/* char,varchar to clob */
CREATE OR REPLACE FUNCTION TO_CLOB(BPCHAR)
RETURNS CLOB
AS $$  select (textin(bpcharout($1))::clob) $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_CLOB(VARCHAR2)
RETURNS CLOB
AS $$  select (textin(varcharout($1))::clob) $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (BPCHAR AS CLOB) WITH FUNCTION TO_CLOB(BPCHAR) AS IMPLICIT;
CREATE CAST (VARCHAR2 AS CLOB) WITH FUNCTION TO_CLOB(VARCHAR2) AS IMPLICIT;

/* timestamp to varchar2 */
CREATE OR REPLACE FUNCTION TO_VARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS VARCHAR2
AS $$  select CAST(timestamp_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

/* interval to varchar2 */
CREATE OR REPLACE FUNCTION TO_VARCHAR2(INTERVAL)
RETURNS VARCHAR2
AS $$  select CAST(interval_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (INTERVAL AS VARCHAR2) WITH FUNCTION TO_VARCHAR2(INTERVAL) AS IMPLICIT;

/* char,varchar2 to interval */
CREATE OR REPLACE FUNCTION TO_INTERVAL(BPCHAR)
RETURNS INTERVAL
AS $$  select interval_in(bpcharout($1), 0::Oid, -1) $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_INTERVAL(VARCHAR2)
RETURNS INTERVAL
AS $$  select interval_in(varcharout($1), 0::Oid, -1) $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (BPCHAR AS INTERVAL) WITH FUNCTION TO_INTERVAL(BPCHAR) AS IMPLICIT;
CREATE CAST (VARCHAR2 AS INTERVAL) WITH FUNCTION TO_INTERVAL(VARCHAR2) AS IMPLICIT;

/* raw to varchar2 */
CREATE CAST (RAW AS VARCHAR2) WITH FUNCTION rawtohex(RAW) AS IMPLICIT;


/* varchar2,char to timestamp */
CREATE OR REPLACE FUNCTION TO_TS(VARCHAR2)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select timestamp_in(varcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION TO_TS(BPCHAR)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select timestamp_in(bpcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION timestamp_to_smalldatetime(TIMESTAMP WITHOUT TIME ZONE)
RETURNS SMALLDATETIME
AS $$  select smalldatetime_in(timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (TIMESTAMP WITHOUT TIME ZONE AS SMALLDATETIME) WITH FUNCTION timestamp_to_smalldatetime(TIMESTAMP WITHOUT TIME ZONE) AS IMPLICIT;

CREATE OR REPLACE FUNCTION smalldatetime_to_timestamp(smalldatetime)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select timestamp_in(smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (smalldatetime AS TIMESTAMP WITHOUT TIME ZONE) WITH FUNCTION smalldatetime_to_timestamp(smalldatetime) AS IMPLICIT;

/* smalldatetime to text */
CREATE OR REPLACE FUNCTION TO_TEXT(smalldatetime)
RETURNS TEXT
AS $$  select CAST(smalldatetime_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (smalldatetime AS TEXT) WITH FUNCTION TO_TEXT(smalldatetime) AS IMPLICIT;

/* smalldatetime to varchar2 */
CREATE OR REPLACE FUNCTION SMALLDATETIME_TO_VARCHAR2(smalldatetime)
RETURNS VARCHAR2
AS $$  select CAST(smalldatetime_out($1) AS VARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (smalldatetime AS VARCHAR2) WITH FUNCTION SMALLDATETIME_TO_VARCHAR2(smalldatetime) AS IMPLICIT;

/* varchar2, bpchar to smalldatetime */
CREATE OR REPLACE FUNCTION VARCHAR2_TO_SMLLDATETIME(VARCHAR2)
RETURNS SMALLDATETIME
AS $$  select smalldatetime_in(varcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION BPCHAR_TO_SMALLDATETIME(BPCHAR)
RETURNS SMALLDATETIME
AS $$  select smalldatetime_in(bpcharout($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (VARCHAR2 AS SMALLDATETIME) WITH FUNCTION VARCHAR2_TO_SMLLDATETIME(VARCHAR2) AS IMPLICIT;

CREATE CAST (BPCHAR AS SMALLDATETIME) WITH FUNCTION BPCHAR_TO_SMALLDATETIME(BPCHAR) AS IMPLICIT;
/*abstime TO smalldatetime*/
CREATE OR REPLACE FUNCTION abstime_to_smalldatetime(ABSTIME)
RETURNS SMALLDATETIME
AS $$  select smalldatetime_in(timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (ABSTIME AS SMALLDATETIME) WITH FUNCTION abstime_to_smalldatetime(ABSTIME) AS IMPLICIT;

/*smalldatetime_to_abstime*/
CREATE OR REPLACE FUNCTION smalldatetime_to_abstime(smalldatetime)
RETURNS abstime
AS $$  select abstimein(smalldatetime_out($1))  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (smalldatetime AS abstime) WITH FUNCTION smalldatetime_to_abstime(smalldatetime) AS IMPLICIT;

/*smalldatetime to time*/
CREATE OR REPLACE FUNCTION smalldatetime_to_time(smalldatetime)
RETURNS time
AS $$  select time_in(smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (smalldatetime AS time) WITH FUNCTION smalldatetime_to_time(smalldatetime) AS IMPLICIT;

/*smalldatetime_to_timestamptz*/
CREATE OR REPLACE FUNCTION smalldatetime_to_timestamptz(smalldatetime)
RETURNS TIMESTAMP WITH TIME ZONE
AS $$  select timestamptz_in(smalldatetime_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (smalldatetime AS TIMESTAMP WITH TIME ZONE) WITH FUNCTION smalldatetime_to_timestamptz(smalldatetime) AS IMPLICIT;

/*timestamptz_to_smalldatetime*/
CREATE OR REPLACE FUNCTION timestamptz_to_smalldatetime(TIMESTAMP WITH TIME ZONE)
RETURNS smalldatetime
AS $$  select smalldatetime_in(TIMESTAMPTZ_OUT($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (TIMESTAMP WITH TIME ZONE AS smalldatetime) WITH FUNCTION timestamptz_to_smalldatetime(TIMESTAMP WITH TIME ZONE) AS IMPLICIT;
create type exception as (code integer, message varchar2);
create or replace function regexp_substr(text,text)
returns text
AS '$libdir/plpgsql','regexp_substr'
LANGUAGE C STRICT IMMUTABLE;

create or replace function bitand(bigint,bigint)
returns bigint 
as $$ select $1 & $2 $$
LANGUAGE SQL STRICT IMMUTABLE;

create or replace function regexp_like(text,text)
returns boolean as $$ select $1 ~ $2 $$
LANGUAGE SQL STRICT IMMUTABLE;

create or replace function regexp_like(text,text,text)
returns boolean as $$ 
select case $3 when 'i' then $1 ~* $2 else $1 ~ $2 end;$$
LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION INTERVALTONUM(INTERVAL)
RETURNS NUMERIC
AS '$libdir/plpgsql','intervaltonum'
LANGUAGE C STRICT IMMUTABLE;

CREATE CAST (INTERVAL AS NUMERIC) WITH FUNCTION INTERVALTONUM(INTERVAL) AS IMPLICIT;

/* add for nvarcahr2 data type */
CREATE OR REPLACE FUNCTION TO_NUMERIC(NVARCHAR2)
RETURNS NUMERIC
AS $$ SELECT TO_NUMBER($1::TEXT)$$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (NVARCHAR2 AS NUMERIC) WITH FUNCTION TO_NUMERIC(NVARCHAR2) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_INTEGER(NVARCHAR2)
RETURNS INTEGER
AS $$ SELECT int4in(nvarchar2out($1))$$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (NVARCHAR2 AS INTEGER) WITH FUNCTION TO_INTEGER(NVARCHAR2) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(CLOB)
RETURNS NVARCHAR2
AS $$  select CAST(textout($1::text) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (CLOB AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(CLOB) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_CLOB(NVARCHAR2)
RETURNS CLOB
AS $$  select (textin(nvarchar2out($1))::clob) $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (NVARCHAR2 AS CLOB) WITH FUNCTION TO_CLOB(NVARCHAR2) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select CAST(timestamp_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (TIMESTAMP WITHOUT TIME ZONE AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select CAST(interval_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (INTERVAL AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(INTERVAL) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT CAST(numeric_out($1) AS NVARCHAR2)    $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select CAST(int2out($1) AS NVARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select CAST(int4out($1) AS NVARCHAR2)  $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select CAST(int8out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select CAST(float4out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select CAST(float8out($1) AS NVARCHAR2) $$
LANGUAGE SQL  STRICT IMMUTABLE;

CREATE CAST (INT2 AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(INT2) AS IMPLICIT;
CREATE CAST (INT4 AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(INT4) AS IMPLICIT;
CREATE CAST (INT8 AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(INT8) AS IMPLICIT;
CREATE CAST (NUMERIC AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(NUMERIC) AS IMPLICIT;
CREATE CAST (FLOAT4 AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(FLOAT4) AS IMPLICIT;
CREATE CAST (FLOAT8 AS NVARCHAR2) WITH FUNCTION TO_NVARCHAR2(FLOAT8) AS IMPLICIT;

CREATE OR REPLACE FUNCTION TO_TS(NVARCHAR2)
RETURNS TIMESTAMP WITHOUT TIME ZONE
AS $$  select timestamp_in(nvarchar2out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT;
CREATE CAST (NVARCHAR2 AS TIMESTAMP WITHOUT TIME ZONE) WITH FUNCTION TO_TS(NVARCHAR2) AS IMPLICIT;

create or replace function regex_like_m(text,text) returns boolean
as $$
declare
        source_line integer := 1;
        regex_line integer := 1;
        position integer := 1;
        i integer := 1;
        j integer := 1;
        regex_temp text := '';
        flag boolean := false;
        TYPE array_text is varray(1024) of text;
        source_array array_text := array_text();
        regex_array array_text := array_text();
begin
	if left($2,1) <> '^' and right($2,1) <> '$' then
		return $1 ~ $2;
	end if;
	--source string to source_array
	for i in 1..length($1) loop
		if substr($1,i,1) ~ '\n' then
			if position = i then
				source_array(source_line) := '\n';
			else
				source_array(source_line) := substr($1,position,i - position);
			end if;
			position := i + 1;
			source_line := source_line + 1;
		end if;
	end loop;
	if position <= length($1) or position = 1 then
		source_array(source_line) := substr($1,position);
	else 
		if position > length($1) then
			source_line := source_line - 1;
		end if;
	end if;
		
	--regexp string to regex_array
	position := 1;
	for i in 1..length($2) loop
		if substr($2,i,1) ~ '\n' then
			if position = i then
				regex_array(regex_line) := '\n';
			else
				regex_array(regex_line) := substr($2,position,i - position);
			end if;
			position := i + 1;
			regex_line := regex_line + 1;
		end if;
	end loop;
		if position <= length($2) or position = 1 then
			regex_array(regex_line) := substr($2,position);
		else
			if position > length($2) then
				regex_line := regex_line - 1;
			end if;
		end if;
	
	--start
	for i in 1..source_line loop
		if source_array[i] ~ regex_array[j] then
			flag := true;
			j := j + 1;
			while j <= regex_line loop
				i := i + 1;
				if source_array[i] ~ regex_array[j] then
					j := j + 1;
				else
					flag := false;
					exit;
				end if;
			end loop;
			exit;
		end if;
	end loop;
	if left($2,1) = '^' then
		regex_temp := substr($2,2);
	else
		regex_temp := $2;
	end if;
	if right($2,1) = '$' then
		regex_temp := substr(regex_temp,1,length(regex_temp)-1);
	end if;
	if flag then
 		flag := $1 ~ regex_temp;
 	end if;
	return flag;
end;
$$ LANGUAGE plpgsql;

create or replace function regexp_like(text,text,text) 
returns boolean
as $$
declare
	regex_char varchar(1);
begin
	for i in 1..length($3) loop
		regex_char := substr($3,i,1);
		if regex_char <> 'i' and  regex_char <> 'm' and  regex_char <> 'c' then
			dbms_output.put_line('illegal argument for function');
			return false;
		end if;
	end loop;
	case right($3, 1) 
		when 'i' then return $1 ~* $2;
		when 'c' then return $1 ~ $2;
		when 'm' then return regex_like_m($1,$2);
	end case;
end;
$$ LANGUAGE plpgsql;

CREATE VIEW SYS.USER_OBJECTS AS
	SELECT
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'table'::NAME
			WHEN cs.relkind='i'
				THEN 'index'::NAME
			WHEN cs.relkind='S'
				THEN 'sequence'::NAME
			WHEN cs.relkind='v'
				THEN 'view'::NAME
		END AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs
	WHERE cs.relkind in('r', 'f', 'i', 'S', 'v')
		AND pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	WHERE pg_get_userbyid(pc.proowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	WHERE pg_get_userbyid(te.typowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op
	WHERE pg_get_userbyid(op.oprowner)=SYS_CONTEXT('USERENV','CURRENT_USER');

CREATE VIEW SYS.ALL_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'table'::NAME
			WHEN cs.relkind='i'
				THEN 'index'::NAME
			WHEN cs.relkind='S'
				THEN 'sequence'::NAME
			WHEN cs.relkind='v'
				THEN 'view'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op;

CREATE VIEW ALL_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'table'::NAME
			WHEN cs.relkind='i'
				THEN 'index'::NAME
			WHEN cs.relkind='S'
				THEN 'sequence'::NAME
			WHEN cs.relkind='v'
				THEN 'view'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op;
CREATE VIEW USER_OBJECTS AS
	SELECT
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'table'::NAME
			WHEN cs.relkind='i'
				THEN 'index'::NAME
			WHEN cs.relkind='S'
				THEN 'sequence'::NAME
			WHEN cs.relkind='v'
				THEN 'view'::NAME
		END AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs
	WHERE cs.relkind in('r', 'f', 'i', 'S', 'v')
		AND pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	WHERE pg_get_userbyid(pc.proowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	WHERE pg_get_userbyid(te.typowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op
	WHERE pg_get_userbyid(op.oprowner)=SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW SYS.DBA_PROCEDURES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(f.proname AS VARCHAR2(64)) AS OBJECT_NAME,
		f.pronargs AS ARGUMENT_NUMBER
	FROM pg_proc f LEFT JOIN pg_authid n ON(n.oid=f.proowner);
CREATE OR REPLACE VIEW DBA_PROCEDURES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(f.proname AS VARCHAR2(64)) AS OBJECT_NAME,
		f.pronargs AS ARGUMENT_NUMBER
	FROM pg_proc f LEFT JOIN pg_authid n ON(n.oid=f.proowner);
CREATE OR REPLACE VIEW USER_PROCEDURES AS
	SELECT
		*
	FROM DBA_PROCEDURES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');

CREATE OR REPLACE VIEW SYS.DBA_TABLES AS
	SELECT * FROM
	(
		SELECT
		/* modify the max length of OWNER,TABLE_NAME,TABLESPACE_NAME, same with their definition */
			CAST(c.rolname AS VARCHAR2(64)) AS OWNER,
			CAST(a.relname AS VARCHAR2(64)) AS TABLE_NAME,
			CAST(b.spcname AS VARCHAR2(64)) AS TABLESPACE_NAME,
			CAST('valid' AS VARCHAR2(8)) AS STATUS,
			CASE 
				WHEN a.relpersistence = 't' OR a.relpersistence = 'g' THEN 'y'::CHAR
				ELSE 'n'::CHAR
			END AS TEMPORARY,
			CAST ('no' AS VARCHAR2 ) AS DROPPED
		FROM pg_class a,pg_tablespace b,pg_authid c
		WHERE (a.reltablespace=b.oid OR (a.reltablespace=0 AND b.spcname='pg_default'))
			AND (a.relowner=c.oid)
			AND (a.relkind='r')
)tbl ORDER BY OWNER,TABLE_NAME;
CREATE OR REPLACE VIEW DBA_TABLES AS
	SELECT * FROM
	(
		SELECT
		/* modify the max length of OWNER,TABLE_NAME,TABLESPACE_NAME, same with their definition */
			CAST(c.rolname AS VARCHAR2(64)) AS OWNER,
			CAST(a.relname AS VARCHAR2(64)) AS TABLE_NAME,
			CAST(b.spcname AS VARCHAR2(64)) AS TABLESPACE_NAME,
			CAST('valid' AS VARCHAR2(8)) AS STATUS,
			CASE 
				WHEN a.relpersistence = 't' OR a.relpersistence = 'g' THEN 'y'::CHAR
				ELSE 'n'::CHAR 
			END AS TEMPORARY,
			CAST ('no' AS VARCHAR2 ) AS DROPPED
		FROM pg_class a,pg_tablespace b,pg_authid c
		WHERE (a.reltablespace=b.oid OR (a.reltablespace=0 AND b.spcname='pg_default'))
			AND (a.relowner=c.oid)
			AND (a.relkind='r')
)tbl ORDER BY OWNER,TABLE_NAME;
CREATE OR REPLACE VIEW SYS.USER_TABLES AS
	SELECT
		*
	FROM DBA_TABLES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_TABLES AS
	SELECT
		*
	FROM DBA_TABLES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');

CREATE VIEW SYS.ALL_TABLES AS
		SELECT
			ad.rolname AS OWNER,
			cs.relname AS TABLE_NAME,
			ts.spcname AS TABLESPACE_NAME,
			CASE
				WHEN cs.relpersistence = 't' OR cs.relpersistence = 'g' THEN 'y'::CHAR
				ELSE 'n'::CHAR
			END AS TEMPORARY,
			CAST('no' AS VARCHAR2 ) AS DROPPED
		FROM pg_class cs
			LEFT JOIN pg_authid ad ON (ad.oid = cs.relowner)
			LEFT JOIN pg_tablespace ts ON (ts.oid = cs.reltablespace)
		WHERE cs.relkind in ('r','f')
			AND (pg_has_role(cs.relowner, 'usage')
				OR has_table_privilege(cs.oid, 'select, insert, update, delete, truncate, references, trigger')
				OR has_any_column_privilege(cs.oid, 'select, insert, update, references'));
CREATE VIEW ALL_TABLES AS
		SELECT
			ad.rolname AS OWNER,
			cs.relname AS TABLE_NAME,
			ts.spcname AS TABLESPACE_NAME,
			CASE
				WHEN cs.relpersistence = 't' OR cs.relpersistence = 'g' THEN 'y'::CHAR
				ELSE 'n'::CHAR
			END AS TEMPORARY,
			CAST('no' AS VARCHAR2 ) AS DROPPED
		FROM pg_class cs
			LEFT JOIN pg_authid ad ON (ad.oid = cs.relowner)
			LEFT JOIN pg_tablespace ts ON (ts.oid = cs.reltablespace)
		WHERE cs.relkind in ('r','f')
			AND (pg_has_role(cs.relowner, 'usage')
				OR has_table_privilege(cs.oid, 'select, insert, update, delete, truncate, references, trigger')
				OR has_any_column_privilege(cs.oid, 'select, insert, update, references'));
CREATE OR REPLACE VIEW SYS.DBA_TAB_COLUMNS AS 
SELECT 
OWNER,
TABLE_NAME,
COLUMN_NAME,
DATA_TYPE,
COLUMN_ID,
DATA_LENGTH,
COMMENTS
FROM
(
	/* modify the max length of OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE, same with their definition */
	SELECT 
	  tmp.*,
	  des.description AS COMMENTS
	FROM  
	(SELECT
		col.attrelid,
		col.attnum,
		CAST(owner.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(tbl.relname AS VARCHAR2(64)) AS TABLE_NAME,
		CAST(col.attname AS VARCHAR2(64)) AS COLUMN_NAME,
		CAST(pg_type.typname AS VARCHAR2(128)) AS DATA_TYPE,
		CAST(col.attnum AS INTEGER) AS COLUMN_ID,
		CAST(
			(CASE col.attlen < 0 AND col.atttypid = 1043
				WHEN TRUE THEN (col.atttypmod - 4) ::INTEGER
				ELSE col.attlen
			END) AS INTEGER) AS DATA_LENGTH
	FROM pg_attribute col,pg_class tbl,pg_authid owner,pg_type
	WHERE col.attrelid = tbl.oid
	AND tbl.relowner = owner.oid 
	AND col.atttypid = pg_type.oid
	AND col.attnum > 0
	AND (tbl.relkind='r')
	)tmp LEFT JOIN PG_DESCRIPTION des
	ON (tmp.attrelid = des.objoid AND tmp.attnum = des.objsubid)
) tbl;

CREATE OR REPLACE VIEW DBA_TAB_COLUMNS AS 
SELECT 
OWNER,
TABLE_NAME,
COLUMN_NAME,
DATA_TYPE,
COLUMN_ID,
DATA_LENGTH,
COMMENTS
FROM 
(
	/* modify the max length of OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE, same with their definition */
	SELECT
	  tmp.*,
	  des.description AS COMMENTS
	FROM  
	(SELECT
		col.attrelid,
		col.attnum,
		CAST(owner.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(tbl.relname AS VARCHAR2(64)) AS TABLE_NAME,
		CAST(col.attname AS VARCHAR2(64)) AS COLUMN_NAME,
		CAST(pg_type.typname AS VARCHAR2(128)) AS DATA_TYPE,
		CAST(col.attnum AS INTEGER) AS COLUMN_ID,
		CAST(
			(CASE col.attlen < 0 AND col.atttypid = 1043
				WHEN TRUE THEN (col.atttypmod - 4) ::INTEGER
				ELSE col.attlen
			END) AS INTEGER) AS DATA_LENGTH
	FROM pg_attribute col,pg_class tbl,pg_authid owner,pg_type
	WHERE col.attrelid = tbl.oid
	AND	tbl.relowner = owner.oid 
	AND	col.atttypid = pg_type.oid
	AND	col.attnum > 0
	AND	(tbl.relkind='r')
	)tmp LEFT JOIN PG_DESCRIPTION des
	ON (tmp.attrelid = des.objoid AND tmp.attnum = des.objsubid)
) tbl ;
CREATE VIEW SYS.ALL_TAB_COLUMNS AS 
 	SELECT
		OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		COLUMN_ID,
		DATA_LENGTH
 	FROM DBA_TAB_COLUMNS;
CREATE VIEW ALL_TAB_COLUMNS AS 
 	SELECT 
		OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		COLUMN_ID,
		DATA_LENGTH
 	FROM DBA_TAB_COLUMNS;
CREATE VIEW SYS.ALL_ALL_TABLES AS
	SELECT
		ad.rolname AS OWNER,
		cs.relname AS TABLE_NAME,
		ts.spcname AS TABLESPACE_NAME
	FROM pg_class cs
		LEFT JOIN pg_authid ad ON (ad.oid = cs.relowner)
		LEFT JOIN pg_tablespace ts ON (cs.reltablespace = ts.oid)
		WHERE cs.relkind IN ('r', 't', 'f')
			AND (pg_has_role(cs.relowner, 'usage')
				OR has_table_privilege(cs.oid, 'select, insert, update, delete, truncate, references, trigger')
				OR has_any_column_privilege(cs.oid, 'select, insert, update, references'));
CREATE VIEW ALL_ALL_TABLES AS
	SELECT
		ad.rolname AS OWNER,
		cs.relname AS TABLE_NAME,
		ts.spcname AS TABLESPACE_NAME
	FROM pg_class cs
		LEFT JOIN pg_authid ad ON (ad.oid = cs.relowner)
		LEFT JOIN pg_tablespace ts ON (cs.reltablespace = ts.oid)
		WHERE cs.relkind IN ('r', 't', 'f')
			AND (pg_has_role(cs.relowner, 'usage')
				OR has_table_privilege(cs.oid, 'select, insert, update, delete, truncate, references, trigger')
				OR has_any_column_privilege(cs.oid, 'select, insert, update, references'));
CREATE OR REPLACE VIEW SYS.DBA_VIEWS AS
	SELECT 
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(relname AS VARCHAR2(64)) AS VIEW_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='v';
CREATE OR REPLACE VIEW DBA_VIEWS AS
	SELECT 
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(relname AS VARCHAR2(64)) AS VIEW_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='v';
CREATE VIEW SYS.ALL_VIEWS AS
	SELECT
		PG_GET_USERBYID(C.RELOWNER) AS OWNER,
		C.RELNAME AS VIEW_NAME,
		LENGTH(PG_GET_VIEWDEF(C.OID)) AS TEXT_LENGTH,
		PG_GET_VIEWDEF(C.OID) AS TEXT
	FROM PG_CLASS C
	WHERE C.RELKIND = 'v';
CREATE VIEW ALL_VIEWS AS
	SELECT
		PG_GET_USERBYID(C.RELOWNER) AS OWNER,
		C.RELNAME AS VIEW_NAME,
		LENGTH(PG_GET_VIEWDEF(C.OID)) AS TEXT_LENGTH,
		PG_GET_VIEWDEF(C.OID) AS TEXT
	FROM PG_CLASS C
	WHERE C.RELKIND = 'v';
CREATE OR REPLACE VIEW SYS.USER_VIEWS AS
	SELECT
		*
	FROM DBA_VIEWS
	WHERE owner = SYS_CONTEXT('userenv','current_user');
CREATE OR REPLACE VIEW USER_VIEWS AS
	SELECT
		*
	FROM DBA_VIEWS
	WHERE owner = SYS_CONTEXT('userenv','current_user');
CREATE OR REPLACE VIEW SYS.DBA_TRIGGERS AS
	SELECT
		CAST(tgname AS VARCHAR2(64)) AS TRIGGER_NAME,
		CAST(c.relname AS VARCHAR2(64)) AS TABLE_NAME,
		CAST(n.rolname AS VARCHAR2(64)) AS TABLE_OWNER
	FROM pg_trigger t 
		LEFT JOIN pg_class c ON(t.tgrelid = c.oid)
		LEFT JOIN pg_authid n ON(n.oid = c.relowner)
	WHERE relkind='r';
CREATE OR REPLACE VIEW DBA_TRIGGERS AS
	SELECT
		CAST(tgname AS VARCHAR2(64)) AS TRIGGER_NAME,
		CAST(c.relname AS VARCHAR2(64)) AS TABLE_NAME,
		CAST(n.rolname AS VARCHAR2(64)) AS TABLE_OWNER
	FROM pg_trigger t 
		LEFT JOIN pg_class c ON(t.tgrelid = c.oid)
		LEFT JOIN pg_authid n ON(n.oid = c.relowner)
	WHERE relkind='r';
CREATE OR REPLACE VIEW SYS.USER_TRIGGERS AS
	SELECT
		*
	FROM DBA_TRIGGERS
	WHERE table_owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_TRIGGERS AS
	SELECT
		*
	FROM DBA_TRIGGERS
	WHERE table_owner = SYS_CONTEXT('USERENV','CURRENT_USER');


CREATE OR REPLACE VIEW SYS.USER_TAB_COLUMNS AS
	SELECT
		*
	FROM DBA_TAB_COLUMNS
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_TAB_COLUMNS AS
	SELECT
		*
	FROM DBA_TAB_COLUMNS
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW SYS.DBA_SOURCE AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(f.proname AS VARCHAR2(64)) AS NAME,
		f.prosrc AS TEXT
	FROM pg_proc f LEFT JOIN pg_authid n ON (n.oid=f.proowner);
CREATE OR REPLACE VIEW DBA_SOURCE AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(f.proname AS VARCHAR2(64)) AS NAME,
		f.prosrc AS TEXT
	FROM pg_proc f LEFT JOIN pg_authid n ON (n.oid=f.proowner);
CREATE VIEW SYS.ALL_SOURCE AS
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS NAME,
		'procedure'::NAME AS TYPE,
		pc.prosrc AS TEXT
	FROM pg_proc pc
	WHERE pg_has_role(pc.proowner, 'USAGE')
		OR has_function_privilege(pc.oid,'EXECUTE');
CREATE VIEW ALL_SOURCE AS
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS NAME,
		'procedure'::NAME AS TYPE,
		pc.prosrc AS TEXT
	FROM pg_proc pc
	WHERE pg_has_role(pc.proowner, 'USAGE')
		OR has_function_privilege(pc.oid,'EXECUTE');
CREATE VIEW SYS.ALL_DEPENDENCIES AS
	SELECT
		CAST(NULL AS  VARCHAR2(30)) AS OWNER,
		CAST(NULL AS  VARCHAR2(30)) AS NAME,
		CAST(NULL AS  VARCHAR2(17)) AS TYPE,
		CAST(NULL AS  VARCHAR2(30)) AS REFERENCED_OWNER,
		CAST(NULL AS  VARCHAR2(64)) AS REFERENCED_NAME,
		CAST(NULL AS  VARCHAR2(17)) AS REFERENCED_TYPE,
		CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,
		CAST(NULL AS NUMBER) AS SCHEMAID,
		CAST(NULL AS  VARCHAR2(4)) AS DEPENDENCY_TYPE
	FROM DUAL;
CREATE VIEW ALL_DEPENDENCIES AS
	SELECT
		CAST(NULL AS  VARCHAR2(30)) AS OWNER,
		CAST(NULL AS  VARCHAR2(30)) AS NAME,
		CAST(NULL AS  VARCHAR2(17)) AS TYPE,
		CAST(NULL AS  VARCHAR2(30)) AS REFERENCED_OWNER,
		CAST(NULL AS  VARCHAR2(64)) AS REFERENCED_NAME,
		CAST(NULL AS  VARCHAR2(17)) AS REFERENCED_TYPE,
		CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,
		CAST(NULL AS NUMBER) AS SCHEMAID,
		CAST(NULL AS VARCHAR2(4)) AS DEPENDENCY_TYPE
	FROM DUAL;				
CREATE OR REPLACE VIEW SYS.USER_SOURCE AS
	SELECT
		*
	FROM DBA_SOURCE
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_SOURCE AS
	SELECT
		*
	FROM DBA_SOURCE
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW SYS.DBA_SEQUENCES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS SEQUENCE_OWNER,
		CAST(relname AS VARCHAR2(64)) AS SEQUENCE_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='S';
CREATE OR REPLACE VIEW DBA_SEQUENCES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS SEQUENCE_OWNER,
		CAST(relname AS VARCHAR2(64)) AS SEQUENCE_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='S';
CREATE VIEW SYS.ALL_SEQUENCES AS
	SELECT
		ns.rolname AS SEQUENCE_OWNER,
		cs.relname AS SEQUENCE_NAME,
		(pg_sequence_parameters(cs.oid)).minimum_value AS MIN_VALUE,
		(pg_sequence_parameters(cs.oid)).maximum_value AS MAX_VALUE,
		(pg_sequence_parameters(cs.oid)).increment AS INCREMENT_BY,
		(CASE
			WHEN ((pg_sequence_parameters(cs.oid)).cycle_option) THEN 'y'::CHAR
			ELSE 'n'::CHAR
		END) AS CYCLE_FLAG
	FROM pg_class cs
		LEFT JOIN pg_authid ns ON (cs.relowner = ns.oid)
	WHERE cs.relkind = 'S'
		AND (NOT pg_is_other_temp_schema(ns.oid))
		AND (pg_has_role(cs.relowner, 'USAGE')
			OR has_sequence_privilege(cs.oid, 'SELECT, UPDATE, USAGE'));
CREATE VIEW ALL_SEQUENCES AS
	SELECT
		ns.rolname AS SEQUENCE_OWNER,
		cs.relname AS SEQUENCE_NAME,
		(pg_sequence_parameters(cs.oid)).minimum_value AS MIN_VALUE,
		(pg_sequence_parameters(cs.oid)).maximum_value AS MAX_VALUE,
		(pg_sequence_parameters(cs.oid)).increment AS INCREMENT_BY,
		(CASE
			WHEN ((pg_sequence_parameters(cs.oid)).cycle_option ) THEN 'y'::CHAR
			ELSE 'n'::CHAR
		END) AS CYCLE_FLAG
	FROM pg_class cs
		LEFT JOIN pg_authid  ns ON (cs.relowner = ns.oid)
	WHERE cs.relkind = 'S'
		AND (NOT pg_is_other_temp_schema(ns.oid))
		AND (pg_has_role(cs.relowner, 'USAGE')
			OR has_sequence_privilege(cs.oid, 'SELECT, UPDATE, USAGE'));
CREATE OR REPLACE VIEW SYS.USER_SEQUENCES AS
	SELECT
		*
	FROM DBA_SEQUENCES
	WHERE sequence_owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_SEQUENCES AS
	SELECT
		*
	FROM DBA_SEQUENCES
	WHERE sequence_owner = SYS_CONTEXT('USERENV','CURRENT_USER');

CREATE VIEW SYS.ALL_PROCEDURES AS
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME
	FROM pg_proc pc
	WHERE pg_has_role(pc.proowner, 'USAGE')
		OR has_function_privilege(pc.oid,'EXECUTE');
CREATE VIEW ALL_PROCEDURES AS
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME
	FROM pg_proc pc
	WHERE pg_has_role(pc.proowner, 'USAGE')
		OR has_function_privilege(pc.oid,'EXECUTE');
CREATE OR REPLACE VIEW SYS.USER_PROCEDURES AS
	SELECT
		*
	FROM DBA_PROCEDURES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');



CREATE OR REPLACE VIEW SYS.DBA_INDEXES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(relname AS VARCHAR2(64)) AS INDEX_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='i';
CREATE OR REPLACE VIEW DBA_INDEXES AS
	SELECT
		CAST(n.rolname AS VARCHAR2(64)) AS OWNER,
		CAST(relname AS VARCHAR2(64)) AS INDEX_NAME
	FROM pg_class c LEFT JOIN pg_authid n ON(n.oid=c.relowner)
	WHERE relkind='i';
CREATE OR REPLACE VIEW SYS.USER_INDEXES AS
	SELECT
		*
	FROM DBA_INDEXES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW USER_INDEXES AS
	SELECT
		*
	FROM DBA_INDEXES
	WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER');
CREATE OR REPLACE VIEW SYS.DBA_USERS AS
	SELECT
		CAST(rolname AS VARCHAR2(64)) AS USERNAME
	FROM pg_authid;
CREATE OR REPLACE VIEW DBA_USERS AS
	SELECT
		CAST(rolname AS VARCHAR2(64)) AS USERNAME
	FROM pg_authid;
CREATE VIEW SYS.ALL_USERS AS
	SELECT
		ad.rolname AS USERNAME,
		ad.oid AS USER_ID
	FROM pg_authid ad;
CREATE VIEW ALL_USERS AS
	SELECT
		ad.rolname AS USERNAME,
		ad.oid AS USER_ID
	FROM pg_authid ad;

CREATE VIEW SYS.DBA_DATA_FILES AS
	SELECT
		SPCNAME AS TABLESPACE_NAME,
		pg_tablespace_size(SPCNAME)::double precision AS BYTES
	FROM PG_TABLESPACE;
CREATE VIEW DBA_DATA_FILES AS
	SELECT
		SPCNAME AS TABLESPACE_NAME,
		pg_tablespace_size(SPCNAME)::double precision AS BYTES
	FROM PG_TABLESPACE;

CREATE OR REPLACE VIEW SYS.DBA_TABLESPACES AS
	SELECT
		CAST(spcname AS VARCHAR2(64)) AS TABLESPACE_NAME
	FROM pg_tablespace;
CREATE OR REPLACE VIEW DBA_TABLESPACES AS
	SELECT
		CAST(spcname AS VARCHAR2(64)) AS TABLESPACE_NAME
	FROM pg_tablespace;
CREATE VIEW SYS.DBA_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'TABLE'::NAME
			WHEN cs.relkind='i'
				THEN 'INDEX'::NAME
			WHEN cs.relkind='S'
				THEN 'SEQUENCE'::NAME
			WHEN cs.relkind='v'
				THEN 'VIEW'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'PROCEDURE'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'RULE'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'TRIGGER'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'TYPE'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'OPERATOR'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op;
	
CREATE VIEW DBA_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
			WHEN cs.relkind IN ('r', 'f')
				THEN 'TABLE'::NAME
			WHEN cs.relkind='i'
				THEN 'INDEX'::NAME
			WHEN cs.relkind='S'
				THEN 'SEQUENCE'::NAME
			WHEN cs.relkind='v'
				THEN 'VIEW'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_class cs where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'PROCEDURE'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE
	FROM pg_proc pc
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'RULE'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_rewrite re 
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'TRIGGER'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'TYPE'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'OPERATOR'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE
	FROM pg_operator op;
REVOKE ALL on DBA_OBJECTS FROM public;
REVOKE ALL on V$SESSION FROM public;
REVOKE ALL on V$SESSION_LONGOPS FROM public;
REVOKE ALL on DBA_INDEXES FROM public;
REVOKE ALL on DBA_PROCEDURES FROM public;
REVOKE ALL on DBA_SEQUENCES FROM public;
REVOKE ALL on DBA_SOURCE FROM public;
REVOKE ALL on DBA_TAB_COLUMNS FROM public;
REVOKE ALL on DBA_TABLES FROM public;
REVOKE ALL on DBA_TABLESPACES FROM public;
REVOKE ALL on DBA_TRIGGERS FROM public;
REVOKE ALL on DBA_USERS FROM public;
REVOKE ALL on DBA_VIEWS FROM public;
REVOKE ALL on DBA_DATA_FILES FROM public;

create or replace function rawtohex(text)
returns text
AS '$libdir/plpgsql','rawtohex'
LANGUAGE C STRICT IMMUTABLE;

CREATE VIEW SYS.ALL_COL_COMMENTS AS 
	SELECT
		COLUMN_NAME,
		TABLE_NAME,
		OWNER,
		COMMENTS
	FROM DBA_TAB_COLUMNS;
CREATE VIEW ALL_COL_COMMENTS AS 
	SELECT
		COLUMN_NAME,
		TABLE_NAME,
		OWNER,
		COMMENTS
	FROM DBA_TAB_COLUMNS;

CREATE OR REPLACE FUNCTION pg_nodes_memmon(OUT InnerNName text, OUT InnerUsedMem integer, OUT InnerTopCtxt integer, OUT NName text, OUT UsedMem text, OUT SharedBufferCache text,  OUT TopContext text)
RETURNS setof record
AS $$
DECLARE
  row_data record;
  row_data1 record;
  row_name record;
  query_str text;
  query_str1 text;
  shared_bcache text;
  query_str_nodes text;
  itr integer;
  BEGIN
    --Get all the node names
    query_str_nodes := 'SELECT node_name FROM pgxc_node';

    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''N_name'''' as nname1, sum(usedsize) as usedmem1 from pv_session_memory_detail where 1=1 ''';
      query_str1 := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select contextname as contextname1, sum(usedsize) as usedsize1 from pv_session_memory_detail group by contextname order by usedsize1 desc limit 3 ''';

      FOR row_data IN EXECUTE(query_str) LOOP
        row_data.nname1 := CASE
          WHEN LENGTH(row_name.node_name)<20
          THEN row_name.node_name || right('                    ',20-length(row_name.node_name))
          ELSE SUBSTR(row_name.node_name,1,20)
          END;

        InnerNName := row_data.nname1;
        NName := row_data.nname1;
        InnerUsedMem := row_data.usedmem1;
        UsedMem := pg_size_pretty(row_data.usedmem1);

        EXECUTE 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT pg_size_pretty(count(*) * 8192) || ''''(Utilization: '''' || pg_size_pretty((SELECT setting FROM pg_settings WHERE name =''''shared_buffers'''') * 8192) || ''''/'''' || round(count(*)/(SELECT setting FROM pg_settings WHERE name =''''shared_buffers''''),2)*100 || ''''%)'''' FROM pg_buffercache_pages() WHERE relfilenode IS NOT NULL '''
        INTO shared_bcache;

        SharedBufferCache := shared_bcache;

        itr := 1;
        FOR row_data1 IN EXECUTE(query_str1) LOOP
          --We should have 3 return rows
          TopContext := row_data1.contextname1 || '(' || pg_size_pretty(row_data1.usedsize1) || ')';
          InnerTopCtxt := row_data1.usedsize1;
          IF itr = 1 THEN
            RETURN next;
          ELSE
            NName := '';
            UsedMem := '';
            SharedBufferCache := '';
            RETURN next;
          END IF;
          itr := itr + 1;
        END LOOP;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_nodes_memory(OUT node_name text, OUT used_memory text, OUT shared_buffer_cache text, OUT top_context_memory text)
RETURNS setof record
AS $$
  BEGIN
    RETURN QUERY EXECUTE 'SELECT nname as node_name, usedmem as used_memory, sharedbuffercache as shared_buffer_cache, topcontext as top_context_memory FROM pg_nodes_memmon() ORDER BY innerusedmem DESC, innernname, innertopctxt DESC';
  END; $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
DECLARE
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	total_num bigint;
	BEGIN
		EXECUTE 'SELECT count(1) FROM ' || _table_name
			INTO total_num;

		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';

		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''DN_name'''' as dnname1, count(1) as count1 from ' || $1 || '''';

			FOR row_data IN EXECUTE(query_str) LOOP
				row_data.dnname1 := CASE 
					WHEN LENGTH(row_name.node_name)<20 
					THEN row_name.node_name || right('                    ',20-length(row_name.node_name)) 
					ELSE SUBSTR(row_name.node_name,1,20) 
					END;
				DNName := row_data.dnname1;
				Num := row_data.count1;
				IF total_num = 0 THEN
				Ratio := 0.000 ||'%';
				ELSE
				Ratio := ROUND(row_data.count1/total_num*100,3) || '%';
				END IF;
				RETURN next;
			END LOOP;
		END LOOP;

		RETURN;
	END; $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION table_skewness(text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
	BEGIN
		RETURN QUERY EXECUTE 'SELECT * FROM distributed_count(''' || $1 || ''') ORDER BY num DESC, dnname';
	END; $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, type text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_name text;
db_name text;
success_time1 timestamp with time zone;
success_time2 timestamp with time zone;
success_count integer;
SQL_STMT  VARCHAR2(400);
BEGIN
	SELECT SESSION_USER INTO user_name;
	SELECT CURRENT_DATABASE() INTO db_name;
	IF flag = true THEN
		--get the last login success info.
		SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_success'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) ||'order by time desc limit 1;';
	ELSE
		--get the count of success login before.
		EXECUTE 'SELECT count(*) FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_success'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) INTO success_count;

		--if success_count is more than 1, calculate the failed login record between success_time1 and success_time2.
		IF success_count > 1 THEN
			--get the last 1 login success info.
			EXECUTE 'SELECT time FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_success'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) ||'order by time desc limit 1;' INTO success_time1;

			--get the last 2 login success info.
			EXECUTE 'SELECT time FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_success'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) || 'AND time <'|| quote_literal(success_time1) || 'order by time desc limit 1;' INTO success_time2;

			--get the login failed info between last 1 and 2 login success.
			SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_failed'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) || 'AND time between' || quote_literal(success_time2) || 'and' || quote_literal(success_time1) ||';';
		ELSIF success_count = 1 THEN
			--get the last 1 login success info.
			EXECUTE 'SELECT time FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_success'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) ||'order by time desc limit 1;' INTO success_time1;

			--calculate the failed login record before last 1 success login time.
			SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_failed'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) || 'AND time < ' || quote_literal(success_time1) || ';';
		ELSE
			--this is no login audit condition.
			SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE type = ''login_failed'' AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) || ' AND database =' || quote_literal(db_name) || ';';
		END IF;
	END IF;

	--return the login audit message queryed from audit log.
	return query EXECUTE SQL_STMT;
END; $$
LANGUAGE plpgsql;

/*
 * pg_thread_wait_status
 */
CREATE VIEW pg_thread_wait_status AS
    SELECT
			S.node_name,
			S.datid AS datid,
			S.thread_name,
			S.cn_pid,
			S.pid,
			S.ppid,
			S.plevel,
            S.status AS wait_status,
            S.query
    FROM pg_stat_get_status(NULL) AS S;

/*
 * PGXC system view to look for session stat
 */

CREATE OR REPLACE FUNCTION pgxc_get_thread_wait_status()
RETURNS setof pg_thread_wait_status
AS $$
DECLARE
	row_data pg_thread_wait_status%rowtype;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_thread_wait_status''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql';

CREATE VIEW pgxc_thread_wait_status AS
    SELECT DISTINCT * from pgxc_get_thread_wait_status();