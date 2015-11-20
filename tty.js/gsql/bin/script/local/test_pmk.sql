DECLARE
    pmk_oid oid;
    class_count int;
    proc_count int;
BEGIN
    --if pmk schema not exist, it will raise an error.
    select oid from pg_namespace where nspname='pmk' into pmk_oid;
    select count(*) from pg_class where relnamespace=pmk_oid into class_count;
    select count(*) from pg_proc where pronamespace=pmk_oid into proc_count;
    raise info 'pmk schema exist. class count is %, proc count is %.', class_count , proc_count;
END;
/