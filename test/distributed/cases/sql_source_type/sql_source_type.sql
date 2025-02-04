-- prepare
drop account if exists bvt_sql_source_type;
create account if not exists `bvt_sql_source_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- testcase
-- @session:id=1&user=bvt_sql_source_type:admin:accountadmin&password=123456
create database if not exists ssb;
use ssb;
/* cloud_user */drop table if exists __mo_t1;
/* cloud_nonuser */ create table __mo_t1(a int);
insert into __mo_t1 values(1);
select * from __mo_t1;
/* cloud_nonuser */ use system;/* cloud_user */show tables;
-- @session

-- result check
select sleep(16);
select statement, sql_source_type from system.statement_info where account="bvt_sql_source_type" and status != 'Running' and statement not like '%mo_ctl%' order by request_at desc limit 3;

-- cleanup
drop account if exists bvt_sql_source_type;
