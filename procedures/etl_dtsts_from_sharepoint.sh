#!/bin/bash
source ~/.bashrc

wget -P cc --http-user username --http-password 'password'  http://some/site/015000.aspx
wget -P cc --http-user username --http-password 'password'  http://some/site/1500120000.aspx
wget -P cc --http-user username --http-password 'password'  http://some/site/2000125000.aspx
wget -P cc --http-user username --http-password 'password'  http://some/site/2500130000.aspx

SQLDIR="/path/to/sql/bin/"
JAVADIR="path/to/java_install/jdk-21.0.8/bin/"

$JAVADIR/java -jar /path/to/refusecc-1.0-SNAPSHOT.jar cc

rm -r cc
mkdir cc


connector="conn_string"

rows_before=$($SQLDIR/psql "$connector" -t -c "select rows_num from gate.refusers_delta where created_dttm = (select max(created_dttm) from gate.refusers_delta)")
$SQLDIR/psql "$connector" -c "truncate gate.refusers_kc1"

for file in resultcsv/*
    do
        cat $file | $SQLDIR/psql "$connector" -t -c  "COPY gate.refusers_kc1 (filial, fio, type_communication, reason, notes, phone_number, sms_refuse, created_by, created_dt, id, birth_dt)
		FROM STDIN (FORMAT CSV, DELIMITER ';', FORCE_NULL (filial, fio, type_communication, reason, notes, phone_number, sms_refuse, created_by, created_dt, id, birth_dt))"
    done

$SQLDIR/psql "$connector" -t -c "update gate.refusers_kc1 set record_created_at  = now(), record_updated_at = now()"

$SQLDIR/psql "$connector" -t -c "insert into gate.refusers_delta (select now(), (select n_live_tup from pg_catalog.pg_stat_all_tables where relname ='refusers_kc1'), (select n_live_tup 
from  pg_catalog.pg_stat_all_tables where relname = 'refusers_kc1') - $rows_before)"

rm -r resultcsv
mkdir resultcsv

exit 0
