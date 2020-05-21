create table statements(statement_id integer primary key autoincrement, description text, statement text);

insert into statements(description, statement) values("", "select subscriber_id from analytics.abt_subscriber_current group by subscriber_id having count(*) > 1"); 
insert into statements(description, statement) values("", "select count(*) from analytics.abt_subscriber_current where is_active is not true"); 
insert into statements(description, statement) values("", "select count(*) from analytics.abt_subscriber_current where active_record_flag is not true"); 
insert into statements(description, statement) values("", "select count(*) from analytics.abt_subscriber_current where status not in ('A','S')"); 
insert into statements(description, statement) values("", "select count_from_base - count_from_sub_current as active_diff from (select count(*) as count_from_sub_current from analytics.abt_subscriber_current ) as a, (select count(*) as count_from_base from  base.import_fokus_base_subscriber where sub_status in ('A','S')) as b"); 
insert into statements(description, statement) values("", "select count(*) from analytics.abt_subscriber_current"); 
insert into statements(description, statement) values("", "select subscriber_id from analytics.abt_subscriber_history where is_active is true group by 1 having count(*) > 1"); 
insert into statements(description, statement) values("", "select subscriber_id from analytics.abt_subscriber_history where active_record_flag is true group by 1 having count(*) > 1"); 
insert into statements(description, statement) values("", "select subscriber_id from analytics.abt_subscriber_history where is_last is true group by 1 having count(*) > 1"); 
insert into statements(description, statement) values( "", "select subscriber_id from analytics.abt_subscriber_history where is_last_valid is true group by 1 having count(*) > 1"); 
insert into statements(description, statement) values( "", "select count(*) from analytics.abt_subscriber_current a left outer join analytics.abt_subscriber_history b on a.subscriber_current_key = b.subscriber_current_key where b.subscriber_current_key is null"); 
insert into statements(description, statement) values("", "select soc_group, count(*) from analytics.abt_service_current where soc_group not in ('No_group','Topup') and soc_group is not NULL group by 1 order by 1"); 
insert into statements(description, statement) values("", "select 0"); 
insert into statements(description, statement) values("", "select 1"); 
insert into statements(description, statement) values("", "select null");

create table checks(check_id integer primary key  autoincrement, description text, statement_1 integer not null, relation text, statement_2 integer not null, foreign key(statement_1) references statements,  foreign key(statement_2) references statements);
insert into checks(description, statement_1, relation, statement_2) values("", 1, "=", 13);
insert into checks(description, statement_1, relation, statement_2) values("", 2, "=", 11); 
insert into checks(description, statement_1, relation, statement_2) values("", 3, "=", 11); 
insert into checks(description, statement_1, relation, statement_2) values("", 4, "=", 11); 


create table rules(rule_id integer not null, description text, check_id integer not null);
insert into rules values(1, "", 1); 
insert into rules values(1, "", 2); 
insert into rules values(1, "", 3); 
insert into rules values(1, "", 4); 

create table rule_result(rule_result_id integer primary key  autoincrement, rule_id integer not null, result text, exec_date text not null, foreign key(rule_id) references rules);

create table check_result(check_result_id integer primary key  autoincrement, rule_result_id integer, check_id integer not null, result text, exec_date text not null, foreign key(check_id) references checks, foreign key(rule_result_id) references rule_result);

create table statement_result(statement_result_id integer primary key  autoincrement, check_result_id integer, statement_id integer not null, result text, exec_date text not null, foreign key(statement_id) references statements, foreign key(check_result_id) references check_result);
