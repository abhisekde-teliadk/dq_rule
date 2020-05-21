create table statements(statement_id int primary key not null, description text, statement text)

insert into statements values(1, "", "select subscriber_id from analytics.abt_subscriber_current group by subscriber_id having count(*) > 1"); 
insert into statements values(2, "", "select count(*) from analytics.abt_subscriber_current where is_active is not true"); 
insert into statements values(3, "", "select count(*) from analytics.abt_subscriber_current where active_record_flag is not true"); 
insert into statements values(4, "", "select count(*) from analytics.abt_subscriber_current where status not in ('A','S')"); 
insert into statements values(5, "", "select count_from_base - count_from_sub_current as active_diff from (select count(*) as count_from_sub_current from analytics.abt_subscriber_current ) as a, (select count(*) as count_from_base from  base.import_fokus_base_subscriber where sub_status in ('A','S')) as b"); 
insert into statements values(6, "", "select count(*) from analytics.abt_subscriber_current"); 
insert into statements values(7, "", "select subscriber_id from analytics.abt_subscriber_history where is_active is true group by 1 having count(*) > 1"); 
insert into statements values(8, "", "select subscriber_id from analytics.abt_subscriber_history where active_record_flag is true group by 1 having count(*) > 1"); 
insert into statements values(9, "", "select subscriber_id from analytics.abt_subscriber_history where is_last is true group by 1 having count(*) > 1"); 
insert into statements values(10, "", "select subscriber_id from analytics.abt_subscriber_history where is_last_valid is true group by 1 having count(*) > 1"); 
insert into statements values(11, "", "select count(*) from analytics.abt_subscriber_current a left outer join analytics.abt_subscriber_history b on a.subscriber_current_key = b.subscriber_current_key where b.subscriber_current_key is null"); 
insert into statements values(12, "", "select soc_group, count(*) from analytics.abt_service_current where soc_group not in ('No_group','Topup') and soc_group is not NULL group by 1 order by 1"); 
insert into statements values(13, "", "select count(*) from analytics.abt_intake_kpi where subscriber_history_key is null and service_history_key is null "); 
insert into statements values(14, "", "select count(*) from analytics.abt_intake_kpi a left outer join analytics.abt_subscriber_history b on a.subscriber_history_key = b.subscriber_history_key where a.subscriber_history_key is not null and b.subscriber_history_key is null"); 
insert into statements values(15, "", "select count(*) from analytics.abt_intake_kpi a left outer join analytics.abt_service_history b on a.service_history_key = b.service_history_key and b.soc_group = 'Extra_sim' where a.service_history_key is not null and b.service_history_key is null"); 
insert into statements values(16, "", "select soc, sum(closing_stock) from analytics.abt_kpis_report where year(now())*100+month(now()) = month group by 1 order by 1"); 
insert into statements values(17, "", "select * from analytics.abt_stock_kpi"); 
insert into statements values(18, "", "select case when is_extra_sim is true then 'Ekstra sim' else 'Priceplan' end as type, sum(subs_qty) from analytics.abt_stock_kpi where stock_date = subdate(to_date(now()),1) group by 1 order by 1"); 
insert into statements values(19, "", "select count(*) from analytics.abt_subscriber_current"); 
insert into statements values(20, "", "select count(*) from analytics.abt_service_current where soc_group = 'Extra_sim'"); 
insert into statements values(21, "", "select * from analytics.abt_intake_kpi where subscriber_id=206558"); 
insert into statements values(22, "", "select * from analytics.abt_subscriber_history where subscriber_id=206558 order by start_date"); 
insert into statements values(23, "", "select subscriber_id,count(*) from analytics.abt_subscriber_history where is_active=1 group by 1 having count(*) > 1"); 
insert into statements values(24, "", "select subscriber_id,is_active,* from analytics.abt_subscriber_history where subscriber_id = 14277975"); 
insert into statements values(25, "", "select 0"); 
insert into statements values(26, "", "select 1"); 
insert into statements values(27, "", "select null");

create table checks(check_id int primary key not null, description text, statement_1 int not null, relation text, statement_2 int not null, foreign key(statement_1) references statements,  foreign key(statement_2) references statements);
insert into checks values(1, "", 1, "=", 25);
insert into checks values(2, "", 2, "=", 25); 
insert into checks values(3, "", 3, "=", 25); 
insert into checks values(4, "", 4, "=", 25); 
insert into checks values(5, "", 5, "=", 25);

create table rules(rule_id int not null, description text, check_id int not null);
insert into rules values(1, "", 1); 
insert into rules values(1, "", 2); 
insert into rules values(1, "", 3); 
insert into rules values(1, "", 4); 
insert into rules values(1, "", 5);

