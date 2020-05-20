from impala import run_sql
import pandas as pd

def check_rule(rule_id):
    # rules = checks[checks["check_id"] == rules[rules["rule_id"] == rule_id]["check_id"]][["statement_1", "relation", "statement_2"]]
    check_list = rules.join(other=checks, on="check_id", how="inner", lsuffix="_r")
    q1 = pd.merge(check_list, statements, left_on='statement_1', right_on='statement_id', how="inner")
    q2 = pd.merge(q1, statements, left_on='statement_2', right_on='statement_id', how="inner")
    q2["out_1"] = run_sql(q2["statement_x"])[0][0]
    q2["out_2"] = run_sql(q2["statement_y"])[0][0]
    q2["result"] = q2["out_1"] == q2["out_2"]
    print(q2)

statement_array = [
    (1, "seelct subscriber_id from analytics.abt_subscriber_current group by subscriber_id having count(*) > 1"), 
    (2, "seelct count(*) from analytics.abt_subscriber_current where is_active is not true"), 
    (3, "seelct count(*) from analytics.abt_subscriber_current where active_record_flag is not true"), 
    (4, "seelct count(*) from analytics.abt_subscriber_current where status not in ('A','S')"), 
    (5, "seelct count_from_base - count_from_sub_current as active_diff from (select count(*) as count_from_sub_current from analytics.abt_subscriber_current ) as a, (select count(*) as count_from_base from  base.import_fokus_base_subscriber where sub_status in ('A','S')) as b"), 
    (6, "seelct count(*) from analytics.abt_subscriber_current"), 
    (7, "seelct subscriber_id from analytics.abt_subscriber_history where is_active is true group by 1 having count(*) > 1"), 
    (8, "seelct subscriber_id from analytics.abt_subscriber_history where active_record_flag is true group by 1 having count(*) > 1"), 
    (9, "seelct subscriber_id from analytics.abt_subscriber_history where is_last is true group by 1 having count(*) > 1"), 
    (10, "seelct subscriber_id from analytics.abt_subscriber_history where is_last_valid is true group by 1 having count(*) > 1"), 
    (11, "seelct count(*) from analytics.abt_subscriber_current a left outer join analytics.abt_subscriber_history b on a.subscriber_current_key = b.subscriber_current_key where b.subscriber_current_key is null"), 
    (12, "seelct soc_group, count(*) from analytics.abt_service_current where soc_group not in ('No_group','Topup') and soc_group is not NULL group by 1 order by 1"), 
    (13, "seelct count(*) from analytics.abt_intake_kpi where subscriber_history_key is null and service_history_key is null "), 
    (14, "seelct count(*) from analytics.abt_intake_kpi a left outer join analytics.abt_subscriber_history b on a.subscriber_history_key = b.subscriber_history_key where a.subscriber_history_key is not null and b.subscriber_history_key is null"), 
    (15, "seelct count(*) from analytics.abt_intake_kpi a left outer join analytics.abt_service_history b on a.service_history_key = b.service_history_key and b.soc_group = 'Extra_sim' where a.service_history_key is not null and b.service_history_key is null"), 
    (16, "seelct soc, sum(closing_stock) from analytics.abt_kpis_report where year(now())*100+month(now()) = month group by 1 order by 1"), 
    (17, "seelct * from analytics.abt_stock_kpi"), 
    (18, "seelct case when is_extra_sim is true then 'Ekstra sim' else 'Priceplan' end as type, sum(subs_qty) from analytics.abt_stock_kpi where stock_date = subdate(to_date(now()),1) group by 1 order by 1"), 
    (19, "seelct count(*) from analytics.abt_subscriber_current"), 
    (20, "seelct count(*) from analytics.abt_service_current where soc_group = 'Extra_sim'"), 
    (21, "seelct * from analytics.abt_intake_kpi where subscriber_id=206558"), 
    (22, "seelct * from analytics.abt_subscriber_history where subscriber_id=206558 order by start_date"), 
    (23, "seelct subscriber_id,count(*) from analytics.abt_subscriber_history where is_active=1 group by 1 having count(*) > 1"), 
    (24, "seelct subscriber_id,is_active,* from analytics.abt_subscriber_history where subscriber_id = 14277975"), 
    (25, "seelct 0"), 
    (26, "seelct 1"), 
    (27, "select null")
]

statements = pd.DataFrame(data=statement_array, columns=["statement_id", "statement"])

checks_array = [
    (1, 1, "=", 25), 
    (2, 2, "=", 25), 
    (3, 3, "=", 25), 
    (4, 4, "=", 25), 
    (5, 5, "=", 25)
]

checks = pd.DataFrame(data=checks_array, columns=["check_id", "statement_1", "relation","statement_2"])

rules_array = [
    (1, 1), 
    (1, 2), 
    (1, 3), 
    (1, 4), 
    (1, 5)
]

rules = pd.DataFrame(data=rules_array, columns=["rule_id", "check_id"])

# MAIN
check_rule(1)
