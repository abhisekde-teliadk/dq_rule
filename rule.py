from database import Impala, SQLite3
import pandas as pd

class DQRule:
    def check_rule(self, rule_id):
        check_list = self.rules.join(other=self.checks, on="check_id", how="inner", lsuffix="_r")
        q1 = pd.merge(check_list, self.statements, left_on='statement_1', right_on='statement_id', how="inner")
        q2 = pd.merge(q1, self.statements, left_on='statement_2', right_on='statement_id', how="inner")

        q2["statement_x"].applymap(lambda x: self.repo.run_sql(x))
        q2["statement_y"].applymap(lambda x: self.repo.run_sql(x))
        
        #q2["out_x"] = self.repo.run_sql(q2["statement_x"])[0][0]
        #q2["out_y"] = self.repo.run_sql(q2["statement_y"])[0][0]
        #q2["result"] = q2["out_1"] == q2["out_2"]
        print(q2)
        self.repo.close()

    def __init__(self):
        self.repo = SQLite3()
        self.impala = Impala()
        statement_array = self.repo.run_sql("select * from statements")
        checks_array = self.repo.run_sql("select * from checks")
        rules_array = self.repo.run_sql("select * from rules")

        self.statements = pd.DataFrame(data=statement_array, columns=["statement_id", "description", "statement"])
        self.checks = pd.DataFrame(data=checks_array, columns=["check_id", "description", "statement_1", "relation","statement_2"])
        self.rules = pd.DataFrame(data=rules_array, columns=["rule_id", "description", "check_id"])
        #check_rule(1)
        #repo.close()
