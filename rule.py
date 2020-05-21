from database import Impala, SQLite3
import pandas as pd
from datetime import datetime

class DQRule:
    def check(self):
        exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result_1 = []
        result_2 = []
        result_c = []
        result_r = 'True'

        for l in self.__sql1__:
            sql = l[2]
            result_1.append(self.__impala__.run_sql(sql))

        for l in self.__sql2__:
            sql = l[2]
            result_2.append(self.__impala__.run_sql(sql))

        for i in range(0, len(self.__check__)):
            relation = self.__check__[i][3]
            if relation == "=":
                result_c[i] = (result_1[i] == result_2 [i]).__str__()
            elif relation == "<=":
                result_c[i] = (result_1[i] <= result_2 [i]).__str__()
            elif relation == ">=":
                result_c[i] = (result_1[i] >= result_2 [i]).__str__()
            elif relation == "in":
                result_c[i] = '' #TODO
            else:
                result_c[i] = '' #TODO

            sql = 'insert into check_result(check_id, result, exec_date) values({}, "{}", "{}")'.format(self.__check__[0], result_c[i], exec_ts)
            self.__repo__.run_sql(sql)
            
        # a rule is ok if all __check__s in it are ok
        for r in result_c:
            if r == 'False':
                result_r = r
                break
        
        sql = 'insert into rule_result(rule_id, result, exec_date) values({}, "{}", "{}")'.format(self.rule_id, result_r, exec_ts)
        self.__repo__.run_sql(sql)
        self.__repo__.close()

        # final o/p
        self.check_results = result_c
        self.rule_result = result_r
        print(result_c)

    def __init__(self, rule_id):
        self.rule_id = rule_id
        self.__repo__ = SQLite3()
        self.__impala__ = Impala()
        self.__sql1__ = self.__repo__.run_sql("select s.* from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_1) where r.rule_id = " + rule_id.__str__())
        self.__sql2__ = self.__repo__.run_sql("select s.* from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_2) where r.rule_id = " + rule_id.__str__())
        self.__check__ = self.__repo__.run_sql("select c.* from rules r join checks c on c.check_id = r.check_id where r.rule_id = 1")
        self.check_results = []
        self.rule_result = ''
