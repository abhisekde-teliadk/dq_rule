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
        print("Results: ")
        for l in self.__sql1__:
            sql = l[2]
            check_id = l[3]
            result = self.__impala__.run_sql(sql)
            result_1.append(result)
            print(check_id.__str__() + ".1: " + result.__str__())

        for l in self.__sql2__:
            sql = l[2]
            check_id = l[3]
            result = self.__impala__.run_sql(sql)
            result_2.append(result)
            print(check_id.__str__() + ".2: " + result.__str__())

        for i in range(0, len(self.__check__)):
            relation = self.__check__[i][3]
            if relation == "=":
                result_c.append((result_1[i] == result_2 [i]).__str__())
            elif relation == "<=":
                result_c.append((result_1[i] <= result_2 [i]).__str__())
            elif relation == ">=":
                result_c.append((result_1[i] >= result_2 [i]).__str__())
            elif relation == "in":
                result_c.append('') #TODO
            else:
                result_c.append('') #TODO

            sql = 'insert into check_result(check_id, result, exec_date) values({}, "{}", "{}")'.format(self.__check__[0][0].__str__(), result_c[i], exec_ts)
            print(sql)
            self.__repo__.run_sql(sql)
            
        # a rule is ok if all __check__s in it are ok
        for r in result_c:
            if r == 'False':
                result_r = r
                break
        
        sql = 'insert into rule_result(rule_id, result, exec_date) values({}, "{}", "{}")'.format(self.__check__[0][0].__str__(), result_r, exec_ts)
        print(sql)
        self.__repo__.run_sql(sql)


        # final o/p
        self.check_results = result_c
        self.rule_result = result_r
        print("Check status: ")
        print(result_c)
        print("Rule status: " + result_r)

        self.__repo__.close()

    def __init__(self, rule_id):
        self.rule_id = rule_id
        self.__repo__ = SQLite3()
        self.__impala__ = Impala()
        self.__sql1__ = self.__repo__.run_sql("select s.*, c.check_id from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_1) where r.rule_id = " + self.rule_id.__str__())
        self.__sql2__ = self.__repo__.run_sql("select s.*, c.check_id from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_2) where r.rule_id = " + self.rule_id.__str__())
        self.__check__ = self.__repo__.run_sql("select c.* from rules r join checks c on c.check_id = r.check_id where r.rule_id = 1")
        self.check_results = []
        self.rule_result = ''
