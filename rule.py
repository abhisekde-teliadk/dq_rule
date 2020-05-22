from database import Impala, SQLite3
import pandas as pd
from datetime import datetime
import threading

class DQRule:
    def __exec_sql__(self, sql, storage, check_id): 
        print(check_id, sql)
        result = self.__impala__.run_sql(sql)
        storage[check_id] = result

    def check(self):
        threads = []
        l_range = len(self.__sql1__)
        exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for l in range(l_range):
            sql = self.__sql1__[l][2]
            check_id = self.__sql1__[l][3]
            t = threading.Thread(target=self.__exec_sql__, args=[sql, self.__result_1__, check_id])
            t.start()
            threads.append(t)

            sql = self.__sql2__[l][2]
            check_id = self.__sql1__[l][3]
            t = threading.Thread(target=self.__exec_sql__, args=[sql, self.__result_2__, check_id])
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()

        print("Test case execution results: ")
        for l in self.__result_1__.keys():
            relation = self.__check__[l][3]
            check_id = self.__check__[l][0].__str__()
            result_1 = self.__result_1__[check_id].__str__()
            result_2 = self.__result_2__[check_id].__str__()

            if relation == "=":
                test = result_1 == result_2
            elif relation == "<=":
                test = result_1 <= result_2
            elif relation == ">=":
                test = result_1 >= result_2
            elif relation == "in": 
                test = ''  #TODO
            else:
                test = ''  #TODO
            
            verdict = 'Not defined'
            if test.__str__() == 'True':
                verdict = 'Pass'
            else:
                verdict = 'Fail'

            self.__result_c__[check_id] = verdict
            
            print("C" + check_id + ".Q1: " + result_1)
            print("C" + check_id + ".Q2: " + result_2)
            sql = 'insert into check_result(check_id, result, exec_date) values({}, "{}", "{}")'.format(check_id, self.__result_c__[l], exec_ts)
            self.__repo__.run_sql(sql)

        # A rule is Pass, if all test cases in checks are Pass
        for r in self.__result_c__:
            if r == 'Fail':
                self.__result_r__ = r
                break
        
        sql = 'insert into rule_result(rule_id, result, exec_date) values({}, "{}", "{}")'.format(self.rule_id, self.__result_r__, exec_ts)
        self.__repo__.run_sql(sql)

        print("")
        # final o/p
        self.check_results = self.__result_c__
        self.rule_result = self.__result_r__
        print("Rule status: " + self.__result_r__)

        self.__repo__.close()

    def __init__(self, rule_id):
        self.rule_id = rule_id
        self.__repo__ = SQLite3()
        self.__impala__ = Impala()
        self.__sql1__ = self.__repo__.run_sql("select s.*, c.check_id from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_1) where r.rule_id = " + self.rule_id.__str__())
        self.__sql2__ = self.__repo__.run_sql("select s.*, c.check_id from rules r join checks c on c.check_id = r.check_id join statements s on (s.statement_id = c.statement_2) where r.rule_id = " + self.rule_id.__str__())
        self.__check__ = self.__repo__.run_sql("select c.*, rule_id from rules r join checks c on c.check_id = r.check_id where r.rule_id = " + self.rule_id.__str__())
        self.check_results = []
        self.rule_result = ''
        self.__result_1__ = {}
        self.__result_2__ = {}
        self.__result_c__ = {}
        self.__result_r__ = 'Pass'
