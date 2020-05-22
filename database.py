import sqlite3
from sqlite3 import Error
from subprocess import Popen,call,PIPE
import requests
import json
import os
import re
import sys


class SQLite3:
    def __init__(self, db_file="datamall.db"):
        self.__db_file__ = db_file
        self.connection = self.__create_connection__(db_file)
        assert self.connection != None, "Can not open DB " + db_file

    def __create_connection__(self, db_file):
        self.connection = None
        try:
            self.connection = sqlite3.connect(db_file)
        except Error as e:
            print(e)
        return self.connection

    def run_sql(self, command):
        data = []
        try:
            c = self.connection.cursor()
            data = c.execute(command).fetchall()
            
        except Error as e:
            print(e)
        finally:
            if c:
                c.close()

        return data

    def close(self):
        if self.connection:
            self.connection.commit()
            self.connection.close()

class Impala:  
    def __init__(self):
        env = os.environ
        assert env.get('TF_KDCIP','') != '', 'Cannot find KDC IP in environment variables!'
        kdcip = env['TF_KDCIP']
        url = 'http://' + kdcip + ':5000/service/impalad?ip=1'
        self.impd = ''
        try:
            r = requests.get(url)
            self.impd = r.text
        except Exception as e:
            print(e.__str__())
        assert r.ok, 'Cannot retrieve ['+url+'] from the registry!'

    def run_sql(self, command):
        cmd = Popen(["kinit","-k","-t","/home/centos/impala.keytab","impala"], stdout= PIPE, stderr=PIPE ) 
        (o , e) = cmd.communicate()
        assert len(e) == 0, 'Can initiate Kerberos ticket with: ' + "kinit -k -t /home/centos/impala.keytab impala"
        m = re.compile('^Fetched (\d+) row.*')
        cmd_arr = ["impala-shell", "-i", self.impd, "--ssl", "-B", "-q", command]
        print(command)
        cmd = Popen(cmd_arr, stdout= PIPE, stderr=PIPE )
        (o , e) = cmd.communicate()
        if cmd.returncode == 0:
            fetched = None
            for line in e.split('\n'):
                ma = m.match(line)
                if ma:
                    fetched = int(ma.group(1))
            if fetched:
                rows = o.split('\n')
                rows = (r for r in rows if r!= '')
                return list(i.split('\t') for i in rows if i != '')
            return []
        else:
            raise Exception(e)
