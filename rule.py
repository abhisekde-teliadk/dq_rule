from subprocess import Popen,call,PIPE
import requests
import json
import os
import re
import sys

def get_impala_url():
    env = os.environ
    assert env.get('TF_KDCIP','') != '', 'Cannot find KDC IP in environment variables!'
    kdcip = env['TF_KDCIP']
    url = 'http://' + kdcip + ':5000/service/impalad?ip=1'

    impd = ''
    try:
        r = requests.get(url)
        impd = r.text
    except Exception as e:
        print('Cannot retrieve ['+url+'] from the registry!')
    assert r.ok, 'Cannot retrieve ['+url+'] from the registry!'
    return impd

def run_sql(impd, command):
    print("SQL: " + command)
    cmd = Popen(["kinit","-k","-t","/home/centos/impala.keytab","impala"], stdout= PIPE, stderr=PIPE ) 
    (o , e) = cmd.communicate()

    assert len(e) == 0, 'Can initiate Kerberos ticket with: ' + "kinit -k -t /home/centos/impala.keytab impala"
    m = re.compile('^Fetched (\d+) row.*')
    cmd = Popen(["impala-shell", "-i", impd, "--ssl", "-B", "-q", command], stdout= PIPE, stderr=PIPE )
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

# MAIN
impala_url = get_impala_url()
if len(impala_url) > 0:
    sql_output = run_sql(impala_url, sys.argv[0])
    print(sql_output)
else:
    print('SQL execution failed!')
