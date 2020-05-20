from subprocess import Popen,call,PIPE
import requests
import json
import os
import re
import sys

def get_impala_url():
    e = os.environ
    assert e.get('TF_KDCIP','') != '', 'Cannot find KDC IP in environment variables!'
    kdcip = e['TF_KDCIP']
    url = "http://"+kdcip+":5000/service/impalad?ip=1"
    r = requests.get(url)
    assert r.ok, 'Cannot retrieve ['+url+'] from the registry!'
    impd = r.text  
    return impd
        
def run_sql(impd, statement):
    m = re.compile('^Fetched (\d+) row.*')
    cmd = Popen(["impala-shell","-i", impd ,"--ssl", "-B", "-q", statement], stdout= PIPE, stderr=PIPE )
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
impd = get_impala_url()
sql_output = run_sql(impd, sys.argv[0])
print(sql_output)