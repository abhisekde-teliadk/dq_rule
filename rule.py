from subprocess import Popen,call,PIPE
import requests
import json
import os
import re
import sys

def get_from_register( url, asDict = False ):
    r = requests.get(registerurl+url)
    assert r.ok, 'Cannot retrieve ['+url+'] from the registry!'
    if asDict:
        return json.loads(r.text)
    else:
        return r.text
        
def run_command(command):
    print(command)
    m = re.compile('^Fetched (\d+) row.*')
    cmd = Popen(["impala-shell","-i",impd,"--ssl","-B","-q",command], stdout= PIPE, stderr=PIPE )
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
e = os.environ
assert e.get('TF_KDCIP','') != '', 'Cannot find KDC IP in environment variables!'
kdcip = e['TF_KDCIP']
registerurl = "http://"+kdcip+":5000/"
impd = get_from_register('service/impalad?ip=1' )    
global_cfg = get_from_register('config/global',True)

sql_output = run_command(sys.argv[0])
print(sql_output)