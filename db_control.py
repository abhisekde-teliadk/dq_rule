from subprocess import Popen,call,PIPE
import requests
import json
import os
import re

#execfile('t.py')
e = os.environ
assert e.get('TF_KDCIP','') != '', 'Cannot find KDC IP in environment variables!'
kdcip = e['TF_KDCIP']

registerurl = "http://"+kdcip+":5000/"

def get_from_register( url, asDict = False ):
    r = requests.get(registerurl+url)
    assert r.ok, 'Cannot retrieve ['+url+'] from the registry!'
    if asDict:
        return json.loads(r.text)
    else:
        return r.text
        
def run_command( command ):
    m = re.compile('^Fetched (\d+) row.*')
    if command.startswith('grant') or command.startswith('create'):
        print('[+] > running: '+command)
    elif command.startswith('revoke'):
        print('[-] > running: '+command)        
    else:
        print(' > running: '+command)
    cmd = Popen(["impala-shell","-i",impd,"--ssl","-B","-q",command], stdout= PIPE, stderr=PIPE )
    ( o , e ) = cmd.communicate()
    if cmd.returncode == 0:
        fetched = None
        for line in e.split('\n'):
            ma = m.match(line)
            if ma:
                fetched = int(ma.group(1))
        if fetched:
            rows = o.split('\n')
            rows = ( r for r in rows if r!= '' )
            return list( i.split('\t') for i in rows if i != '' )
        return []
    else:
        raise Exception(e)

def list_dbs():
    res = []
    l = run_command('show databases')
    for i in l:
        if i[0] not in ('default','_impala_builtins' ):
            res.append(i[0])
    return res

def list_roles():
    res = []
    l = run_command('show roles')
    for i in l:
        res.append(i[0])
    return res


def list_grants( role ):
    res = []
    l = run_command('show grant role '+role)
    for i in l:
        res.append( {"scope" : i[0], "database" : i[1], "table": i[2], "column" : i[3], "uri" : i[4], "privilege" : i[5]} )
    return res
    
def list_group_roles( group ):
    res = []
    l = run_command('show role grant group '+group)
    for i in l:
        res.append(i[0])
    return res    
            
impd = get_from_register( 'service/impalad?ip=1' )    
global_cfg = get_from_register('config/global',True)

grants =  {}
for group in global_cfg['groups']:
    grants[group] = {}
    if 'server' in global_cfg['groups'][group]['grants']:
        grants[group] = { "server": "all "} 
    if 'database' in global_cfg['groups'][group]['grants']:
        if 'database' not in grants[group]:
            grants[group]['database'] = {}
        dbgrants = global_cfg['groups'][group]['grants']['database']
        for perm in dbgrants:
            for db in dbgrants[perm]:
                grants[group]['database'][db] = 'all' if perm == 'rw' else 'select'

databases = list_dbs()
roles = list_roles()
if len(roles) == 0:
    print 'Init: we need to grant Impala (hive group) an admin role'


for role in roles:
    if role.replace('_role','') not in grants:
        print 'Dropping role '+role
        run_command('drop role '+role)        
        
for group in grants:
    if group+'_role' not in roles:
        print 'Adding role for group '+group
        run_command('create role '+group+'_role')        
        run_command('grant role '+group+'_role to group '+group )
    g = list_grants(group+'_role')
    if 'server' in grants[group]:
        run_command('grant all on server to '+group+'_role')         
                
#print list_grants('data_developer_role')

for db in databases:
    if db not in global_cfg['databases']:
        print 'Dropping db '+db
        run_command('drop database '+db)
                
for db in global_cfg['databases']:
    if db not in databases:
        print 'Adding db '+db
        run_command('create database '+db+' location "'+global_cfg['databases'][db]['path']+'"')

for group in grants:
    if 'database' in grants[group]:
        for db in grants[group]['database']:
            if db not in global_cfg['databases']:
                print('!ERROR - database '+db+' should be granted to group '+group+' but not found in the database list')
                continue
            grant = grants[group]['database'][db]                
            role_grants = list_grants( group+'_role' )
            found = False
            for gr in role_grants:
                if gr['database'] == db and gr['privilege'] == grant and gr['scope'] == 'database':
                    found = True
                if gr['database'] == db and gr['privilege'] != grant and gr['scope'] == 'database':
                    run_command('revoke '+gr['privilege']+' on database '+db+' from role '+group+'_role')
                    if gr['privilege'] == 'all':
                        run_command('revoke all on uri "'+global_cfg['databases'][db]['path']+'" from role '+group+'_role')
                if gr['scope'] == 'server':
                        run_command('revoke all on server from role '+group+'_role')                
            if not found:
                run_command('grant '+grant+' on database '+db+' to role '+group+'_role')       
                if grant == 'all':
                    run_command('grant all on uri "'+global_cfg['databases'][db]['path']+'" to role '+group+'_role')   
                    if 'additional_uri' in global_cfg['databases'][db]:
                        run_command('grant all on uri "'+global_cfg['databases'][db]['additional_uri']+'" to role '+group+'_role')                           


for group in grants:
    if 'database' not in grants[group]:
        # The group has server permission lets remove all the database permissions
        role_grants = list_grants( group+'_role' )
        for gr in role_grants:
            if gr['scope'] == 'database':
                run_command('revoke '+gr['privilege']+' on database '+gr['database']+' from role '+group+'_role')
            if gr['scope'] == 'uri':
                run_command('revoke all on uri "'+gr['uri']+'" from role '+group+'_role')
    else:
        # The group has database permissions, lets remove those which are not defined in config
        role_grants = list_grants( group+'_role' )
        for gr in role_grants:
            if gr['database'] not in global_cfg['databases'] and gr['scope'] == 'database':
                print('!ERROR - granted db '+gr['database']+' not found in the database list')
            if gr['database'] not in grants[group]['database'] and gr['scope'] == 'database':
                 run_command('revoke '+gr['privilege']+' on database '+gr['database']+' from role '+group+'_role')
                 if gr['privilege'] == 'all':
                      run_command('revoke all on uri "'+global_cfg['databases'][gr['database']]+'" from role '+group+'_role')

