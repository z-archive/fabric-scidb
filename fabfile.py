#!/usr/bin/env python

import os
import os.path
import scidb
from fabric.api import env, run as bad_run, local as bad_local, execute, get
from fabric.decorators import runs_once, hosts, parallel
from fabric.context_managers import settings, hide, cd
from scidb import get_path
from os.path import basename
#env.use_ssh_config=True
#env.disable_known_hosts=True
#env.forward_agent=True
#env.no_keys=True

run = scidb.split(bad_run)
local = scidb.split(bad_local)
    
@hosts('localhost')
def ilist():
    column_list = ['id', 'host', 'port', 'online', 'path']
    format_string = '\t'.join(list('%%(%s)s' % column for column in column_list))
    header_string = format_string % dict(zip(column_list, column_list))
    print os.linesep.join([header_string] + [format_string % i for i in scidb.ilist()])

@parallel
def clean_remote():
    run('find %s -name "*.log" -exec rm {} +' % get_path(env.host))
    run('find %s -name "core.*" -exec rm {} +' % get_path(env.host))

@hosts('localhost')
def clean_local():
    local('find . -name "*.log" -exec rm {} +')
    local('find . -name "core.*" -exec rm {} +')

@hosts('localhost')
def clean():
    execute(clean_local)
    execute(clean_remote)

@hosts('localhost')
def stop():
    local("scidb.py stopall cheshire")

@hosts('localhost')
def start():
    local("scidb.py startall cheshire")
    local("sleep 5")

@hosts('localhost')
def restart():
    execute(stop)
    execute(clean)
    execute(start)

@hosts('localhost')
def query(query):
    local('time iquery -aq "%s"' % query)

@parallel
def capture():
    host = env.host
    path = scidb.get_path(host)
    id = scidb.get_id(host)
    log_list = run('find %s -name "*.log"' % path)
    core_list = run('find %s -name "core.*"' % path)
    for remote_path in log_list + core_list:
        print "'%s'" % remote_path
        get(remote_path, local_path='host-%s-%s' % (id, basename(remote_path)))

@hosts('localhost')
def test(q):
    execute(restart)
    execute(query, q)
    execute(capture)
    execute(backup)

@hosts('localhost')
def backup():
    log_list = local('find . -name "*.log"', capture=True)
    core_list = local('find . -name "core.*"', capture=True)
    path = 'backup-%s' % (max([-1] + map(int,
                                        list(item.replace('./backup-', '') 
                                             for item in 
                                             local('find . -name "backup-*"', 
                                                   capture=True)))) + 1)
    os.mkdir(path)
    for name in log_list + core_list:
        local('cp %s %s/' % (name, path))
    print "RESULT IN %s" % path

def ls():
    host = env.host
    path = scidb.get_path(host)
    log_list = run('find %s -name "*.log"' % path)
    core_list = run('find %s -name "core.*"' % path)
    for path in log_list + core_list:
        print "'%s'" % path


