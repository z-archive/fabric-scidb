#!/usr/bin/env python

import os
import os.path
import scidb
from fabric.api import env, run as bad_run, local as bad_local, execute, get, sudo
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
    local('rm -rf host-*')

@hosts('localhost')
def clean():
    execute(clean_local)
    execute(clean_remote)

@hosts('localhost')
def stop():
    local("/opt/scidb/12.7/bin/scidb.py stopall cheshire")

@hosts('localhost')
def start():
    local("/opt/scidb/12.7/bin/scidb.py startall cheshire")
    local("sleep 10")

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
    path = 'host-%s' % scidb.get_id(host)
    local('mkdir -p %s' % path)

    def local_path(remote_path):
        return os.path.join(path, basename(remote_path))
    def backup(remote_path):
        path = local_path(remote_path)
        local('rm -rf %s' % path)
        get(remote_path, local_path=path)
    def backup_with_link(remote_path):
        backup(remote_path)
        local_host_path = local_path(remote_path)
        link_path = '%s-%s' % (path, basename(remote_path))
        local('rm -rf %s' % link_path)
        local('ln %s %s' % (local_host_path, link_path))

    map(backup, run('find %s -name "*.log"' % scidb.get_path(host))) 
    map(backup_with_link, run('find %s -name "core.*"' % scidb.get_path(host)))


@hosts('localhost')
def test(q):
    execute(stop)
    execute(clean)
    execute(build)
    execute(start)
    execute(query, q)
    execute(capture)
    execute(backup)

@hosts('localhost')
def backup():
    path = 'backup-%s' % (max([-1] + map(int,
                                        list(item.replace('./backup-', '') 
                                             for item in 
                                             local('find . -name "backup-*"', 
                                                   capture=True)))) + 1)
    os.mkdir(path)
    for dir in local('ls | grep host', capture=True):
        local('cp -R %s %s/' % (dir, path))
    print "result in '%s'" % path

@hosts('localhost')
def backup_clean():
    local('rm -rf backup-*')

@hosts('localhost')
def build():
    for path in ['/data/src/trunk.oleg', '/data/src/p4.oleg']:
        local('cd %s && ./build' % path)

@hosts('localhost')
def rebuild():
    for path in ['/data/src/trunk.oleg', '/data/src/p4.oleg']:
        local('cd %s && ./rebuild' % path)

def ps():
    run('ps ax | grep scidb | grep mnt')

@parallel
def kill_scidb():
    with settings(warn_only=True):
        run("ps ax | grep scidb | grep mnt | awk '{print $1}' | xargs kill")
    with settings(warn_only=True):
        run("sleep 5")
    with settings(warn_only=True):
        run("ps ax | grep scidb | grep mnt | awk '{print $1}' | xargs kill -9")

@hosts('localhost')
def kill():
    execute(kill_scidb)
    execute(ps)

def ls():
    host = env.host
    path = scidb.get_path(host)
    log_list = run('find %s -name "*.log"' % path)
    core_list = run('find %s -name "core.*"' % path)
    for path in log_list + core_list:
        print "'%s'" % path


