#!/usr/bin/env python

import os
import pickle
import subprocess
from functools import wraps

from fabric.api import env, local
from fabric.context_managers import settings, hide
from fabric.decorators import hosts

CACHE={}

def single(f):
    global CACHE
    CACHE[f.__name__] = None
    @wraps(f)
    def wrapper():
        global CACHE
        if CACHE[f.__name__] is None:
            CACHE[f.__name__] = f()
        return CACHE[f.__name__]
    return wrapper

def split(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return filter(lambda item: item != '',
                      f(*args, **kwargs).replace('\r', '').split('\n'))
    return wrapper


INSTANCE_LIST_DUMP='/home/ubuntu/1793/instance_list_dump'

@hosts('127.0.0.1')
@single
def idict():
    #with settings(hide('running')):
    if True:
        idict = {}
        try:
            output = local('iquery -aq "%s"' % "list('instances')", capture=True)
            output = output.split(os.linesep)[1:]
            for line in output:
                (_, host, port, id, online, path) = tuple(line.split(","))
                idict[id] = dict(id=id, 
                                 host=host[1:-1],
                                 port=port,
                                 online=online,
                                 path=path[1:-1])
            pickle.dump(idict, open(INSTANCE_LIST_DUMP, 'w'))
        except BaseException:
            if os.path.exists(INSTANCE_LIST_DUMP):
                idict = pickle.load(open(INSTANCE_LIST_DUMP, 'r'))
            else:
                raise
        return idict

@single
def ilist():
    l = list(idict())
    l.sort()
    return list(idict()[id] for id in l)

@single 
def hdict():
    result = {}
    for id in idict():
        result[ idict()[id]['host'] ] = idict()[id]
    return result

def get(host, key):
    return hdict()[host][key]

def get_path(host):
    return get(host, 'path')

def get_id(host):
    return get(host, 'id')
    
env.hosts = list(i['host'] for i in ilist())
