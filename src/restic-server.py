#!/usr/bin/python

from flask import Flask, jsonify, request, abort, Response
from werkzeug.datastructures import Headers
from re import findall

from enum import Enum

import pprint, os, json
import logging

logging.basicConfig(level=logging.INFO)

# c.f. https://restic.readthedocs.io/en/latest/100_references.html#rest-backend

pp = pprint.PrettyPrinter(depth=4)
app = Flask(__name__)

class RequestStatus(Enum):
    OKAY = 200
    FAILURE = 404

file_system = {}

api_versions = { 1: "application/vnd.x.restic.rest.v1", 2: "application/vnd.x.restic.rest.v2"}

def api_version_from_header(request_header):
    api_version = 1
    try:
        accept = request.headers["Accept"]
        api_version = int(accept[-1:])
    except KeyError:
        pass
    return api_version

def print_fs():
    logging.debug("FS contents %s", file_system.keys())
    return

def build_repo_path(repo, dir):
    return os.path.join(repo, dir)

def normalize_path(path):
    if path[0] == "/":
        return path[1:]
    return path

def check_exists(repo, name):
    file_path = build_repo_path(repo, name)
    logging.debug("check_exists %s",  file_path)
    print_fs()
    return file_path in file_system

def mkrepo(path):
    path = normalize_path(path)
    logging.debug("Creating repo: %s",  path)
    file_system[path] = {}
    return RequestStatus.OKAY

def store_data(path, content):
    path = normalize_path(path)
    logging.debug("Storing data to: %s",  path)
    file_system[path] = content
    print_fs()
    return RequestStatus.OKAY

def read_data(path, read_range):
    path = normalize_path(path)
    logging.debug("Reading data from: %s",  path)

    return (file_system[path][read_range[0] : read_range[1]+1 if read_range[1] else None], len(file_system[path]))

def ls_dir(path, api=2):
    path = normalize_path(path)
    logging.debug("listing: %s",  path)
    if api == 2:
        res = [ { "name" : k[len(path):], "size" : len(v)} for k,v in file_system.items() if k.startswith(path) ]
    else:
        res = [ k[len(path):]  for k in file_system if k.startswith(path) ]
    return json.dumps(res)

def remove(path):
    path = normalize_path(path)
    logging.debug("removing: %s",  path)
    if file_system.pop(path, None):
        return RequestStatus.OKAY
    else:
        return RequestStatus.FAILURE

def get_range(request_header):
    begin = 0
    end = None
    logging.debug("Get range: %s", pp.pformat(request.headers))
    try:
        ranges = findall(r"\d+", request_header["Range"])
        begin  = int( ranges[0] )
        if len(ranges)>1:
            end = int( ranges[1] )
    except KeyError:
        pass

    return (begin, end)


@app.route('/<path:repo>/<string:name>', methods=['HEAD'])
def head_object(repo, name):
    if check_exists(repo, name):
        return Response("{}", status=200, mimetype=api_versions[2])
    else:
        return Response("{}", status=404, mimetype=api_versions[2])


@app.route('/<path:repo>/<string:name>', methods=['GET'])
def get_object(repo, name):
    headers = Headers()

    read_range = get_range(request.headers)
    logging.info("Get range parsed %s", read_range)
    contents, full_len = read_data(request.path, read_range)

    if contents == None:
        return Response("{}", status=RequestStatus.FAILURE.value, mimetype=api_versions[2]) 
    else:
        headers.add('Content-Transfer-Encoding','binary')
        status=RequestStatus.OKAY.value
        if read_range[1]:
            status=206
            headers.add('Content-Range','bytes %s-%s/%s' % (str(read_range[0]),str(read_range[1]),str(full_len)) )
        return Response(contents, status=status, mimetype='application/octet-stream', headers=headers) 

@app.route('/<path:repo>/<string:name>/', methods=['GET'])
def list_object(repo, name):
    api_version = api_version_from_header(request.headers)
    contents = ls_dir(request.path, api=api_version)

    if contents == None:
        return Response("{}", status=RequestStatus.FAILURE.value, mimetype=api_versions[api_version]) 
    else:
        return Response(contents, status=RequestStatus.OKAY.value, mimetype=api_versions[api_version]) 

@app.route('/<path:path>', methods=['POST'])
def post_object(path):
    request_status = None
    if request.args.get("create"):
        request_status = mkrepo(request.path)
    else:
        request_status = store_data(request.path, request.data)
    return Response("", status=request_status.value, mimetype=api_versions[2]) 


@app.route('/<path:path>', methods=['DELETE'])
def delete_object(path):  
    return Response("", status=remove(request.path).value, mimetype=api_versions[2]) 

