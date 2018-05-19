#!/usr/bin/python

from flask import Flask, jsonify, request, abort, Response
from io import BytesIO
from werkzeug.datastructures import Headers
from re import findall
from jottalib import JFS

from enum import Enum

import itertools
import pprint, posixpath, json, os, errno
import logging

logging.basicConfig(level=logging.INFO)

# c.f. https://restic.readthedocs.io/en/latest/100_references.html#rest-backend

pp = pprint.PrettyPrinter(depth=4)
app = Flask(__name__)

class RequestStatus(Enum):
    OKAY = 200
    FAILURE = 404

class JottaRest:
    
    def __init__(self, auth):
        self.client = JFS.JFS(auth)

    def check_exists(self, repo, name):
        file_path = build_repo_path(repo, name)
        logging.debug("check_exists %s",  file_path)
        
        try:
            f = self.client.getObject(file_path)
        except JFS.JFSError:
            return False
        if isinstance(f, (JFS.JFSFile, JFS.JFSFolder, JFS.JFSIncompleteFile)) and f.is_deleted():
            return False
        return True

    def mkdir(self, path):
        parentfolder = os.path.dirname(path)
        newfolder = os.path.basename(path)
        logging.debug("mkdir parent = %s, folder = %s",  parentfolder, newfolder)
        try:
            f = self.client.getObject(parentfolder)
        except JFS.JFSError:
            raise OSError(errno.ENOENT, '')
        if not isinstance(f, JFS.JFSFolder):
            raise OSError(errno.EACCES) # can only create stuff in folders
        if isinstance(f, (JFS.JFSFile, JFS.JFSFolder)) and f.is_deleted():
            raise OSError(errno.ENOENT)

        return f.mkdir(newfolder)

    def mkrepo(self, path):
        path = normalize_path(path)
        logging.debug("Creating repo: %s",  path)
        
        try:
            self.mkdir(path)
            for subdir in ["data", "index", "keys", "locks", "snapshots"]:
                self.mkdir(posixpath.join(path, subdir))
        except OSError:
            return RequestStatus.FAILURE
        return RequestStatus.OKAY

    def store_data(self, path, content):
        path = normalize_path(path)
        logging.debug("Storing data to: %s",  path)

        self.client.up(path, BytesIO(content))

        return RequestStatus.OKAY

    def read_data(self, path, read_range):
        path = normalize_path(path)
        logging.debug("Reading data from: %s (%s)",  path, read_range)

        try:
            f = self.client.getObject(path)
        except JFS.JFSError:
            raise OSError(errno.ENOENT, '')
        if isinstance(f, (JFS.JFSFile, JFS.JFSFolder)) and f.is_deleted():
            raise OSError(errno.ENOENT)
        # gnu tools may happily ask for content beyond file size
        # but jottacloud doesn't like that
        # so we make sure we stay within file size (f.size)
        end = min(read_range[1]+1, f.size) if read_range[1] else f.size
        logging.debug("f.readpartial(%s, %s) on file of size %s" % (read_range[0], end, f.size))
        
        return ( f.readpartial(read_range[0], end) if end - read_range[0] > 0 else "", f.size)

    def readdir(self, path):
        if path == '/':
            for d in self.client.devices:
                yield d.name
        else:
            p = self.client.getObject(path)
            if isinstance(p, JFS.JFSDevice):
                for name in p.mountPoints.keys():
                    yield (name, 0)
            else:
                for el in itertools.chain(p.folders(), p.files()):
                    if not el.is_deleted():
                        yield (el.name, el.size)

    def ls_dir(self, path, api=2):
        path = normalize_path(path)
        logging.debug("listing: %s",  path)

        if api == 2:
            res = [ { "name" : k, "size" : s} for k,s in self.readdir(path) ]
        else:
            res = [ k for k,v in self.readdir(path) ]
        return json.dumps(res)

    def remove(self, path):
        path = normalize_path(path)
        logging.debug("removing: %s",  path)
        try:
            f = self.client.getObject(path)
        except JFS.JFSError:
            return RequestStatus.FAILURE
        f.delete()
        return RequestStatus.OKAY
           

jotta_rest = JottaRest(auth = JFS.get_auth_info())


api_versions = { 1: "application/vnd.x.restic.rest.v1", 2: "application/vnd.x.restic.rest.v2"}

def api_version_from_header(request_header):
    api_version = 1
    try:
        accept = request.headers["Accept"]
        api_version = int(accept[-1:])
    except KeyError:
        pass
    return api_version

def build_repo_path(repo, dir):
    return normalize_path(posixpath.join(repo, dir))

def normalize_path(path):
    return posixpath.normpath(posixpath.join("/", path))




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
    if jotta_rest.check_exists(repo, name):
        return Response("{}", status=200, mimetype=api_versions[2])
    else:
        return Response("{}", status=404, mimetype=api_versions[2])


@app.route('/<path:repo>/<string:name>', methods=['GET'])
def get_object(repo, name):
    headers = Headers()

    read_range = get_range(request.headers)
    logging.info("Get range parsed %s", read_range)
    contents, full_len = jotta_rest.read_data(request.path, read_range)

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
    contents = jotta_rest.ls_dir(request.path, api=api_version)

    if contents == None:
        return Response("{}", status=RequestStatus.FAILURE.value, mimetype=api_versions[api_version]) 
    else:
        return Response(contents, status=RequestStatus.OKAY.value, mimetype=api_versions[api_version]) 

@app.route('/<path:path>', methods=['POST'])
def post_object(path):
    request_status = None
    if request.args.get("create"):
        request_status = jotta_rest.mkrepo(request.path)
    else:
        request_status = jotta_rest.store_data(request.path, request.data)
    return Response("", status=request_status.value, mimetype=api_versions[2])


@app.route('/<path:path>', methods=['DELETE'])
def delete_object(path):  
    return Response("", status=jotta_rest.remove(request.path).value, mimetype=api_versions[2])



if __name__ == "__main__":
    app.run()
