#
# validator.py
# Data Culpa Python Client
#
# Copyright (c) 2020 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#

import json
import requests

HAS_PANDAS = False
try:
    import pandas as pd
    HAS_PANDAS = True
except:
    pass 


def show_versions():
    # print out some version debug stuff... 
    # see sklearn.show_versions() for inspiration
    print("Need to implement show_version!")
    return    

class DataCulpaValidator:
    HTTP = "http"
    HTTPS = "https"
    # FIXME: http, message queues, etc.

    def __init__(self, 
                 protocol, 
                 dc_host, 
                 dc_port=7777, 
                 api_access_id=None, 
                 api_secret=None):
        self.protocol = protocol
        #if self.protocol == self.HTTP:
        #    assert dc_host == "localhost", "HTTP is only supported for localhost"
        #else:
        #assert self.protocol == self.HTTPS, "invalid protocol: only https is supported in this release"

        self.host = dc_host
        self.port = dc_port
        self.api_access_id = api_access_id
        self.api_secret = api_secret

    def test_connection(self):
        url = self._get_base_url() + "test/connection"
        r = requests.get(url=url,
                         headers=self._json_headers())
        if r.status_code != 200:
            print("got status code %s for %s" % (r.status_code, url))
            return 1

        try:
            jr = json.loads(r.content)
            if jr.get('status') is None:
                print("missing status")
                return 1
        except:
            print("Error parsing result: __%s__", r.content)
            return 1

        # FIXME: needs more error handling.
        return 0

    def _get_base_url(self):
        return "%s://%s:%s/" % (self.protocol, self.host, self.port)

    def _build_pipeline_url_suffix(self,
                                   pipeline_name, 
                                   pipeline_environment, 
                                   pipeline_stage, 
                                   pipeline_version):
        s = "%s/%s/%s/%s" % (pipeline_name, pipeline_environment, pipeline_stage, pipeline_version)
        return s

    def _json_headers(self):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        return headers

    def queue_record(self,
                    record,
                    pipeline_name, 
                    pipeline_environment="default",
                    pipeline_stage="default",
                    pipeline_version="default",
                    extra_metadata=None):
        
        assert isinstance(record, dict), "record must be a dict"
        assert isinstance(pipeline_name, str), "pipeline_name must be a string"
        if extra_metadata is not None:
            assert isinstance(extra_metadata, dict), "extra_metadata must be a dict"
        # endif

        suffix = self._build_pipeline_url_suffix(pipeline_name, 
                                                 pipeline_environment,
                                                 pipeline_stage,
                                                 pipeline_version)   
        path = "queue/enqueue/" + suffix
 
        rs_str = json.dumps(record)
        r = requests.post(url=self._get_base_url() + path, 
                          data=rs_str, 
                          headers=self._json_headers())
        try:
            jr = json.loads(r.content)
            return jr.get('queue_id'), jr.get('queue_count'), jr.get('queue_age')
        except:
            print("Error parsing result: __%s__", r.content)
        return (None, 0, 0)

    def queue_commit(self, queue_id):
        path = "queue/commit/%s" % queue_id
        url = self._get_base_url() + path
        r = requests.post(url=url, 
                          data="", 
                          headers=self._json_headers())
        try:
            jr = json.loads(r.content)
            return jr
        except:
            print("Error parsing result: __%s__", r.content)
        return None

    def validation_status(self, queue_id):
        path = "validation/status/%s" % queue_id
        url = self._get_base_url() + path
        r = requests.get(url=url, headers=self._json_headers())
        if r.status_code != 200:
            return "Error"

        try:
            jr = json.loads(r.content)
            return jr
        except:
            print("Error parsing result: __%s__", r.content)
        return None
