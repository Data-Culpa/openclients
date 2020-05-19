#
# __init__.py
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

class DataCulpaValidator:
    HTTP = "http"

    def __init__(self, protocol, dc_host, dc_port=7777):
        self.protocol = protocol
        assert self.protocol == self.HTTP, "invalid protocol: only http is supported in this release"

        self.host = dc_host
        self.port = dc_port

        # HTTPS vs HTTP
        # HTTP proxies
        # Lots of things to add here.

        # FIXME: secrets handling.


    def test_connection(self):
        # FIXME: need to do something useful.
            # maybe also verify we can login, etc.
        raise Exception("Not implemented")

    def _get_base_url(self):
        return "http://%s:%s/" % (self.host, self.port)

    def validate_blocking(self, 
                          record_set,
                          pipeline_name, 
                          pipeline_stage="default",
                          pipeline_environment="default",
                          metadata=None):

        rs_str = json.dumps(record_set)

        # call the DC server, wait for feedback
        path = "validate/" + pipeline_name
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url=self._get_base_url() + path, 
                          data=rs_str, 
                          headers=headers)
        return r.content
    
    def validate_async(self,
                       pipeline_name, 
                       pipeline_stage="default",
                       pipeline_environment="default",
                       record_set,
                       extra_metadata):
        # call and immediately return, nhttpot waiting for server connection.
        # FIXME: timeouts on network connectivity, etc.
        job_id = "not implemented"
        return job_id

    def queue_record(self,
                    pipeline_name, 
                    pipeline_stage="default",
                    pipeline_environment="default",
                    data_item,
                    extra_metadata):

        return

    def queue_interim_validate(self,
                               pipeline_name,
                               pipeline_stage="default",
                               pipeline_environment="default"):
        return

    def queue_commit(self,
                     pipeline_name,
                     pipeline_stage="default",
                     pipeline_environment="default"):
        
        return

    def validate_update(self,
                        job_id,
                        additional_metadata=None,
                        is_finished=False):
        "not implemented"
        return

    def validate_complete(self, job_id):
        """This is the same as calling validate_update(job_id, is_finished=True)"""
        return self.validate_update(job_id, is_finished=True)
    