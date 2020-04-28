#
# __init__.py
# Data Culpa Python Client
#
# Copyright © 2020 Data Culpa, Inc. All rights reserved.
#

import json
import requests

class DataCulpaHttpRest:

    def __init__(self, dc_host, dc_port=7777):
        self.host = dc_host
        self.port = dc_port
        # HTTPS vs HTTP
        # HTTP proxies
        # Lots of things to add here.

    def test_connection(self):
        # FIXME: need to do something useful.
            # maybe also verify we can login, etc.
        raise Exception("Not implemented")

    def _get_base_url(self):
        return "http://%s:%s/" % (self.host, self.port)

    def validate_blocking(self, pipeline_name, record_set):

        rs_str = json.dumps(record_set)

        # call the DC server, wait for feedback
        path = "validate/" + pipeline_name
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url=self._get_base_url() + path, data=rs_str, headers=headers)
        print("RESPONSE: ", r)
        return
    
    def validate_async(self, pipeline_name, record_set):
        # call and immediately return, not waiting for server connection.
        # FIXME: timeouts on network connectivity, etc.
        return

