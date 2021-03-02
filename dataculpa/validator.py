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

import base64
import json
import os
import requests
import logging
import traceback

from datetime import datetime

HAS_PANDAS = False
try:
    import pandas as pd
    HAS_PANDAS = True
except:
    pass 

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

def show_versions():
    # print out some version debug stuff... 
    # see sklearn.show_versions() for inspiration
    logging.error("Need to implement show_version!")
    return    

class PipelineAlertConfig:
    def __init__(self):
        pass


class DataCulpaValidator:
    HTTP = "http"
    HTTPS = "https"
    # FIXME: http, message queues, etc.

    def __init__(self, 
                 pipeline_name, 
                 pipeline_environment="default",
                 pipeline_stage="default",
                 pipeline_version="default",
                 protocol="http", 
                 dc_host="localhost", 
                 dc_port=7777, 
                 api_access_id=None, 
                 api_secret=None,
                 queue_window=20):

        assert isinstance(pipeline_name, str), "pipeline_name must be a string"

        self.pipeline_name        = pipeline_name
        self.pipeline_environment = pipeline_environment
        self.pipeline_stage       = pipeline_stage
        self.pipeline_version     = pipeline_version

        self.protocol = protocol
        #if self.protocol == self.HTTP:
        #    assert dc_host == "localhost", "HTTP is only supported for localhost"
        #else:
        #assert self.protocol == self.HTTPS, "invalid protocol: only https is supported in this release"

        self.host = dc_host
        self.port = dc_port
        self.api_access_id = api_access_id
        self.api_secret = api_secret
        self.queue_window = queue_window
        self._queue_buffer = []

        self._queue_errors = []
        self._queue_id = None
        self._queue_count = 0


    def __del__(self):
        logging.debug("DataCulpaValidator destructor called")


    def test_connection(self):
        url = self._get_base_url() + "test/connection"
        r = requests.get(url=url,
                         headers=self._json_headers())
        if r.status_code != 200:
            logging.error("got status code %s for %s" % (r.status_code, url))
            return 1

        try:
            jr = json.loads(r.content)
            if jr.get('status') is None:
                logging.error("missing status")
                return 1
        except:
            logging.error("Error parsing result: __%s__", r.content)
            return 1

        # FIXME: needs more error handling.
        return 0

    def _get_base_url(self):
        return "%s://%s:%s/" % (self.protocol, self.host, self.port)

    def _whack_str(self, s):
        return base64.urlsafe_b64encode(s.encode('utf-8')).decode('utf-8')

    def _build_pipeline_url_suffix(self):
        s = "%s/%s/%s/%s" % (self._whack_str(self.pipeline_name), 
                             self._whack_str(self.pipeline_environment), 
                             self._whack_str(self.pipeline_stage), 
                             self._whack_str(self.pipeline_version))
        return s

    def _json_headers(self):
        headers = {'Content-type': 'application/json',
                   'Accept': 'text/plain',
                   
                   }
        return headers

    def _csv_batch_headers(self, file_name):
        headers = {'Content-type': 'text/csv', 
                   'Accept': 'text/plain',
                   'X-agent': 'dataculpa-library',
                   'X-data-type': 'csv',
                   'X-batch-name': base64.urlsafe_b64encode(file_name.encode('utf-8'))
                   }
        return headers

    def load_csv_file(self, file_name):
        """
        Send the raw file contents to Validator and commit the queue.
        """
        suffix = self._build_pipeline_url_suffix()   
        path = "batch-validate/" + suffix
        post_url = self._get_base_url() + path

        timeout = 10
        # if the file is big, allow more time... need something smarter here.
        file_sz = os.stat(file_name).st_size
        if file_sz > (1024 * 1024 * 1024):
            timeout = 60
        elif file_sz > (1024 * 1024):
            timeout = 30
        # endif

        try:
            with open(file_name, "rb") as csv_file:
                r = requests.post(url=post_url, 
                                files={file_name: csv_file}, 
                                headers=self._csv_batch_headers(file_name),
                                timeout=timeout) # 10 second timeout.
                #r.raise_for_status() # turn HTTP errors into exceptions -- 
            self._queue_buffer = []

            return True # worked.
        except requests.exceptions.Timeout:
            logging.error("timed out trying to load csv file...")
#        except requests.exceptions.RetryError:
        except requests.exceptions.HTTPError as err:
            logging.error("got an http error: %s" % err)
        except requests.RequestException as e:
            logging.error("got an request error... server down?: %s" % e)
        except:
            logging.error("got some other error:")
 
        return False # got an error.

    def load_flat_array(self, a):
        """
        Batch load the array of data, e.g., data read in from a csv.reader() call.

        Note that this doesn't support hierarchical data; for that use queue_record calls.
        """
        print("load_array() NOT IMPLEMENTED")

        return

    def queue_metadata(self, queue_id, meta):
        # FIXME: maybe consider queuing locally and then flushing when the user calls commit()?
        # 
        assert isinstance(meta, dict), "meta must be a dict"
        
        if queue_id is None:
            queue_id = self._queue_alloc()

        path = "queue/metadata/%s" % queue_id
        url = self._get_base_url() + path

        rs_str = json.dumps(meta, cls=json.JSONEncoder, default=str)
        r = requests.post(url=url, 
                          data=rs_str, 
                          headers=self._json_headers())
        try:
            jr = json.loads(r.content)
            return (queue_id, jr)
        except:
            self._append_error("Error parsing result: __%s__" % r.content)

        return

    def _queue_alloc(self):
        """Returns a queue_id"""
        suffix = self._build_pipeline_url_suffix()   
        path = "queue/alloc/" + suffix

        post_url = self._get_base_url() + path
        logging.debug("%s: about to alloc queue" % (datetime.now(),))

        try:
            r = requests.post(url=post_url, 
                              data="", 
                              headers=self._json_headers(),
                              timeout=10.0) # 10 second timeout.
        except:
            # FIXME: need to handle ConnectionRefusedError and so on!
            logging.info("Probably got a time out...") 
            traceback.print_exc()
        # endtry

        try:
            jr = json.loads(r.content)
            self._queue_id = jr.get('queue_id')
            #return jr.get('queue_id'), jr.get('queue_count'), jr.get('queue_age')
            # FIXME: improve error handling.
            return self._queue_id
        except:
            logging.debug("Error parsing result: __%s__" % r.content)

        logging.warn("queue alloc failed")
        return None # (None, 0, 0)

  


    def queue_record(self,
                    record,
                    jsonEncoder=json.JSONEncoder):
        assert isinstance(record, dict), "record must be a dict"
        self._jsonEncoder = jsonEncoder
        self._queue_buffer.append(record)
        if len(self._queue_buffer) >= self.queue_window:
            return self._flush_queue()
        return None, 0, 0
    
    def _append_error(self, message):
        self._queue_errors.append( { 'when': datetime.utcnow(), 'message': message })
        return

    def _flush_queue(self):
        suffix = self._build_pipeline_url_suffix()   
        path = "queue/enqueue/" + suffix

        rs_str = json.dumps(self._queue_buffer, cls=self._jsonEncoder, default=str)
        post_url = self._get_base_url() + path
        logging.debug("%s: about to post %d bytes to %s" % (datetime.now(), len(rs_str), post_url))

        try:
            r = requests.post(url=post_url, 
                              data=rs_str, 
                              headers=self._json_headers(),
                              timeout=10.0) # 10 second timeout.
            self._queue_buffer = []
        except:
            # FIXME: need to handle ConnectionRefusedError and so on!
            logging.info("Probably got a time out...") 
            traceback.print_exc()
            # maybe set an error/increment an error counter/etc.
            #return None, 0, 0
#            print("%s: done with post" % (datetime.now(),))

        try:
            jr = json.loads(r.content)
            self._queue_id = jr.get('queue_id')
            #return jr.get('queue_id'), jr.get('queue_count'), jr.get('queue_age')
            # FIXME: improve error handling.
        except:
            logging.debug("Error parsing result: __%s__", r.content)

        return # (None, 0, 0)

    def queue_commit(self):
        """Returns the queue_id if successful -- so that we can query for validation status on that queue_id.
           By the time this returns, the queue has been closed--so the only utility is to query for validation status
        """
        if self._queue_id is None and len(self._queue_buffer) == 0:
            self._append_error("queue_commit called but no data sent or queued to send")
            return
        
        if len(self._queue_buffer) != 0:
            self._flush_queue()
        
        queue_id = self._queue_id
        assert queue_id is not None

        self._queue_id = None

        path = "queue/commit/%s" % queue_id
        url = self._get_base_url() + path
        r = requests.post(url=url, 
                          data="", 
                          headers=self._json_headers())
        try:
            jr = json.loads(r.content)
            return (queue_id, jr)
        except:
            self._append_error("Error parsing result: __%s__" % r.content)

        self._queue_id = None
        return (queue_id, None)

    def get_queue_id(self):
        return self._queue_id

    #def force_flush_if_needed_and_get_queue_id(self):
    #    if self._queue_id is None:
    #        if len(self._queue_buffer) > 0:
    #            self._flush_queue()
    #    return self._queue_id

    def validation_status(self, queue_id, wait_for_processing=False):
        path = "validation/status/%s" % queue_id
        url = self._get_base_url() + path
        r = requests.get(url=url, headers=self._json_headers())
        if r.status_code != 200:
            return "Error"

        try:
            jr = json.loads(r.content)
            return jr
        except:
            logging.error("Error parsing result: __%s__", r.content)
        return None
