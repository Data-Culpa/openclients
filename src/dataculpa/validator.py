#
# validator.py
# Data Culpa Validator Python Client
#
# Copyright (c) 2020-2021 Data Culpa, Inc.
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

logger = logging.getLogger('dataculpa')
#logger.basicConfig(format='%(asctime)s %(message)s', level=logger.DEBUG)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('dataculpa-client.log')
c_handler.setLevel(logging.WARNING)
f_handler.setLevel(logging.ERROR)

# Create formatters and add it to handlers
c_format = logging.Formatter('dataculpa %(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

class DataCulpaConnectionError(Exception):
    def __init__(self, url, message):
        logger.error("Connection error for url %s: __%s__" % (url, message))
        super().__init__("Connection error for URL %s: __%s__" % (url, message))

class DataCulpaServerResponseParseError(Exception):
    def __init__(self, url, payload):
        logger.error("Error parsing result from url %s: __%s__" % (url, payload))
        super().__init__("Bad response from URL %s: __%s__" % (url, payload))

class DataCulpaBadServerCodeError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = "Unexpected status code %s: %s" % (status_code, message)
        super().__init__(self.message)

def log_error(s):
    return

def log_info(s):
    return

def log_debug(s):
    return

def show_versions():
    # print out some version debug stuff... 
    # see sklearn.show_versions() for inspiration
    logger.error("Need to implement show_version!")
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
                 queue_window=20,
                 timeshift=None):

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
        self._queue_ready = False

        self._queue_errors = []
        self._queue_id = None
        self._queue_count = 0

        self._timeshift = timeshift

        self._open_queue()

        self._pipeline_id = None

    def GET(self, url, headers=None):
        if headers is None:
            headers = self._json_headers()

        try:
            r = requests.get(url=url, headers=headers)
            if r.status_code != 200:
                raise DataCulpaBadServerCodeError(r.status_code, "url was %s" % url)
            return r
        except requests.exceptions.Timeout:
            raise DataCulpaConnectionError(url, "timed out")
        except requests.exceptions.HTTPError as err:
            raise DataCulpaConnectionError(url, "http error: %s" % err)
        except requests.RequestException as e:
            raise DataCulpaConnectionError(url, "request error: %s" % e)
        except BaseException as e:
            raise DataCulpaConnectionError(url, "unexpected error: %s" % e)
        return None

    def POST(self, url, data, timeout=10.0, headers=None):
        if headers is None:
            headers = self._json_headers()

        try:
            r = requests.post(url=url,
                              data=data,
                              timeout=timeout,
                              headers=headers)
            if r.status_code != 200:
                raise DataCulpaBadServerCodeError(r.status_code, "url was %s" % url)
            return r
        except requests.exceptions.Timeout:
            raise DataCulpaConnectionError(url, "timed out")
        except requests.exceptions.HTTPError as err:
            raise DataCulpaConnectionError(url, "http error: %s" % err)
        except requests.RequestException as e:
            raise DataCulpaConnectionError(url, "request error: %s" % e)
        except BaseException as e:
            raise DataCulpaConnectionError(url, "unexpected error: %s" % e)
        return None

    def _parseJson(self, url, js_str):
        try:
            jr = json.loads(js_str)
        except:
            raise DataCulpaServerResponseParseError(url, js_str)
        return jr

    def __del__(self):
        logger.debug("DataCulpaValidator destructor called")

    def test_connection(self):
        url = self._get_base_url() + "test/connection"
        r = self.GET(url)
        jr = self._parseJson(url, r.content)

        if jr.get('status') is None:
            logger.error("missing status")
            return 1

        # FIXME: needs more error handling.
        return 0

    def _get_pipeline_id(self):
        if self._pipeline_id is None:
            url = self._get_base_url("data/metadata/pipeline-id/") +  + self._build_pipeline_url_suffix()
            r = self.GET(url)
            jr = self._parseJson(url, r.content)
            self._pipeline_id = jr.get('id')

        return self._pipeline_id


    def get_config(self):
        _id = self._get_pipeline_id()

        if _id is not None:
            # now we can get the config...
            config_url = self._get_base_url("data/metadata/pipeline-config/%s" % str(_id))

            r = self.GET(config_url)
            jr = self._parseJson(config_url, r.content)
            return jr

        return None

    def get_recent_batchnames(self):
        _id = self._get_pipeline_id()

        if _id is not None:
            # now we can get the config...
            config_url = self._get_base_url("data/recent_batch_names/%s" % str(_id))

            r = self.GET(config_url)
            jr = self._parseJson(config_url, r.content)
            return jr

        return None

    def set_use_gold(self, batch_entry):
        raise NotImplementedError

    # add a new call for setting the gold standard configuration item...but to what?
    # we need the list of potential files we could use.

    def _get_base_url(self, s=None):
        if s is None:
            return "%s://%s:%s/" % (self.protocol, self.host, self.port)

        return "%s://%s:%s/%s" % (self.protocol, self.host, self.port, s)

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
        post_url = self._get_base_url("batch-validate/%s" % self._queue_id)

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
                                  timeout=timeout) # variable
                #r.raise_for_status() # turn HTTP errors into exceptions -- 
            self._queue_buffer = []

            return True # worked.
        except requests.exceptions.Timeout:
            logger.error("timed out trying to load csv file...")
#        except requests.exceptions.RetryError:
        except requests.exceptions.HTTPError as err:
            logger.error("got an http error: %s" % err)
        except requests.RequestException as e:
            logger.error("got an request error... server down?: %s" % e)
        except BaseException as e:
            logger.error("got some other error: %s" % e)
 
        return False # got an error.

    def load_flat_array(self, a):
        """
        Batch load the array of data, e.g., data read in from a csv.reader() call.

        Note that this doesn't support hierarchical data; for that use queue_record calls.
        """
        raise NotImplementedError
        
    def queue_metadata(self, meta):
        # FIXME: maybe consider queuing locally and then flushing when the user calls commit()?
        # 
        assert isinstance(meta, dict), "meta must be a dict"
        
        queue_id = None
        if self._queue_id is None:
            self._open_queue()

        if self._queue_id is not None:
            queue_id = self._queue_id
        else:
            self._append_error("No queue object found")
            raise DataCulpaConnectionError

        path = "queue/metadata/%s" % queue_id
        url = self._get_base_url() + path

        rs_str = json.dumps(meta, cls=json.JSONEncoder, default=str)
        r = self.POST(url, rs_str)
        jr = self._parseJson(url, r.content)
        return (queue_id, jr)

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
    
    def has_errors(self):
        # FIXME: we're very inconsistent on how we deal with errors right now.
        return self._queue_errors != []
    
    def get_errors(self):
        return self._queue_errors

    def _calc_timeshift_seconds(self):
        ts = self._timeshift
        if ts is None:
            return 0
        
        # if it's an int, pass that
        if type(ts) == int or type(ts) == float:
            return ts

        # if it's a datetime, calculate the delta to now in seconds.
        if isinstance(ts, datetime):
            # if we got in a timezone, use it... otherwise assume local timezone.
            now = None
            if ts.tzinfo is not None:
                now = datetime.now(ts.tzinfo)
            else:
                now = datetime.now()
            # endif
            
            dt = now - ts
            if dt is not None:
                dt = int(dt.total_seconds())
                return dt

        return 0


    def _open_queue(self):
        j = { 
                'pipeline' : self.pipeline_name,
                'context'  : self.pipeline_environment,
                'stage'    : self.pipeline_stage,
                'version'  : self.pipeline_version,
                'timeshift': self._calc_timeshift_seconds()
        }

        rs_str = json.dumps(j, cls=json.JSONEncoder, default=str)
        post_url = self._get_base_url("queue/open")

        r = self.POST(post_url, rs_str)
        self._queue_buffer = []
        self._queue_ready = True

        jr = self._parseJson(post_url, r.content)
        self._queue_id = jr.get('queue_id')

        return # (None, 0, 0)

    def _flush_queue(self):
        if self._queue_id is None:
            self._open_queue()
            assert self._queue_id is not None, "unable to open a queue"

        rs_str = json.dumps(self._queue_buffer, cls=self._jsonEncoder, default=str)
        post_url = self._get_base_url("queue/enqueue/%s" % self._queue_id)

        r = self.POST(url=post_url, 
                      data=rs_str)
        self._queue_buffer = []

        jr = self._parseJson(post_url, r.content)
        # FIXME: improve error handling.

        return # (None, 0, 0)

    def queue_commit(self):
        """Returns the queue_id if successful -- so that we can query for validation status on that queue_id.
           By the time this returns, the queue has been closed--so the only utility is to query for validation status
        """
        #if self._queue_id is None and len(self._queue_buffer) == 0:
        #    self._append_error("queue_commit called but no data sent or queued to send")
        #    return (None, None)
        
        if len(self._queue_buffer) != 0:
            self._flush_queue()
        # endif

        queue_id = self._queue_id
        assert queue_id is not None

        self._queue_id = None
        url = self._get_base_url("queue/commit/%s" % queue_id)
        r = self.POST(url, "")
        jr = self._parseJson(url, r.content)
        return (queue_id, jr)

    def get_queue_id(self):
        return self._queue_id

    def validation_status(self, queue_id, wait_for_processing=False):
        url = self._get_base_url("validation/status/%s" % queue_id)

        r = self.GET(url)
        jr = self._parseJson(url, r.content)
        return jr

    def query_alerts(self):
        # get alerts for this pipeline
        pid = self._get_pipeline_id()
        assert pid is not None, "FIXME: return an error"
        url = self._get_base_url("data/metadata/alerts/%s" % pid)

        r = self.GET(url)
        jr = self._parseJson(url, r.content)
        return jr


    def query_summary_data(self):
        # get the summary for this pipeline 
        return

    