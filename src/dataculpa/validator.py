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
import logging 
import os
import requests
import sys
import time
import traceback

from datetime import datetime
from dateutil.parser import parse as DateUtilParse

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
        #logger.error("Connection error for url %s: __%s__" % (url, message))
        super().__init__("Connection error for URL %s: __%s__" % (url, message))

class DataCulpaServerResponseParseError(Exception):
    def __init__(self, url, payload):
        #logger.error("Error parsing result from url %s: __%s__" % (url, payload))
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


class DataCulpaValidator:
    HTTP = "http"
    HTTPS = "https"
    # FIXME: http, message queues, etc.

    def __init__(self, 
                 watchpoint_name, 
                 watchpoint_environment="default",
                 watchpoint_stage="default",
                 watchpoint_version="default",
                 protocol="http", 
                 dc_host="localhost", 
                 dc_port=7777, 
                 api_access_id=None, 
                 api_secret=None,
                 queue_window=20,
                 timeshift=None):

        if api_access_id is None or api_secret is None:
            sys.stderr.write("Warning: api_access_id is required with Validator 1.1 and later.\n")

        self.api_access_token = None

        self.watchpoint_name        = watchpoint_name
        self.watchpoint_environment = watchpoint_environment
        self.watchpoint_stage       = watchpoint_stage
        self.watchpoint_version     = watchpoint_version

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

        if self.watchpoint_name is not None:
            self._open_queue()

        self._pipeline_id = None

    def setWatchpointName(self, watchpoint_name):
        assert self.watchpoint_name is None
        self.watchpoint_name = watchpoint_name
        return

    def getWatchpointVariations(self, watchpoint_name):
        self._login_if_needed()

        pName = base64.urlsafe_b64encode(watchpoint_name.encode('utf-8')).decode('utf-8')
        url = "%s://%s:%s/%s" % (self.protocol, self.host, self.port, "data/metadata/watchpoint-variations/%s" % pName)

        r = self.GET(url)
        
        try:
            jr = DataCulpaValidator._parseJson(url, r.content)
            if jr is None:
                return []
        except:
            raise DataCulpaServerResponseParseError
        
        return jr

    def GET(self, url, headers=None, stream=False):
        if headers is None:
            headers = self._json_headers()

        try:
            r = requests.get(url=url, headers=headers, stream=stream)
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
            if isinstance(e, DataCulpaBadServerCodeError):
                raise e # Don't wrap the exception we just made above.
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
                new_text = r.text
                # Chop off the annoying HTML nonsense if it's there..
                if new_text.startswith("<!DOCTYPE"):
                    p_pos = new_text.find("<p>")
                    if p_pos >= 0:
                        new_text = new_text[p_pos + 3:]
                        p_pos = new_text.find("</p>")
                        if p_pos > 0:
                            new_text = new_text[:p_pos]
                raise DataCulpaBadServerCodeError(r.status_code, "url was %s; text = %s" % (r.url, new_text))
            return r
        except requests.exceptions.Timeout:
            raise DataCulpaConnectionError(url, "timed out")
        except requests.exceptions.HTTPError as err:
            raise DataCulpaConnectionError(url, "http error: %s" % err)
        except requests.RequestException as e:
            raise DataCulpaConnectionError(url, "request error: %s" % e)
        except BaseException as e:
            if isinstance(e, DataCulpaBadServerCodeError):
                raise e # Don't wrap the exception we just made above.
            raise DataCulpaConnectionError(url, "unexpected error: %s" % e)
        return None

    @classmethod
    def _parseJson(cls, url, js_str):
        try:
            jr = json.loads(js_str)
        except:
            raise DataCulpaServerResponseParseError(url, js_str)
        return jr

    def _json_headers(self):
        headers = {'Content-type': 'application/json',
                   'Accept': 'text/plain'
                   }

        if self.api_access_token is not None:
            headers['Authorization'] = 'Bearer %s' % self.api_access_token

        return headers

    def __del__(self):
        logger.debug("DataCulpaValidator destructor called")

    def test_connection(self):
        """ 
        Returns 0 if all is well, or returns 1 if the connection worked but we didn't get a response we liked.
        Otherwise will raise a variety of exceptions.
        """
        url = self._get_base_url("test/connection")
        r = self.GET(url)
        jr = self._parseJson(url, r.content)

        if jr.get('status') is None:
            logger.error("missing status")
            return 1

        # FIXME: needs more error handling.
        return 0

    def _whack_str(self, s):
        s = str(s)
        return base64.urlsafe_b64encode(s.encode('utf-8')).decode('utf-8')

    def _build_pipeline_url_suffix(self):
        s = "%s/%s/%s/%s" % (self._whack_str(self.watchpoint_name), 
                             self._whack_str(self.watchpoint_environment), 
                             self._whack_str(self.watchpoint_stage), 
                             self._whack_str(self.watchpoint_version))
        return s

    def _get_pipeline_id(self):
        if self._pipeline_id is None:
            url = self._get_base_url("data/metadata/pipeline-id/") + self._build_pipeline_url_suffix()
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
            if jr.get('id') is None:
                jr['id'] = _id
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

    def set_use_gold(self, yesno, batch_entry=None):
        _id = self._get_pipeline_id()
        if _id is not None:
            existing_config = self.get_config()
            config_url = self._get_base_url("data/metadata/pipeline-config/%s" % str(_id))

            p = existing_config
            if yesno:
                assert batch_entry is not None, "batch_entry must be set to an element in get_recent_batchnames() if use_gold is True"
                p['pipeline_config'] = { 'uses_gold': True, 'gold_batch_names': [ batch_entry ]}
            else:
                p['pipeline_config'] = { 'uses_gold': False }
            js = json.dumps(p)
            r = self.POST(config_url, js)
            jr = self._parseJson(config_url, r.content)
            return jr

        return False

    # add a new call for setting the gold standard configuration item...but to what?
    # we need the list of potential files we could use.

    def _get_base_url(self, s=None):
        if s is None:
            return "%s://%s:%s/" % (self.protocol, self.host, self.port)

        return "%s://%s:%s/%s" % (self.protocol, self.host, self.port, s)

    def _csv_batch_headers(self, file_name):
        headers = {'Content-type': 'text/csv', 
                   'Accept': 'text/plain',
                   'X-agent': 'dataculpa-library',
                   'X-data-type': 'csv',
                   'X-batch-name': base64.urlsafe_b64encode(file_name.encode('utf-8'))
                   }

        if self.api_access_token is not None:
            headers['Authorization'] = 'Bearer %s' % self.api_access_token
        
        return headers

    def load_parquet(self, file_name):
        headers = {'Content-type': 'application/octet-stream', 
                   'Accept': 'text/plain',
                   'X-agent': 'dataculpa-library',
                   'X-data-type': 'parquet',
                   'X-batch-name': base64.urlsafe_b64encode(file_name.encode('utf-8'))
                   }

        if self.api_access_token is not None:
            headers['Authorization'] = 'Bearer %s' % self.api_access_token
        
        return self.load_csv_file(file_name, headers=headers)

    def load_csv_file(self, file_name, headers=None):
        """
        Send the raw file contents to Validator and commit the queue.
        """
        post_url = self._get_base_url("batch-validate/%s" % self._queue_id)

        if headers is None:
            headers = self._csv_batch_headers(file_name)

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
                                  headers=headers,
                                  timeout=timeout) # variable
                #r.raise_for_status() # turn HTTP errors into exceptions -- 
            self._queue_buffer = []

            jr = self._parseJson(post_url, r.content)
            had_error = jr.get('had_error', False)
            if had_error:
                self._append_error(jr)
                return False

            return True
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

    def queue_check_last_record_time(self):
        j = { 
                'pipeline' : self.watchpoint_name,
                'context'  : self.watchpoint_environment,
                'stage'    : self.watchpoint_stage,
                'version'  : self.watchpoint_version
        }

        self._login_if_needed()
        rs_str = json.dumps(j, cls=json.JSONEncoder, default=str)
        post_url = self._get_base_url("queue/timeshift-most-recent")

        r = self.POST(post_url, rs_str)

        jr = self._parseJson(post_url, r.content)

        mt = jr.get('max_time')
        if mt is not None:
            try:
                dt = DateUtilParse(mt)
            except:
                traceback.print_exc()
                return None
            mt = dt
        return mt

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

        assert False, "Unexpected type for timeshift - should an int/float of seconds or a datetime, but got a %s" % type(ts)
        return 0

    def _login_if_needed(self):
        if self.api_access_token is None:
            self.login()
        return

    def _open_queue(self):
        j = { 
                'pipeline' : self.watchpoint_name,
                'context'  : self.watchpoint_environment,
                'stage'    : self.watchpoint_stage,
                'version'  : self.watchpoint_version,
                'timeshift': self._calc_timeshift_seconds()
        }

        self._login_if_needed()
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

    def queue_fastqueue(self, field_name, file_path):
        """Zero-copy: the file-path is on a shared filesystem with Validator"""

        assert len(self._queue_buffer) == 0, "fastqueue cannot be mixed with incremental queueing."

        queue_id = self._queue_id
        assert queue_id is not None

        url = self._get_base_url("queue/fastqueue/%s" % queue_id)
        params = { "field_name": file_path, "field_name": field_name }
        rs_str = json.dumps(params, cls=json.JSONEncoder, default=str)
        r = self.POST(url, rs_str)
        jr = self._parseJson(url, r.content)
        return (queue_id, jr)

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

    def login(self):
        assert self.api_access_id is not None, "need to pass api_access_id and api_secret when creating DataCulpaValidator object"
        assert self.api_secret is not None,    "need to pass api_access_id and api_secret when creating DataCulpaValidator object"

        login_url = self._get_base_url("auth/login")

        p = { "api_key": self.api_access_id, "secret": self.api_secret }
        js = json.dumps(p)
        # need to catch DataCulpaBadServerCodeError and look at 401 here...
        r = self.POST(login_url, js)
        jr = self._parseJson(login_url, r.content)
        self.api_access_token = jr.get('access_token')
        assert self.api_access_token is not None
        return True

    def test_login(self):
        test_url = self._get_base_url("auth/test-login")

    def watchpoint_log_message(self, msg):
        return self.watchpoint_lograw({'message': msg})

    def watchpoint_log_query(self, query_target=None, query_string=None, number_results=None, proc_seconds=None, extras=None):
        d = {}
        assert isinstance(extras, dict)
        if extras is not None:
            d = extras.copy()

        if query_target is not None:
            d['query_target'] = query_target
        if query_string is not None:
            d['query_string'] = query_string
        if number_results is not None:
            d['number_results'] = number_results
        if proc_seconds is not None:
            d['proc_seconds'] = proc_seconds
        return self.watchpoint_lograw(d)

    def watchpoint_lograw(self, data_dict):
        # this is a tool for logging query information, results, or anything else you want your connector
        # to expose to the UI of Validator.
        # recommended fields: query_target, query_string, query_time, number_results
        pipeline_id = self._get_pipeline_id()
        if pipeline_id is None:
            pipeline_id = -1
        url = self._get_base_url("data/watchpoint-log/%s" % pipeline_id)
        params = { "data": data_dict }
        rs_str = json.dumps(params, cls=json.JSONEncoder, default=str)
        r = self.POST(url, rs_str)
        jr = self._parseJson(url, r.content)
        return jr

    def drain_logs(self, llc):
        assert isinstance(llc, LocalLogCache)

        # Note: not thread safe.

        pipeline_id = self._get_pipeline_id()
        if pipeline_id is None:
            pipeline_id = -1

        theList = llc.log_list()
        if len(theList) == 0:
            return

        url = self._get_base_url("data/watchpoint-log-list/%s" % pipeline_id)
        params = {"theList": theList }
        rs_str = json.dumps(params, cls=json.JSONEncoder, default=str)
        r = self.POST(url, rs_str)
        jr = self._parseJson(url, r.content)

        # if we got here, must be ok.
        # empty the llc.
        llc.reset_list()

        return jr

        

class LocalLogCache:
    def __init__(self, logger_object):
        self.logs = []
        self._start_time = time.time()
        self._logger = logger_object # OK to be None
    
    def __del__(self):
        pass # destructor might want to flush things, etc.

    def log_list(self):
        return self.logs

    def reset_list(self):
        self.logs = []
        return

    def _log(self, sev, message, dd=None):
        tn = time.time()
        dt = tn - self._start_time
        if dd is None:
            dd = {}
        
        dd['dc_time_offset'] = dt
        dd['dc_time_now'] = tn
        dd['dc_message'] = message
        dd['dc_sev'] = sev
        self.logs.append(dd)


    def info(self, message):
        if self._logger is not None:
            self._logger.info(message)
        return self._log('info', message)

    def warning(self, message):
        if self._logger is not None:
            self._logger.warning(message)
        return self._log('warning', message)

    def error(self, message):
        if self._logger is not None:
            self._logger.error(message)
        return self._log('error', message)

    def log_sql(self, table, stmt):
        # do something.
        return self._log('info', '%s: %s' % (table, stmt), { 'table': table, 'stmt': stmt })
        