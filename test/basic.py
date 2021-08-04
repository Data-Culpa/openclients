#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# basic.py
# Data Culpa Validator Open Clients
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

from datetime import datetime, timedelta

import os
import random
import socket 
import sys
import time

lib_path = os.path.realpath("../src")
if not os.path.exists(lib_path + "/dataculpa/validator.py"):
    assert False, "I don't know where your cwd is, but it needs to be in test/"
    
sys.path.insert(0, lib_path)

import dataculpa
from dataculpa import DataCulpaValidator

assert dataculpa.__file__.startswith(lib_path)
#print(sys.path)
#print(dataculpa.__file__)


def main():

    dc = DataCulpaValidator("client-3",
                            protocol=DataCulpaValidator.HTTP,
                            dc_host="192.168.1.65", 
                            dc_port=7778)

    rc = dc.test_connection()
    print("TEST CONNECTION:", rc)

    config = dc.get_config()
    assert config is not None
    print("CONFIG:", config)
    assert config.get('id') is not None

    recent_batches = dc.get_recent_batchnames()
    assert recent_batches is not None
    print("RECENT:", recent_batches)


    d = { 'app_name': 'basic test',
          'hostname': socket.gethostname(),
          'run_time': str(datetime.now()),
          'random_value': random.random() }

    # queue_metadata will open a queue if it is not open.
    dc.queue_metadata({ 'meta_field': 'meta_value, wow'} )

    dc.queue_record(d)
    (_id, _content) = dc.queue_commit()
    print("queue_id:", _id)
    print("message: ", _content)
    assert _content.get('had_error') == False, "got an error from the server!"

    allDone = False
    for i in range(10):
        vs = dc.validation_status(_id)
        if vs.get('status', 0) == 100:
            print("done processing: ", vs)
            allDone = True
            break
        time.sleep(1)

    if not allDone:
        print("failed to finish processing")
        sys.exit(2)


    recent_batches = dc.get_recent_batchnames()
    assert recent_batches is not None
    print("RECENT:", recent_batches)

    # FIXME: set use gold.

    # Now load a test CSV file.
    s = \
"""
app_name,hostname,run_time,random_value
test1,localhost,today,6503
test2,localhost,today,4382
test3,localhost,today,5493
test4,,today,3040
"""

    with open("/tmp/test.csv", "w") as fp:
        fp.write(s)
    
    now = datetime.now()
    now = now - timedelta(days=2)
    print(now)
    dc = DataCulpaValidator("client-3",
                            protocol=DataCulpaValidator.HTTP,
                            dc_host="192.168.1.65", 
                            dc_port=7778,
                            timeshift=now)

    rc = dc.test_connection()
    print("TEST CONNECTION:", rc)

    worked_OK = dc.load_csv_file("/tmp/test.csv")
    assert worked_OK == True, "error loading test.csv"



    return

if __name__ == "__main__":
    main()
