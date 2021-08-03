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

from dataculpa import DataCulpaValidator
from datetime import datetime

import random
import socket 
import sys
import time

def main():

    dc = DataCulpaValidator("client-3",
                            protocol=DataCulpaValidator.HTTP,
                            dc_host="192.168.1.65", 
                            dc_port=7778)

    # dc.test_connection()

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
    return

if __name__ == "__main__":
    main()
