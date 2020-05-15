#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# basic.py
# Data Culpa Open Clients
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

from dataculpa import DataCulpaValidator
from datetime import datetime

import random
import socket

import json

def main():

    dc = DataCulpaValidator(DataCulpaValidator.HTTP,
                            "192.168.1.13", 
                            7777)

    # dc.test_connection()

    d = { 'app_name': 'basic test',
          'hostname': socket.gethostname(),
          'run_time': str(datetime.now()),
          'random_value': random.random() }

    dc.validate_blocking([d], "test-pipeline")
    return

if __name__ == "__main__":
    main()
