#
# DataCulpaApiNow.py
# Data Culpa Validator Python Client
#
# Copyright (c) 2020-2022 Data Culpa, Inc.
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

import os

#
# pip3 install dataculpa-client
#  
import dataculpa
from dataculpa import DataCulpaValidator, DataCulpaWatchpointNotDefined, DataCulpaServerError

# N.B.: The Data Culpa library uses the python logging library and you may have opinions on how to shape that.

# You will need to create a new user to use this script in the Validator UI
# MultiGear > Users > Add
DC_USER = os.environ['DC_API_USER']
DC_SECRET = os.environ['DC_API_SECRET']

DC_HOST = os.environ['DC_HOST'] # domain only; no https:// prefix or port
DC_PORT = 7778

def main():
    # Connect to Validator
    dc = DataCulpaValidator(watchpoint_name=None, #"watchpoint-name",
                            # Can also specify other watchpoint metadata here to specify a more specific watchpoint.
                            # Specifying a name here will create a new watchpoint if it does not exist;
                            # pass None for the watchpoint_name and use setWatchpointName() to specify a name
                            # without creating the watchpoint.
                            protocol=DataCulpaValidator.HTTPS,
                            dc_host=DC_HOST,
                            dc_port=DC_PORT,
                            api_access_id=DC_USER,
                            api_secret=DC_SECRET)
    
    res = dc.getWatchpointVariations("watchpoint-name2")
    # res = [{'context': 'default',
    #         'create_time': '2022-04-06T04:29:52.075885',
    #         'name': 'watchpoint-name',
    #         'stage': 'default',
    #         'version': 'default'}]
    # dc.setWatchpointName(res[0].get('name'))

    # Some undercover verification:
    # watchpoint_id = dc._get_pipeline_id()
    # dc._pipeline_id = XX      # Can set or check this to ensure the id matches the URL in the Validator UI

    # Get a 0/1 that things are working.
    rc = dc.test_connection()
    print(rc)

    try:
        dc.run_now(print_debug=True)        # returns nothing
    except Exception as err:
        # can raise DataCulpaWatchpointNotDefined
        #           DataCulpaServerError -- errors such as invalid id, built-in connector not configured, etc.
        print("Exception:", err)
    



if __name__ == "__main__":
    main()


