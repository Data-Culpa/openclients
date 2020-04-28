

from dataculpa import DataCulpaHttpRest
from datetime import datetime

import random
import socket

import json

def main():

    dc = DataCulpaHttpRest("localhost", 7777)

    # dc.test_connection()

    d = { 'app_name': 'basic test',
          'hostname': socket.gethostname(),
          'run_time': str(datetime.now()),
          'random_value': random.random() }

    dc.validate_blocking("test-pipeline", [d])
    return

if __name__ == "__main__":
    main()
