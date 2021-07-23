#!/usr/bin/python3

import json
import cgi
import re
from random import choice
from db_conn_module import *
from datetime import datetime, date
from time import strftime, gmtime
import os
import traceback
from logging_module import add_log
import time
import sys
import html
import gzip


def get_url_category(line):

    # https://regex101.com/r/zHZmt5/5
    pattern = r"(?P<url>(?P<state_abb>[^\/]{2})/(?P<city_url>[^\/]+)/(?P<category>spa|massage)\/)"
    match = re.search(pattern, line)
    if match is None:
        return None
    url_info_category = match
    return url_info_category


def get_url(line):

    # https://regex101.com/r/QXZEE5/3
    pattern = r" /(?P<url>[a-z\/\-0-9\.]*) HTTP"
    match = re.search(pattern, line)
    if match is None:
        return None
    url = match["url"]
    return url


try:

    log_file = 'bestprosintown.com-Feb-2021'

    log_file1 = 'loc8nearme.com-Mar-2021'

    log = open(log_file1)

    count = 0

    for line in log:

        if "http://www.google.com/bot.html" in line:
            continue

        if "+http://www.google.com/adsbot.html" in line:
            continue

        if ('google.com' or 'google.ca') not in line:
            continue

        url = get_url(line)
        if url == '':
            print(f'Url - {url}')
            print(f"line - {line}")

    #     category_info = get_url_category(line)
    #     if category_info is not None:
    #         print(line)
    #
    #         count += 1
    #
    # print(f'Count - {count}')

except Exception as err:
    print(err)
