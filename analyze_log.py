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
import requests

print('Content-Type: text/html')
print()

# https://www.google.com/search? - реферер с поиска гугла

logger = add_log  # создаем объект для записывания логов

try:
    current_day_date = date.today()
    current_time = datetime.now().time()
    current_time = str(current_time).split('.')[0]  # текущ время без добавления милисекунд

    # if not os.path.exists(f'logs/{current_day_date}'):
    #     os.mkdir(f'logs/{current_day_date}', mode=0o777)     # создаем директорию для сегодняшней даты

    # logs = open(f'logs/{current_day_date}/{current_time}.txt', 'w+', encoding='utf-8')  # создание лог файла
except Exception as er:
    logger(er)

try:
    # сonnection with DATABASE
except Exception as er:
    print(er)
    logger(er)


def stop_check():
    if not os.path.exists('stop.txt'):
        if print_on_page:
            print('file deleted<br>')
            print('<h2>Script stopped</h2>')
        sys.exit(0)


def get_brand_id(connection, id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT brand_id FROM testdb.businesses WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [id])
    if len(rows) > 0:
        brand_id = rows[0][0]
        return brand_id
    else:
        return False


def get_brand_name(connection, brand_id):
    if connection is False:
        return False
    if brand_id is False:
        return False

    sql_query = "SELECT name FROM testdb.brands WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [brand_id])
    if len(rows) > 0:
        brand_name = rows[0][0]
        return brand_name
    else:
        return False


def get_city_id(connection, id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT city FROM testdb.businesses WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [id])
    if len(rows) > 0:
        city_id = rows[0][0]
        return city_id
    else:
        return False


def get_state_id(connection, id):
    if connection is False:
        return False
    if id is False:
        return False

    sql_query = "SELECT state FROM testdb.businesses WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [id])
    if len(rows) > 0:
        state_id = rows[0][0]
        return state_id
    else:
        return False


def get_city_url(connection, city_id):
    if connection is False:
        return False
    if city_id is False:
        return False

    sql_query = "SELECT city_url FROM testdb.cities WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [city_id])
    if len(rows) > 0:
        city_url = rows[0][0]
        return city_url
    else:
        return False


def get_state_abb(connection, state_id):
    if connection is False:
        return False
    if state_id is False:
        return False

    sql_query = "SELECT LOWER(abbreviation) FROM testdb.states WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [state_id])
    if len(rows) > 0:
        abbr = rows[0][0]
        return abbr
    else:
        return False


def get_ip(line):

    # https://regex101.com/r/UAjaBK/1
    pattern = r"^(?P<ip>.*?) "
    match = re.search(pattern, line)
    if match is None:
        return None
    ip = match["ip"]
    return ip


def get_url(line):

    # https://regex101.com/r/QXZEE5/3
    pattern = r" /(?P<url>[a-z\/\-0-9\.]*) HTTP"
    match = re.search(pattern, line)
    if match is None:
        return None
    url = match["url"]
    if url == '':
        return None
    return url


def get_refferer(line):

    # https://regex101.com/r/nvSSyZ/4
    pattern = r" \"(?P<refferer>http[/:a-zA-Z0-9\/\.\-\%\&\=\?\+\_]*)\""
    match = re.search(pattern, line)
    if match is None:
        return None
    ref = match["refferer"]
    return ref


def get_url2(line):

    # https://regex101.com/r/5gKJQd/3
    pattern = r" /(?P<url>(?P<state_abb>[^/]{2})/(?P<city_url>[^/]+))/ "
    match = re.search(pattern, line)
    if match is None:
        return None
    return match


def get_brand_url(line):

    # https://regex101.com/r/WEg1gV/2
    pattern = r"/(?P<url>(?P<state_abb>[a-z]{2})/(?P<city_url>[^\/]+)/(?P<brand_url>[a-z]+(-[a-z]+)*)/)"
    match = re.search(pattern, line)
    if match is None:
        return None
    return match


def get_brand_id_from_url(connection, brand_url):  # ПРОТЕСТИРОВАТЬ
    if connection is False:
        return False
    if brand_url is False:
        return False

    values = (brand_url, )

    sql_query = """SELECT id FROM testdb.brands WHERE brand_url LIKE %s"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        brand_id = rows[0][0]
        return brand_id
    else:
        return False


def get_city_id_by_city_url(connection, state_abb, city_url):
    if connection is False:
        return False
    if city_url is False:
        return False
    if state_abb is False:
        return False

    values = (city_url, state_abb)

    city_info = {}

    sql_query = """SELECT id, state FROM testdb.cities WHERE city_url = %s AND state = 
    (SELECT id FROM testdb.states WHERE abbreviation LIKE %s)"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        city_info["id"] = rows[0][0]
        city_info["state_id"] = rows[0][1]
        return city_info
    else:
        return False


# SELECT * FROM `cities` WHERE city_url = 'turners-falls' AND state = (SELECT id FROM states WHERE abbreviation LIKE 'ma') /* /ma/turners-falls/ */


def get_city_id_by_city_url_loc8(connection, state_name, city_url):
    if connection is False:
        return False
    if city_url is False:
        return False
    if state_name is False:
        return False

    values = (city_url, state_name)

    city_info = {}

    sql_query = """SELECT id, state FROM testdb.cities WHERE city_url = %s AND state = 
    (SELECT id FROM testdb.states WHERE name LIKE %s)"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        city_info["id"] = rows[0][0]
        city_info["state_id"] = rows[0][1]
        return city_info
    else:
        return False


# SELECT * FROM `cities` WHERE city_url = 'canton' AND state = (SELECT id FROM states WHERE name LIKE 'ohio')


def get_url_category(line):

    # https://regex101.com/r/zHZmt5/6
    pattern = r"(?P<url>(?P<state_abb>[^\/]{2})/(?P<city_url>[^\/]+)/(?P<category>beauty-salons|barbers|spa|nail-salons|hair-salons|massage|tattoo|fast-food|burgers|sandwich-shops|mexican|american|chinese|indian|thai|italian|pizza|seafood|sushi-bars|vietnamese|barbeque|japanese|sports-bars|salad|diners|southern|asian-fusion|steakhouses|buffet|donuts|bar-and-grill|vegetarian|ice-cream|coffee-shops|bakery|deli|chicken-wings)\/)"
    match = re.search(pattern, line)
    if match is None:
        return None
    url_info_category = match
    return url_info_category


def get_loc8_url_category(line):

    # https://regex101.com/r/jVxNvn/1
    pattern = r"/(?P<url>(?P<state_name>[^\/]+)\/(?P<city_url>[^\/]+)\/(?P<category>liquor-stores))\/"
    match = re.search(pattern, line)
    if match is None:
        return None
    loc8_url_info_category = match
    return loc8_url_info_category


def get_user_agent(line):

    # https://regex101.com/r/CPAXOF/2
    pattern = r"\" \"(?P<useragent>.*?)\"$"
    match = re.search(pattern, line)
    if match is None:
        return None
    user_agent = match["useragent"]
    return user_agent


def get_visit_time(line):

    # https://regex101.com/r/B4bq8H/1
    pattern = r" \[(?P<visittime>.*?) -"
    match = re.search(pattern, line)
    if match is None:
        return None
    visit_time = match["visittime"]
    return visit_time


def get_business_id(site, url):
    if url is False:
        return False
    if site is False:
        return False

    site = site
    values = (url, )

    sql_query = """SELECT id FROM testdb.businesses WHERE """ + site + """ = %s"""
    rows = execute_fetch_prepared(connection, sql_query, values)

    if len(rows) > 0:
        biz_id = rows[0][0]
        return biz_id
    else:
        return False


def create_table(table_name_in_db):

    create_log_table = "CREATE TABLE log_analyzer." + table_name_in_db + """(
  id INTEGER(12) NOT NULL AUTO_INCREMENT,
  ip VARCHAR(64) NULL,
  visit_time VARCHAR(256) NULL,
  user_agent VARCHAR(256) NULL,
  reffering_url VARCHAR(256) NULL,
  business_id INTEGER(12) NULL,
  url VARCHAR(256) NULL,
  state INTEGER(2) NULL,
  city INTEGER(5) NULL,
  brand INTEGER(5) NULL,
  category VARCHAR(64) NULL,
  PRIMARY KEY (id)) ENGINE = InnoDB;
"""
    create = execute_query_without_value(connection, create_log_table)
    return create


def insert_data(table_name, ip, visit_time, user_agent, reffering_url, business_id, url, state, city, brand, category):

    values = (ip, visit_time, user_agent, reffering_url, business_id, url, state, city, brand, category)

    sql_query = "INSERT INTO log_analyzer." + table_name + """(ip, visit_time, user_agent, reffering_url, business_id, 
    url, state, city, brand, category) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert = execute_query(connection, sql_query, values)
    return insert


stop = open('stop.txt', 'w+')
stop.close()


try:

    # log_file = '../bestprosintown.com-Feb-2021'

    # start_time = datetime.now()

    form = cgi.FieldStorage()

    try:
        if form['prints'].value == 'on':
            print_on_page = True  # устанавливаем флаг тру, если включен чекбокс
    except KeyError:
        print_on_page = False  # выключение чекбокса словить не можем, поэтому при ошибке(пустом чекбоксе) ставим фолс

    if print_on_page:
        print('<title>Log Analyzer</title>')
        print('<h1>Log Analyzer</h1>')


    request_field = form['TEXT'].value

    log_file = '../' + request_field

    # для локального тестирования
    # log_file = 'restaurantji.com-Jan-2021'

    break_count = 0
    if print_on_page:
        print(f'Log file name - {log_file}<br>')

    # определяем какой файл парсим, и соответственно делаем замену столбца по которому ищем бизнес айди
    if 'bestpros' in log_file:
        site_url = 'bp_url'
    elif 'loc8' in log_file:
        site_url = 'loc8_url'
    elif 'rest' in log_file:
        site_url = 'rest_url'

    if print_on_page:
        print(f'Site url for parse biz_id - {site_url}<br>')

    # из названия файла делаем названия таблицы в бд(дефизы на нижние подчеркивания меняем)
    table_name = log_file.split('/')[1]
    table_name = table_name.replace('.', '_').replace('-', '_')

    if table_name.endswith('_gz'):
        table_name = table_name.replace('_gz', '')

    # создаем таблицу в бд
    creating_table_log = create_table(table_name)
    if print_on_page:
        print(f'Create file log - {creating_table_log}<br>')
    if creating_table_log is False:
        print("<br>LOG WAS UPDATED IN THIS MONTH, BREAK<br>")
        sys.exit(0)

    try:
        break_limit = form['count'].value
        if break_limit.isdigit():
            break_limit = int(break_limit)
        else:
            break_limit = 100000000000000
    except KeyError:
        break_limit = 100000000000000

    if print_on_page:
        print(f'Break limit for testing - {break_limit}<br>')
    # начинаем работу с файлом логов

    if log_file.endswith('.gz'):
        log = gzip.open(log_file)
    else:
        log = open(log_file)

    for line in log:
        stop_check()
        try:
            if break_count == break_limit:
                break

            if type(line) is bytes:
                line = line.decode()

            if "http://www.google.com/bot.html" in line:
                continue

            if "+http://www.google.com/adsbot.html" in line:
                continue

            if ('google.com' or 'google.ca') not in line:
                continue

            if print_on_page:
                print(f'Line - {line}<br>')

            # обнуляем переменные

            ip = ''
            url = ''
            ref = ''
            user_agent = ''
            visit_time = ''
            city_url = ''
            state_abb = ''
            state_name = ''
            city_id = 0
            state_id = 0
            category_url = ''
            brand_id = 0
            biz_id = 0

            ip = get_ip(line)
            if ip is None:
                ip = ''
            if print_on_page:
                print(f'Ip - {ip}<br>')
            url = get_url(line)
            if print_on_page:
                print(f'Url - {url}<br>')
            ref = get_refferer(line)
            if ref is None:
                continue
            if not (ref.startswith('https://www.google.com/') or ref.startswith('https://www.google.ca/') or ref.startswith('https://google.com') or ref.startswith('https://google.ca')):
                continue
            if print_on_page:
                print(f'Refferer url - {ref}<br>')
            user_agent = get_user_agent(line)
            if user_agent is None:
                user_agent = ''
            if print_on_page:
                print(f'User_agent - {user_agent}<br>')
            # print(user_agent)
            visit_time = get_visit_time(line)
            if visit_time is None:
                visit_time = ''
            if print_on_page:
                print(f'Visit time - {visit_time}<br>')
            # print(visit_time)

            # print(url)
            if url is not None:

                url_for_db = url.replace('/comments/', '').replace('/photos/', '').replace('map/', '').replace('pricelist/', '').replace('appointments/', '').replace('menu/', '')
                if print_on_page:
                    print(f'<br>URL for search biz_id in DB - {url_for_db}<br>')


                biz_id = get_business_id(site_url, url_for_db)
                if biz_id is not False:
                    if print_on_page:
                        print(f'BIZ ID IS NOT FALSE')
                    city_id = get_city_id(connection, biz_id)
                    if print_on_page:
                        print(f"City id - {city_id}<br>")
                    state_id = get_state_id(connection, biz_id)
                    if print_on_page:
                        print(f"State id - {state_id}<br>")

                    # city_url = get_city_url(connection, city_id)
                    # if print_on_page:
                    #     print(f"Url city - {city_url}<br>")
                    # state_abb = get_state_abb(connection, state_id)
                    # if print_on_page:
                    #     print(f"State abb - {state_abb}<br>")

                    brand_id = get_brand_id(connection, biz_id)
                    if print_on_page:
                        print(f"<br>Brand id - {brand_id}<br>")

                    # brand_name = ''
                    # if brand_id != 0:
                    #     brand_name = get_brand_name(connection, brand_id)
                    #     print(f"<br>{brand_name}<br>")
                    # if print_on_page:
                    #     print(f'Brand name - {brand_name}<br>')

                elif biz_id is False:
                    if print_on_page:
                        print("<br>biz id is False<br>")
                    url_info = get_url2(line)
                    if url_info is not None:
                        if print_on_page:
                            print('<br>url2 is not none<br>')
                        url = url_info["url"]
                        state_abb = url_info["state_abb"]
                        city_url = url_info["city_url"]

                        city_info = get_city_id_by_city_url(connection, state_abb, city_url)
                        if city_info is False:
                            continue

                        if print_on_page:
                            print(f'<br>City info - {city_info}<br>')

                        city_id = city_info["id"]
                        if print_on_page:
                            print(f"City id - {city_id}<br>")
                        state_id = city_info["state_id"]
                        if print_on_page:
                            print(f"State id - {state_id}<br>")

                    elif url_info is None:
                        if print_on_page:
                            print('<br>url2 is none<br>')
                        category_info = get_url_category(line)
                        if category_info is not None:
                            url = category_info["url"]
                            state_abb = category_info["state_abb"]
                            city_url = category_info["city_url"]
                            category_url = category_info["category"]

                            if print_on_page:
                                print(f'<br>Category - {category_url}<br>')

                            city_info = get_city_id_by_city_url(connection, state_abb, city_url)
                            if city_info is False:
                                continue

                            if print_on_page:
                                print(f'<br>City info - {city_info}<br>')

                            city_id = city_info["id"]
                            if print_on_page:
                                print(f"City id - {city_id}<br>")
                            state_id = city_info["state_id"]
                            if print_on_page:
                                print(f"State id - {state_id}<br>")

                        elif category_info is None:
                            if print_on_page:
                                print('<br>category info is none<br>')

                            # если нужна проверка урла по restaurantji где урл имеет вид
                            # ^([a-z]{2})/([^\/]+)/([a-z]+(-[a-z]+)*)/$ и достаем brand из урла (3 скобки)

                            # brand_info = get_brand_url(line)
                            # if brand_info is not None:
                            #     url = brand_info["url"]
                            #     state_abb = brand_info["state_abb"]
                            #     city_url = brand_info["city_url"]
                            #     brand_url = brand_info["brand_url"]
                            #
                            #     if print_on_page:
                            #         print(f'<br>Brand url - {brand_url}<br>')
                            #
                            #     brand_id = get_brand_id_from_url(connection, brand_url)  # протестировать
                            #
                            #     city_info = get_city_id_by_city_url(connection, state_abb, city_url)
                            #     if city_info is False:
                            #         continue
                            #
                            #     if print_on_page:
                            #         print(f'<br>City info - {city_info}<br>')
                            #
                            #     city_id = city_info["id"]
                            #     if print_on_page:
                            #         print(f"City id - {city_id}<br>")
                            #     state_id = city_info["state_id"]
                            #     if print_on_page:
                            #         print(f"State id - {state_id}<br>")
                            #
                            # elif brand_info is None:
                            loc8_info_category = get_loc8_url_category(line)
                            if loc8_info_category is not None:
                                url = loc8_info_category["url"]
                                state_name = loc8_info_category["state_name"]
                                city_url = loc8_info_category["city_url"]
                                category_url = loc8_info_category["category"]

                                state_name = state_name.replace('_', ' ')

                                if print_on_page:
                                    print(f'<br>Category - {category_url}<br>')

                                city_info = get_city_id_by_city_url_loc8(connection, state_name, city_url)
                                if city_info is False:
                                    continue

                                if print_on_page:
                                    print(f'<br>City info - {city_info}<br>')

                                city_id = city_info["id"]
                                if print_on_page:
                                    print(f"City id - {city_id}<br>")
                                state_id = city_info["state_id"]
                                if print_on_page:
                                    print(f"State id - {state_id}<br>")

                            else:
                                continue

                            # else:
                            #     continue

                        else:
                            continue
                    else:
                        continue
                else:
                    continue

            # slash_count = url.count('/')

            if url is None:
                url = ''
            inserted_row = insert_data(table_name, ip, visit_time, user_agent, ref, biz_id, url, state_id, city_id, brand_id, category_url)
            if print_on_page:
                print(f'<br>Insert data in db - {inserted_row}<br>')

                print("<hr>")

            break_count += 1

        except Exception:
            stop_check()
            logger("Business url - " + str(url) + "\n" + "Time in log file - " +str(visit_time) + "\n" + traceback.format_exc())
            if print_on_page:
                print("Business url - " + str(url) + "\n" + "Time in log file - " +str(visit_time) + "\n" + traceback.format_exc())
            continue

    log.close()
    break_count = 0

    new_name = re.sub(r'(.*?)\.gz$', r'\1_completed.gz', log_file)

    if log_file.endswith('.gz'):
        os.rename(log_file, new_name)
    else:
        os.rename(log_file, "../" + log_file.split('/')[1]+"_completed")

    # запуск визуализации

    if 'bestpros' in log_file:
        site_name = 'bestprosintown'
    elif 'loc8' in log_file:
        site_name = 'loc8nearme'
    elif 'rest' in log_file:
        site_name = 'restaurantji'

    if 'jan' in log_file.lower():
        month_name = 'Jan'
    elif 'feb' in log_file.lower():
        month_name = 'Feb'
    elif 'mar' in log_file.lower():
        month_name = 'Mar'
    elif 'apr' in log_file.lower():
        month_name = 'Apr'
    elif 'may' in log_file.lower():
        month_name = 'May'
    elif 'jun' in log_file.lower():
        month_name = 'Jun'
    elif 'jul' in log_file.lower():
        month_name = 'Jul'
    elif 'aug' in log_file.lower():
        month_name = 'Aug'

    files = {
        'site': (None, site_name),
        'month': (None, month_name),
    }
    requests.post('http://firstbankmi.com/andrew_workspace/log_analyzer/view_log_data.py', data=files, verify=False)

    # конец запуска визуализации

except Exception as er:
    logger(traceback.format_exc())
    if print_on_page:
        print(traceback.format_exc())

# if slash_count > 3:
                    #     print(url)

# slash_count_db_url = url_for_db.count('/')
    # if slash_count_db_url > 3:
    # url_for_db = '/'.join(url_for_db.split('/')[:3])
    # print(url_for_db)
