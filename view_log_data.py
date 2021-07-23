#!/usr/bin/python3

import cgi
from db_conn_module import *
import os
import traceback
from logging_module import add_log
import sys


print('Content-Type: text/html')
print()

logger = add_log

try:
    # connection with DATABASE
except Exception as er:
    print(er)
    logger(er)


def stop_check():
    if not os.path.exists('stop.txt'):
        if print_on_page:
            print('file deleted<br>')
            print('<h2>Script stopped</h2>')
        sys.exit(0)


def save_cache(text, file_name):
    f = open('cache/' + file_name + '.html', 'a')
    f.writelines(text)
    f.close()
    os.chmod('cache/' + file_name + '.html', 0o777)
    return True


def select_count(table_name):
    if table_name is False:
        return False

    sql_query = "SELECT COUNT(*) FROM log_analyzer." + table_name
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        count = rows[0][0]
        return count
    else:
        return False


def select_state(table_name):
    if table_name is False:
        return False

    state_info = {}

    sql_query = "SELECT COUNT(*) AS Count, state FROM log_analyzer." + table_name + " GROUP BY state ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        countT = 0
        for row in rows:
            state_info[countT] = (row[0], get_state_name(connection, row[1]), row[1])
            countT += 1
    return state_info


def get_state_name(connection, state_id):
    if connection is False:
        return False
    if state_id is False:
        return False

    sql_query = "SELECT name FROM testdb.states WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [state_id])
    if len(rows) > 0:
        abbr = rows[0][0]
        return abbr
    else:
        return False


def select_city(table_name):
    if table_name is False:
        return False

    city_info = {}

    sql_query = "SELECT COUNT(*) AS Count, city, state FROM log_analyzer." + table_name + " GROUP BY city ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        index = 0
        for row in rows:
            view_count = row[0]
            city_name = get_city_name(connection, row[1])
            city_id = row[1]
            state_abb = get_state_abb(connection, row[2])

            city_info[index] = (view_count, city_name, city_id, state_abb)
            index += 1
    return city_info


def get_state_abb(connection, state_id):
    if connection is False:
        return False
    if state_id is False:
        return False

    sql_query = "SELECT abbreviation FROM testdb.states WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [state_id])
    if len(rows) > 0:
        abbr = rows[0][0]
        return abbr
    else:
        return False


def get_city_name(connection, city_id):
    if connection is False:
        return False
    if city_id is False:
        return False

    sql_query = "SELECT name FROM testdb.cities WHERE id = %s"
    rows = execute_fetch(connection, sql_query, [city_id])
    if len(rows) > 0:
        city_url = rows[0][0]
        return city_url
    else:
        return False


def select_biz_id(table_name):
    if table_name is False:
        return False

    biz_id_info = {}

    sql_query = "SELECT COUNT(*) AS Count, business_id, url FROM log_analyzer." + table_name + " WHERE business_id != 0 GROUP BY business_id ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        countT = 0
        for row in rows:
            biz_id_info[countT] = (row[0], row[1], row[2])
            countT += 1
    return biz_id_info


def select_category(table_name):
    if table_name is False:
        return False

    category_info = {}

    sql_query = "SELECT COUNT(*) AS Count, category FROM log_analyzer." + table_name + " WHERE category != '' GROUP BY category ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        countT = 0
        for row in rows:
            category_info[countT] = (row[0], row[1])
            countT += 1
    return category_info


def select_categories_from_biz(table_name):
    if table_name is False:
        return False

    categories_info = {}

    sql_query = """SELECT COUNT(*) as `categories_count`, `category_id`, `testdb`.`categories`.`name`
     FROM log_analyzer.""" + table_name + """ 
     INNER JOIN `testdb`.`categories_list` 
        ON log_analyzer. """ + table_name + """.`business_id`= `testdb`.`categories_list`.`business_id`
    INNER JOIN `testdb`.`categories` 
        ON `testdb`.`categories`.`id` = `category_id`
GROUP BY `category_id`
ORDER BY COUNT(*) DESC
LIMIT 100"""
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        countT = 0
        for row in rows:
            categories_info[countT] = (row[0], row[1], row[2])
            countT += 1
    return categories_info


def select_brand(table_name):
    if table_name is False:
        return False

    brand_info = {}

    sql_query = "SELECT COUNT(*) AS Count, brand FROM log_analyzer." + table_name + " WHERE brand != 0 GROUP BY brand ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        countT = 0
        for row in rows:
            brand_info[countT] = (row[0], get_brand_name(connection, row[1]), row[1])
            countT += 1
    return brand_info


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


def select_city_restaurantji(table_name):
    if table_name is False:
        return False

    city_info = {}

    sql_query = "SELECT COUNT(*) AS Count, city, state FROM log_analyzer." + table_name + " WHERE `business_id` = 0 AND `state` > 0 AND `city` > 0 AND `category` = '' GROUP BY city ORDER BY Count DESC LIMIT 100"
    rows = execute_fetch(connection, sql_query)
    if len(rows) > 0:
        index = 0
        for row in rows:
            view_count = row[0]
            city_name = get_city_name(connection, row[1])
            city_id = row[1]
            state_abb = get_state_abb(connection, row[2])

            city_info[index] = (view_count, city_name, city_id, state_abb)
            index += 1
    return city_info


def is_exists_table(table_name):
    if table_name is False:
        return False

    sql_query = "SELECT EXISTS(SELECT * FROM log_analyzer." + table_name + " WHERE 1)"
    try:
        rows = execute_fetch(connection, sql_query)
        if rows is False:
            return False
        if len(rows) > 0:
            rows = rows[0][0]
            rows = bool(rows)
            return rows
    except Exception as err:
        if print_on_page:
            print(f'<br>Error - {err}<br>')
        add_log(err)
        return False


def get_city_count_old(connection, city_id, table_name):
    if connection is False:
        return False
    if city_id is False:
        return False

    values = (city_id, )
    sql_query = "SELECT COUNT(*) AS Count, city FROM log_analyzer." + table_name + " WHERE city = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


def get_state_count_old(connection, state_id, table_name):
    if connection is False:
        return False
    if state_id is False:
        return False

    values = (state_id, )
    sql_query = "SELECT COUNT(*) AS Count, state FROM log_analyzer." + table_name + " WHERE state = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


def get_biz_id_count_old(connection, biz_id, table_name):
    if connection is False:
        return False
    if biz_id is False:
        return False

    values = (biz_id, )
    sql_query = "SELECT COUNT(*) AS Count, business_id FROM log_analyzer." + table_name + " WHERE business_id != 0 AND business_id = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


def get_category_count_old(connection, category, table_name):
    if connection is False:
        return False
    if category is False:
        return False

    values = (category, )
    sql_query = "SELECT COUNT(*) AS Count, category FROM log_analyzer." + table_name + " WHERE category != '' AND category = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


def get_count_categories_from_biz_old(connection, category_id, table_name):
    if connection is False:
        return False
    if category_id is False:
        return False

    values = (category_id, )
    sql_query = """SELECT COUNT(*) as `categories_count`, `category_id`, `testdb`.`categories`.`name`
     FROM log_analyzer.""" + table_name + """ INNER JOIN `testdb`.`categories_list` 
     ON log_analyzer. """ + table_name + """.`business_id`= `testdb`.`categories_list`.`business_id`
INNER JOIN `testdb`.`categories` ON `testdb`.`categories`.`id` = `category_id` WHERE `category_id` = %s"""
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


def get_brand_count_old(connection, brand_id, table_name):
    if connection is False:
        return False
    if brand_id is False:
        return False

    values = (brand_id, )
    sql_query = "SELECT COUNT(*) AS Count, brand FROM log_analyzer." + table_name + " WHERE brand != 0 AND brand = %s"
    rows = execute_fetch_prepared(connection, sql_query, values)
    if len(rows) > 0:
        old_count = rows[0][0]
        return old_count
    else:
        return False


stop = open('stop.txt', 'w+')
stop.close()

try:

    form = cgi.FieldStorage()

    site_name = form['site'].value

    month = form['month'].value

    full_table_name = site_name + "_com_" + month + "_2021"

    try:
        if form['prints'].value == 'on':
            print_on_page = True  # устанавливаем флаг тру, если включен чекбокс
    except KeyError:
        print_on_page = False  # выключение чекбокса словить не можем, поэтому при ошибке(пустом чекбоксе) ставим фолс

    if is_exists_table(full_table_name) is False:
        if print_on_page:
            print(f"<br><br><b>TABLE - {full_table_name} is NOT FOUND in database</b><br><br>")
        sys.exit(0)

    # логика для получения данных с прошлого месяца

    old_month_dict = {'Aug': 'Jul', 'Jul': 'Jun', 'Jun': 'May', 'May': 'Apr', 'Apr': 'Mar', 'Mar': 'Feb', 'Feb': 'Jan'}

    old_month = old_month_dict[month]
    old_month_table_name = site_name + "_com_" + old_month + "_2021"

    # конец

    if is_exists_table(old_month_table_name) is False:
        if print_on_page:
            print(f"<br><br><b>TABLE for PAST month - {full_table_name} is NOT FOUND in database</b><br><br>")
        sys.exit(0)

    if 'bestpros' in site_name:
        domain_url = 'https://www.bestprosintown.com/'
    elif 'loc8' in site_name:
        domain_url = 'https://www.loc8nearme.com/'
    elif 'rest' in site_name:
        domain_url = 'https://www.restaurantji.com/'

    if os.path.exists('cache/' + full_table_name + '.html'):
        # os.remove('cache/' + full_table_name + '.html')
        f = open('cache/' + full_table_name + '.html')
        for line in f:
            if print_on_page:
                print(line)

    else:

        title_html = '<title>Log Analyzer</title>'
        save_cache(title_html, full_table_name)
        if print_on_page:
            print(title_html)
        page_header_html = '<h1>View log DATA (from analyzer)</h1>'
        save_cache(page_header_html, full_table_name)
        if print_on_page:
            print(page_header_html)


        name_table_html = f"<br>Table name - <b>{full_table_name}</b><br><br>"
        save_cache(name_table_html, full_table_name)
        if print_on_page:
            print(name_table_html)


        # текущий месяц
        result_city = select_city(full_table_name)
        result_state = select_state(full_table_name)
        result_biz_id = select_biz_id(full_table_name)
        result_category = select_category(full_table_name)
        result_brand = select_brand(full_table_name)
        result_categories_biz = select_categories_from_biz(full_table_name)
        select_all_rows = select_count(full_table_name)

        if 'rest' in site_name:
            result_city_rest = select_city_restaurantji(full_table_name)

        # all_rows_html = f'Count all rows - <b>{select_all_rows}</b><br><br>'
        # if print_on_page:
        #     print(all_rows_html)

        if 'rest' in site_name:

            table_div_html = f"""<div style='display: grid; max-height: 400px; overflow: auto; grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr 1fr;
                align-items: baseline;grid-column-gap: 8px; width: max-content;'>"""
        else:

            table_div_html = f"""<div style='display: grid; max-height: 400px; overflow: auto; grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr;
                            align-items: baseline;grid-column-gap: 8px; width: max-content;'>"""

        save_cache(table_div_html, full_table_name)
        if print_on_page:
            print(table_div_html)

        div_1row_html = f"""<div style='display: grid; max-height: 400px; overflow: auto;'>
            <div style='display: grid; grid-template-columns: 90px 100px 50px 150px 50px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                <div>Count</div>
                <div><b>% Diff</b></div>
                <div>%</div>
                <div>City</div>
                <div>State</div>
            </div> """
        save_cache(div_1row_html, full_table_name)
        if print_on_page:
            print(div_1row_html)

        for item in result_city:
            stop_check()

            old_count_city = get_city_count_old(connection, result_city[item][2], old_month_table_name)
            try:
                diff = format((result_city[item][0] - old_count_city) / old_count_city * 100, '.3f')
            except ZeroDivisionError:
                diff = 0

            result_city_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px 50px;'>
                            <div>{result_city[item][0]}</div>
                            <div><b>{diff}</b></div>
                            <div>{format((result_city[item][0]/int(select_all_rows))*100, '.3f')}</div>
                            <div>{result_city[item][1]}</div>
                            <div>{result_city[item][3]}</div>
                           </div>"""
            save_cache(result_city_div, full_table_name)
            if print_on_page:
                print(result_city_div)

        div_2row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>
            <div style='display: grid; grid-template-columns: 90px 100px 50px 150px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                <div>Count</div>
                <div><b>% Diff</b></div>
                <div>%</div>
                <div>State</div>
            </div> """
        save_cache(div_2row_html, full_table_name)
        if print_on_page:
            print(div_2row_html)

        for item in result_state:
            stop_check()

            old_count_state = get_state_count_old(connection, result_state[item][2], old_month_table_name)
            try:
                diff = format((result_state[item][0] - old_count_state) / old_count_state * 100, '.3f')
            except ZeroDivisionError:
                diff = 0

            result_state_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px;'>
                            <div>{result_state[item][0]}</div>
                            <div><b>{diff}</b></div>
                            <div>{format((result_state[item][0]/int(select_all_rows))*100, '.3f')}</div>
                            <div>{result_state[item][1]}</div>
                        </div>"""
            save_cache(result_state_div, full_table_name)
            if print_on_page:
                print(result_state_div)

        div_3row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>
    
                <div style='display: grid; grid-template-columns: 90px 100px 50px 150px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                    <div>Count</div>
                    <div><b>% Diff</b></div>
                    <div>%</div>
                    <div>Business id</div>
                </div> """
        save_cache(div_3row_html, full_table_name)
        if print_on_page:
            print(div_3row_html)

        for item in result_biz_id:
            stop_check()

            old_count_biz_id = get_biz_id_count_old(connection, result_biz_id[item][1], old_month_table_name)
            try:
                diff = format((result_biz_id[item][0] - old_count_biz_id) / old_count_biz_id * 100, '.3f')
            except ZeroDivisionError:
                diff = 0

            result_biz_id_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px;'>
                                    <div>{result_biz_id[item][0]}</div>
                                    <div><b>{diff}</b></div>
                                    <div>{format((result_biz_id[item][0]/int(select_all_rows))*100, '.3f')}</div>
                                    <div><a href ="{domain_url}{result_biz_id[item][2]}">{result_biz_id[item][1]}</a></div>
                                </div>"""
            save_cache(result_biz_id_div, full_table_name)
            if print_on_page:
                print(result_biz_id_div)

        div_4row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>
                    <div style='display: grid; grid-template-columns: 90px 100px 50px 150px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                        <div>Count</div>
                        <div><b>% Diff</b></div>
                        <div>%</div>
                        <div>Category</div>
                    </div> """
        save_cache(div_4row_html, full_table_name)
        if print_on_page:
            print(div_4row_html)

        for item in result_category:
            stop_check()

            old_count_category = get_category_count_old(connection, result_category[item][1], old_month_table_name)
            try:
                diff = format((result_category[item][0] - old_count_category) / old_count_category * 100, '.3f')
            except ZeroDivisionError:
                diff = 0

            result_category_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px;'>
                                            <div>{result_category[item][0]}</div>
                                            <div><b>{diff}</b></div>
                                            <div>{format((result_category[item][0]/int(select_all_rows))*100, '.3f')}</div>
                                            <div>{result_category[item][1]}</div>
                                        </div>"""
            save_cache(result_category_div, full_table_name)
            if print_on_page:
                print(result_category_div)

        if 'rest' in site_name:

            div_5row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>
                        <div style='display: grid; grid-template-columns: 90px 100px 50px 150px 50px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                            <div><b>Count for Rest</b></div>
                            <div><b>% Diff</b></div>
                            <div>%</div>
                            <div>City</div>
                            <div>State</div>
                        </div> """
            save_cache(div_5row_html, full_table_name)
            if print_on_page:
                print(div_5row_html)

            for item in result_city_rest:
                stop_check()

                old_count_city_rest = get_city_count_old(connection, result_city_rest[item][2], old_month_table_name)
                try:
                    diff = format((result_city_rest[item][0] - old_count_city_rest) / old_count_city_rest * 100, '.3f')
                except ZeroDivisionError:
                    diff = 0

                result_city_rest_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px 50px;'>
                                        <div>{result_city_rest[item][0]}</div>
                                        <div><b>{diff}</b></div>
                                        <div>{format((result_city_rest[item][0] / int(select_all_rows)) * 100, '.3f')}</div>
                                        <div>{result_city_rest[item][1]}</div>
                                        <div>{result_city_rest[item][3]}</div>
                                       </div>"""
                save_cache(result_city_rest_div, full_table_name)
                if print_on_page:
                    print(result_city_rest_div)


        div_6row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>
    
                        <div style='display: grid; grid-template-columns: 90px 100px 50px 150px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                            <div>Count</div>
                            <div><b>% Diff</b></div>
                            <div>%</div>
                            <div>Brand</div>
                        </div> """
        save_cache(div_6row_html, full_table_name)
        if print_on_page:
            print(div_6row_html)

        for item in result_brand:
            stop_check()

            old_count_brand = get_brand_count_old(connection, result_brand[item][2], old_month_table_name)
            try:
                diff = format((result_brand[item][0] - old_count_brand) / old_count_brand * 100, '.3f')
            except ZeroDivisionError:
                diff = 0

            result_brand_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px;'>
                                                    <div>{result_brand[item][0]}</div>
                                                    <div><b>{diff}</b></div>
                                                    <div>{format((result_brand[item][0]/int(select_all_rows))*100, '.3f')}</div>
                                                    <div>{result_brand[item][1]}</div>
                                                </div>"""
            save_cache(result_brand_div, full_table_name)
            if print_on_page:
                print(result_brand_div)

        div_7_row_html = f"""</div><div style='display: grid; max-height: 400px; overflow: auto;'>

                                <div style='display: grid; grid-template-columns: 90px 100px 50px 150px; position: sticky; top: 0;background: #ccc; padding: 4px;'>
                                    <div>Count</div>
                                    <div><b>% Diff</b></div>
                                    <div>%</div>
                                    <div>Category name</div>
                                </div> """

        save_cache(div_7_row_html, full_table_name)
        if print_on_page:
            print(div_7_row_html)

        for item in result_categories_biz:
            stop_check()

            old_count_categories_biz = get_count_categories_from_biz_old(connection, result_categories_biz[item][1],
                                                                         old_month_table_name)
            try:
                diff = format(
                    (result_categories_biz[item][0] - old_count_categories_biz) / old_count_categories_biz * 100, '.3f')
            except Exception as err:
                logger(traceback.format_exc())
                diff = 0

            result_categories_biz_div = f"""<div style='display: grid; grid-template-columns: 90px 100px 50px 150px;'>
                                                            <div>{result_categories_biz[item][0]}</div>
                                                            <div><b>{diff}</b></div>
                                                            <div>{format((result_categories_biz[item][0] / int(select_all_rows)) * 100, '.3f')}</div>
                                                            <div>{result_categories_biz[item][2]}</div>
                                                        </div>"""
            save_cache(result_categories_biz_div, full_table_name)
            if print_on_page:
                print(result_categories_biz_div)

        last_div = '</div></div>'
        save_cache(last_div, full_table_name)
        if print_on_page:
            print(last_div)


except Exception as er:
    logger(traceback.format_exc())
    if print_on_page:
        print(er)
