import os
from datetime import datetime, timedelta
import requests
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2
from psycopg2 import OperationalError

DATABASE_URL = os.environ['DATABASE_URL']
ZBX_USERNAME = os.environ['ZBX_USERNAME']
SND_PATH = os.environ['SND_PATH']
STATUS_PATH = os.environ['STATUS_PATH']
MS_TEAMS_WEBHOOK = os.environ['MS_TEAMS_WEBHOOK']
IMAGE_URL_FAIL = os.environ['IMAGE_URL_FAIL']
IMAGE_URL_OK = os.environ['IMAGE_URL_OK']
psycopg2.connect(DATABASE_URL)
connection = psycopg2.connect(DATABASE_URL, sslmode='require')
update_post_zbx_mon_alert = "UPDATE zbx_mon SET send_state = '1' WHERE id = "
update_post_zbx_mon_ok = "UPDATE zbx_mon SET send_state = '0' WHERE id = "

app = Flask(__name__)


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def extract_value_from_db(table_name, row_id, col_name):
    table_state = execute_read_query(connection, "SELECT " + col_name + " FROM " + table_name + " WHERE id = " + str(row_id + 1))
    return table_state[0][0]


def get_count(table_name):
    table_count = execute_read_query(connection, "SELECT count(*) FROM " + table_name)
    return table_count[0][0]


def get_row(row):
    node_name = extract_value_from_db('zbx_mon', row, 'node_name')
    alert = extract_value_from_db('zbx_mon', row, 'send_state')
    send_msteams = extract_value_from_db('zbx_mon', row, 'send_msteams')
    send_telegram = extract_value_from_db('zbx_mon', row, 'send_telegram')
    return {'node_name': node_name, 'alert': alert, 'ok_msg': False, 'time': datetime.now(),
            'send_msteams': send_msteams, 'send_telegram': send_telegram}


def get_tlg(row):
    token = extract_value_from_db('method_telegram', row, 'token')
    chat_id = extract_value_from_db('method_telegram', row, 'chat_id')
    return {'token': token, 'chat_id': chat_id}


nodelist = []
telegram_tokens = []
for row_number in range(get_count('zbx_mon')):
    nodelist.append(get_row(row_number))
for row_number in range(get_count('method_telegram')):
    telegram_tokens.append(get_tlg(row_number))


def worker():
    item_index = 0
    for item in nodelist:
        state_checker(item, item_index)
        item_index = item_index + 1


def state_checker(message, index):
    if datetime.now() - message.get('time') > timedelta(minutes=3):
        if message.get('alert') is False:
            message.update({'alert': True})
            print(' '.join(["Status Alert:", str(datetime.now() - message.get('time'))]))
            execute_query(connection, update_post_zbx_mon_alert + str(index + 1))
            if message.get('send_msteams') is not None:
                print(''.join(["Alert message send to MS Teams ", sender_msteams(True)]))
            if message.get('send_telegram') is not None:
                print(''.join(["Alert message send to Telegram ", sender_tlg(index, True)]))
            return print('Status ' + message.get('node_name') + ' switched to Alert')
        else:
            return print(' '.join(['Status ' + message.get('node_name') + ' is Alert:', str(datetime.now() - message.get('time'))]))
    else:
        if message.get('ok_msg') is True and message.get('alert') is True:
            message.update({'alert': False})
            execute_query(connection, update_post_zbx_mon_ok + str(index + 1))
            if message.get('send_msteams') is not None:
                print(''.join(["Alive message send to MS Teams ", sender_msteams(False)]))
            if message.get('send_telegram') is not None:
                print(''.join(["Alive message send to Telegram ", sender_tlg(index, False)]))
            return print('Status ' + message.get('node_name') + ' switched to OK')
        else:
            return print(' '.join(['Status ' + message.get('node_name') + ' is OK,', "time now:", str(datetime.now()), "  ",
                                   "time msg:", str(message.get('time'))]))


def sender_msteams(state):
    if state:
        response = requests.post(MS_TEAMS_WEBHOOK, json={'themeColor': 'ff0000', 'summary': 'Zabbix', 'sections': [{
            'activityTitle': 'Zabbix замолчал!', 'activityImage': IMAGE_URL_FAIL}]})
        return ''.join(["(response: ", str(response.status_code), " ", response.text, ')'])
    else:
        response = requests.post(MS_TEAMS_WEBHOOK, json={'themeColor': '00ff00', 'summary': 'Zabbix', 'sections': [{
            'activityTitle': 'Zabbix ожил!', 'activityImage': IMAGE_URL_OK}]})
        return ''.join(["(response: ", str(response.status_code), " ", response.text, ')'])


def sender_tlg(number, state):
    if state:
        response = requests.post("https://api.telegram.org/bot" + telegram_tokens[number]['token'] + "/sendMessage",
                                 data={"chat_id": telegram_tokens[number]['chat_id'], "text": nodelist[number]['node_name'] + " замолчал!"})
        return ''.join(["(response: ", str(response.status_code), ')'])
    else:
        response = requests.post("https://api.telegram.org/bot" + telegram_tokens[number]['token'] + "/sendMessage",
                                 data={"chat_id": telegram_tokens[number]['chat_id'], "text": nodelist[number]['node_name'] + " ожил!"})
        return ''.join(["(response: ", str(response.status_code), ')'])


scheduler = BackgroundScheduler()
scheduler.add_job(worker, 'interval', minutes=1)
scheduler.start()


@app.route("/")
def hello():
    return "Hello, World!"


@app.route(STATUS_PATH)
def status():
    msg_time = nodelist[0]['time']
    return {
        'alert': nodelist[0]['alert'],
        'time_now': datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        'ok_msg:': nodelist[0]['ok_msg'],
        'time_msg': msg_time.strftime('%Y/%m/%d %H:%M:%S')
    }


@app.route(SND_PATH, methods=['POST'])
def receive_msg():
    data = request.json  # JSON -> dict
    index = next((i for i, item in enumerate(nodelist) if item['node_name'] == data['username']), None)
    if index is not None and data['text'] == 'all_ok':
        nodelist[index]['ok_msg'] = True
        nodelist[index]['time'] = datetime.now()
        return {"ok": True}
    else:
        return {"ok": False}


port = int(os.environ.get("PORT", 5000))
app.run(host="0.0.0.0", port=port)
