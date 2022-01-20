import os
from datetime import datetime, timedelta
import requests
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2
from psycopg2 import OperationalError

DATABASE_URL = os.environ['DATABASE_URL']
TLG_LINK = os.environ['TLG_LINK']
TLG_CHAT_ID = os.environ['TLG_CHAT_ID']
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


def extract_value_from_db(row_id, col_name):
    table_state = execute_read_query(connection, "SELECT " + col_name + " FROM zbx_mon WHERE id = " + str(row_id + 1))
    return table_state[0][0]


def get_count():
    table_count = execute_read_query(connection, "SELECT count(*) FROM zbx_mon")
    return table_count[0][0]


nodeList = [{'alert': extract_value_from_db(0, 'send_state'), 'ok_msg': False, 'time': datetime.now()}]
nodelist_temp = []
for row in range(get_count()):
    node_name = extract_value_from_db(row, 'node_name')
    alert = extract_value_from_db(row, 'send_state')
    node = {'node_name': node_name, 'alert': alert, 'ok_msg': False, 'time': datetime.now()}
    nodelist_temp.append(node)


def worker():
    item_index = 0
    for item in nodeList:
        state_checker(item, item_index)
        item_index = item_index + 1


def state_checker(message, index):
    if datetime.now() - message.get('time') > timedelta(minutes=3):
        if message.get('alert') is False:
            message.update({'alert': True})
            print(' '.join(["Status Alert:", str(datetime.now() - message.get('time'))]))
            execute_query(connection, update_post_zbx_mon_alert + str(index + 1))
            return print(''.join(["Alert message send to Telegram ", sender_tlg(True),
                                  ", MS Teams ", sender_msteams(True)]))
        else:
            return print(' '.join(["Status Alert:", str(datetime.now() - message.get('time'))]))
    else:
        if message.get('ok_msg') is True and message.get('alert') is True:
            message.update({'alert': False})
            execute_query(connection, update_post_zbx_mon_ok + str(index + 1))
            return print(''.join(["Alive message send to Telegram ", sender_tlg(False),
                                  ", MS Teams ", sender_msteams(False)]))
        else:
            return print(' '.join(["Status OK,", "time now:", str(datetime.now()), "  ",
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


def sender_tlg(state):
    if state:
        response = requests.post(TLG_LINK, data={"chat_id": TLG_CHAT_ID, "text": "Zabbix замолчал!"})
        return ''.join(["(response: ", str(response.status_code), ')'])
    else:
        response = requests.post(TLG_LINK, data={"chat_id": TLG_CHAT_ID, "text": "Zabbix ожил!"})
        return ''.join(["(response: ", str(response.status_code), ')'])


scheduler = BackgroundScheduler()
scheduler.add_job(worker, 'interval', minutes=1)
scheduler.start()


@app.route("/")
def hello():
    return "Hello, World!"


@app.route(STATUS_PATH)
def status():
    msg_time = nodeList[0]['time']
    return {
        'alert': nodeList[0]['alert'],
        'time_now': datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        'ok_msg:': nodeList[0]['ok_msg'],
        'time_msg': msg_time.strftime('%Y/%m/%d %H:%M:%S')
    }


@app.route(SND_PATH, methods=['POST'])
def receive_msg():
    data = request.json  # JSON -> dict
    if data['username'] == ZBX_USERNAME and data['text'] == 'all_ok':
        nodeList[0]['ok_msg'] = True
        nodeList[0]['time'] = datetime.now()
        return {"ok": True}
    else:
        return {"ok": False}


port = int(os.environ.get("PORT", 5000))
app.run(host="0.0.0.0", port=port)
