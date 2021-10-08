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
update_post_zbx_mon_alert = """
    UPDATE
        zbx_mon
    SET
        send_state = '1'
    WHERE
        id = 1
    """
update_post_zbx_mon_ok = """
    UPDATE
        zbx_mon
    SET
        send_state = '0'
    WHERE
        id = 1
    """
select_zbx_mon = "SELECT send_state FROM zbx_mon WHERE id=1"

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


def extract_state():
    table_state = execute_read_query(connection, select_zbx_mon)
    return table_state[0][0]


message = {'alert': extract_state(), 'text': '', 'time': datetime.now()}


def check():
    print(' '.join(["time now:", str(datetime.now()), "  ", "time msg:", str(message.get('time'))]))
    if datetime.now() - message.get('time') > timedelta(minutes=3):
        if message.get('alert') is False:
            message.update({'alert': True})
            print(' '.join(["not ok:", str(datetime.now() - message.get('time'))]))
            execute_query(connection, update_post_zbx_mon_alert)
            return print(''.join(["Alert message send to Telegram ", sender_tlg(True),
                              ", MS Teams ", sender_msteams(True)]))
        else:
            return print(' '.join(["not ok:", str(datetime.now() - message.get('time'))]))
    else:
        if message.get('text') == 'all_ok' and message.get('alert') is True:
            message.update({'alert': False})
            execute_query(connection, update_post_zbx_mon_ok)
            return print(''.join(["Alive message send to Telegram ", sender_tlg(False),
                              ", MS Teams ", sender_msteams(False)]))


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
scheduler.add_job(check, 'interval', minutes=1)
scheduler.start()


@app.route("/")
def hello():
    return "Hello, World!"


@app.route(STATUS_PATH)
def status():
    msg_time = message.get('time')
    beauty_time = msg_time.strftime('%Y/%m/%d %H:%M:%S')
    return {
        'alert': message.get('alert'),
        'time': datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        'message': message['text'] + ' ' + beauty_time
    }


@app.route(SND_PATH, methods=['POST'])
def receive_msg():
    data = request.json  # JSON -> dict
    username = data['username']
    text = data['text']
    if username == ZBX_USERNAME:
        message.update({'text': text, 'time': datetime.now()})
        return {"ok": True}
    else:
        return {"ok": False}


port = int(os.environ.get("PORT", 5000))
app.run(host="0.0.0.0", port=port)
