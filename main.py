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
message = {'send': False, 'text': '', 'time': datetime.now()}


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


def check():
    print(' '.join(["time now:", str(datetime.now()), "  ", "time msg:", str(message.get('time'))]))
    if datetime.now() - message.get('time') > timedelta(minutes=2):
        message.update({'text': 'not_ok'})
        print(' '.join(["not ok:", str(datetime.now() - message.get('time'))]))


def report():
    if message.get('text') == 'not_ok' and (execute_read_query(connection, select_zbx_mon))[0][0] is False:
        message.update({'send': True})
        execute_query(connection, update_post_zbx_mon_alert)
        print("Report sent")
        return requests.post(TLG_LINK, data={"chat_id": TLG_CHAT_ID, "text": "Zabbix замолчал!"})
    if message.get('text') == 'all_ok' and (execute_read_query(connection, select_zbx_mon))[0][0] is True:
        message.update({'send': False})
        execute_query(connection, update_post_zbx_mon_ok)
        print("Alive sent")
        return requests.post(TLG_LINK, data={"chat_id": TLG_CHAT_ID, "text": "Zabbix ожил!"})


scheduler = BackgroundScheduler()
scheduler.add_job(report, 'interval', minutes=1)
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
        'send': message.get('send'),
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

