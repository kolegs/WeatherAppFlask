from flask import Flask, request, jsonify
import psycopg2
import requests
from pyfcm import FCMNotification

app = Flask(__name__)

con = psycopg2.connect(database="postgres", user="postgres", password="test123", host="192.168.20.132", port="5432")
print("Database opened successfully")


def remove_old_data(token):
    cur = con.cursor()
    sql = """DELETE FROM "Places" WHERE user_id IN (SELECT id FROM "Users" WHERE token = %s);"""
    cur.execute(sql, (token, ));
    sql = """DELETE FROM "Users" WHERE token = %s;"""
    cur.execute(sql, (token, ));
    cur.close()


def insert_token(token, language, time):
    cur = con.cursor()
    sql = """INSERT INTO "Users"(token, language, time)
             VALUES(%s, %s, %s) RETURNING id;"""
    cur.execute(sql, (token, language, time));
    id = cur.fetchone()[0]
    cur.close()
    return id


def insert_place(new_id, latitude, longitude, frost, rain, storm, snow):
    cur = con.cursor()
    sql = """INSERT INTO "Places"(user_id, latitude, longitude, frost, rain, storm, snow)
             VALUES(%s, %s, %s, %s, %s, %s, %s);"""
    cur.execute(sql, (new_id, latitude, longitude, frost, rain, storm, snow));
    cur.close()


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/send/<title>/<message>')
def send(title, message):
    push_service = FCMNotification(api_key="AAAAFNnM5LY:APA91bHftpk7ulAOFtHI13OTLU3a_xX70IBKzDtMvTtMFnOYkGHrCB7OAyM8MF3tn2KDBiGdMNlS3d7Ab_DRRx2gZkqLaYrHoUdcGjbAeMa38KYGWCSwoloCW4XZBG4sDU1poVd8-1hM")

    cur = con.cursor()
    sql = """SELECT token FROM "Users";"""
    cur.execute(sql)

    records = cur.fetchall()
    for row in records:
        token = row[0].strip()
        registration_id = token
        message_title = title
        message_body = message
        result = push_service.notify_single_device(registration_id=registration_id, message_title=message_title,
                                                   message_body=message_body)
        if result['failure'] > 0:
            print("Removed token: " + token)
            remove_old_data(token)
            con.commit()
        else:
            print("Sent notification to: " + token)

    return "Sent"


@app.route('/register', methods = ['POST'])
def register():
    content = request.json
    print(content)
    remove_old_data(content["token"])
    new_id = insert_token(content["token"], content["language"], content["sync_time"])
    print(new_id)
    for place in content["places"]:
        insert_place(new_id,
                     place["latitude"], place["longitude"],
                     place["frost"], place["rain"], place["storm"], place["snow"])
    con.commit()
    return jsonify({"uuid": "abc"})


if __name__ == '__main__':
    app.debug = True
    app.run(host = '0.0.0.0',port=5000)
