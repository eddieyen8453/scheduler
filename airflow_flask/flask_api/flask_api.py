# import random
import os
import pandas as pd
import json
import logging
logging.basicConfig(level=logging.INFO)
from datetime import datetime
from flask import Flask, request, Response
from flask_redis import FlaskRedis  # add
import pymssql

app = Flask(__name__)
app.config['REDIS_URL'] = 'redis://10.1.106.139:8091/0' # redis for flask, not for airflow
r = FlaskRedis(app)

def upsert_redis(member_id_list, latest_transaction_list):
    for key,value in zip(member_id_list,latest_transaction_list):
        # logging.info(key, value)
        r.set(key, value)

def upsert_trigger(server, user, password, database, upsert_trigger_time_part_list):
    conn = pymssql.connect(server=server,
                        user=user,
                        password=password,
                        database=database)
    cursor = conn.cursor(as_dict=True) 
    try:
        logging.info("trigger_time start!")
        command = """MERGE INTO dbo.flasktest2 AS target
        USING (VALUES (%s, %s)) AS source (member_id, trigger_time)
        ON target.member_id = source.member_id
        WHEN NOT MATCHED THEN
            INSERT (member_id, trigger_time)
            VALUES (source.member_id, source.trigger_time);"""
        cursor.executemany(command ,upsert_trigger_time_part_list)
        conn.commit()
        logging.info("trigger_time done!")
        dictionary = {"response_code":200}
    except Exception as e :
        logging.info(e) 
        logging.info("upsert trigger time record fail")
        dictionary = {"response_code":400} # upsert trigger_time record fail
    conn.close()
    return dictionary

def upsert_prediction(server, user, password, database, upsert_data_part_list):
    conn = pymssql.connect(server=server,
                        user=user,
                        password=password,
                        database=database)
    cursor = conn.cursor(as_dict=True) 
    try:
        logging.info("prediction start!")
        command = """MERGE INTO dbo.flasktest1 AS target
        USING (VALUES (%s, %s, %s)) AS source (member_id, predict_duration, sending_group)
        ON target.member_id = source.member_id
        WHEN MATCHED THEN
            UPDATE SET target.predict_duration = source.predict_duration, target.sending_group = source.sending_group
        WHEN NOT MATCHED THEN
            INSERT (member_id, predict_duration, sending_group)
            VALUES (source.member_id, source.predict_duration, source.sending_group);"""
        cursor.executemany(command ,upsert_data_part_list)
        conn.commit()
        logging.info("prediction done!")
        dictionary = {"response_code":200}
    except Exception as e :
        logging.info(e) 
        logging.info("upsert prediction record fail")
        dictionary = {"response_code":400} # upsert prediction record fail
    conn.close()
    return dictionary

@app.route('/check',methods=["GET","POST"])
def check():
    dic = {}
    dictionary = {"response_code":200,"response_content":dic}
    return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

@app.route('/',methods=["GET","POST"])
# show redis db on the home page
def hello_world():
    key_list = []
    value_list = []
    keys = r.keys()
    for key in keys:
        key_list.append(key.decode('utf-8')) # bytes need decode
        value = r.get(key)
        value_list.append(value.decode('utf-8')) # bytes need decode
    
    keyvalue_list = []
    for keyvalue in zip(key_list,value_list):
        keyvalue_list.append(keyvalue)
    dic = {"key-value" : keyvalue_list}
    return str(dic)

# try to insert key into redis
@app.route('/insert_key',methods=["GET","POST"])
def insert_key():
    try:
        key = request.json['key']
        r.incr(key)
        dic = {}
        dictionary = {"response_code":200,"response_content":dic}
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    except Exception as e:
        logging.info('======Error:{}======'.format(e))
        dic = {}
        dictionary = {"response_code":400,"response_content":dic}
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    
# try to empty redis db
@app.route('/empty_db',methods=["GET","POST"])
def empty_db():
    try:
        r.flushdb()
        dic = {}
        dictionary = {"response_code":200,"response_content":dic}
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    except Exception as e:
        logging.info("CCC")
        logging.info('======Error:{}======'.format(e))
        dic = {}
        dictionary = {"response_code":400,"response_content":dic}
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

# using redis to save MQ transaction record
@app.route('/mq_transaction',methods=["GET","POST"])
def mq_transaction():
    logging.info('======mq_transaction_start======')
    member_id = request.json["member_id"]
    latest_transaction = request.json["latest_transaction_time"]

    list_len = len(member_id)
    batch_size = 100000 # each part
    quotient = list_len // batch_size
    reminder = list_len % batch_size
    for i in range(quotient):
        logging.info("{} th loop".format(i))
        # upsert trigger record
        member_id_list = member_id[(i*batch_size):((i+1)*batch_size)]
        latest_transaction_list = latest_transaction[(i*batch_size):((i+1)*batch_size)]
        # print(member_id_list)
        upsert_redis(member_id_list, latest_transaction_list)
        logging.info("{} th loop done!".format(i))
    if reminder==0:
        pass
    else:
        # logging.info("reminder start!")
        member_id_list = member_id[(quotient)*batch_size:-1]
        latest_transaction_list = latest_transaction[(quotient)*batch_size:-1]
        upsert_redis(member_id_list, latest_transaction_list)
    dic = {}
    dictionary = {"response_code":200,"response_content":dic}
    logging.info('======mq_transaction_done======')
    return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

# using sql to save prediction duration record
@app.route('/prediction_duration',methods=["GET","POST"])
def prediction_duration():
    logging.info('======prediction_duration_start======')
    member_id = request.json["member_id"]
    predict_duration = request.json["predict_duration"]
    logging.info('======prepare_upsertdata_start======')
    data = {"member_id":member_id, "predict_duration":predict_duration}
    # can be auto
    table_integer = len(data['member_id']) // len(range(0,24)) # quotient
    table_remainder = len(data['member_id']) % len(range(0,24)) # reminder
    sending_group_lst = list(range(0,24))*table_integer + list(range(0,table_remainder)) # quotient * len(range) + reminder = total length
    data['sending_group'] = sending_group_lst
    data["trigger_time"] = list(len(data['member_id'])*["2000-01-01 00:00:00"]) # init trigger time
    
    # trigger record for new member who do not have trigger time yet
    upsert_trigger_time_list = []
    for upsert_trigger_time in zip(data['member_id'],data['trigger_time']):
        upsert_trigger_time_list.append(upsert_trigger_time)

    # prediction record for upsert into sql 
    upsert_data_list = []
    for upsert_data in zip(data['member_id'],data['predict_duration'],data['sending_group']):
        upsert_data_list.append(upsert_data)
    logging.info('======list_len = {}======'.format(len(upsert_trigger_time_list)))
    logging.info('======connect to sql start======')
    server = '10.11.36.178' # server ip
    user = 'user' # user name
    password = '12345678'
    database = "test" # select db name
    # upsert sql infomation
    try:
        conn = pymssql.connect(server=server,
                                user=user,
                                password=password,
                                database=database)
        cursor = conn.cursor(as_dict=True) 


        list_len = len(upsert_trigger_time_list) # total data size
        batch_size = 10000 # each part
        quotient = list_len // batch_size
        reminder = list_len % batch_size
        for i in range(quotient):
            logging.info("{} th loop".format(i))
            # upsert trigger record
            upsert_trigger_time_part_list = upsert_trigger_time_list[(i*batch_size):((i+1)*batch_size)]
            upsert_data_part_list = upsert_data_list[(i*batch_size):((i+1)*batch_size)]
            dic = upsert_trigger(server, user, password, database, upsert_trigger_time_part_list)
            if dic["response_code"]==400:
                return Response(json.dumps(dic),headers ={'Content-Type':'application/json'})
            dic = upsert_prediction(server, user, password, database, upsert_data_part_list)
            if dic["response_code"]==400:    
                return Response(json.dumps(dic),headers ={'Content-Type':'application/json'})
            
        if reminder==0:
            pass
        else:
            logging.info("reminder start!")
            upsert_trigger_time_part_list = upsert_trigger_time_list[(quotient)*batch_size:-1]
            upsert_data_part_list = upsert_data_list[(quotient)*batch_size:-1]
            if dic["response_code"]==400:
                return Response(json.dumps(dic),headers ={'Content-Type':'application/json'})
            dic = upsert_prediction(server, user, password, database, upsert_data_part_list)
            if dic["response_code"]==400:    
                return Response(json.dumps(dic),headers ={'Content-Type':'application/json'})
        
        logging.info("done!")
        dictionary = {"response_code":200}
        logging.info('======prediction_duration_done======')
    except Exception as e :
        logging.info(e) 
        logging.info("sql connect fail")
        dictionary = {"response_code":400} # sql connect fail
    return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

# using transaction and prediction record to caculate target client
@app.route('/get_duration',methods=["GET","POST"])
def get_duration():
    logging.info('======get_duration_start======')
    traffic_control = request.json["traffic_control"] # assume tarffic control is 7 days
    sending_group = int(request.json["sending_group"]) # split table to 24 part
    logging.info('======sending group is : {}======'.format(sending_group))
    
    # get MQ transaction from redis
    key_list = []
    value_list = []
    keys = r.keys()
    for key in keys:
        key_list.append(int(key.decode('utf-8'))) #bytes need decode
        value = r.get(key)
        value_list.append(value.decode('utf-8')) #bytes need decode
    dic ={"member_id":key_list,"latest_transaction_time":value_list}
    MQ_record = pd.DataFrame(dic)
    
    # get prediction and trigger record from sql base on there sending group
    # sql server info 
    server = '10.11.36.178' # server ip
    user = 'user' # user name
    password = '12345678'
    database = "test" # select db name
    # upsert sql infomation
    try:
        conn = pymssql.connect(server=server,
                                user=user,
                                password=password,
                                database=database)
        cursor = conn.cursor(as_dict=True) 
        try:
            # do merge in sql for cost down
            command = "SELECT * FROM dbo.flasktest1 AS table1 INNER JOIN dbo.flasktest2 AS table2 ON table1.member_id = table2.member_id WHERE sending_group="+ str(sending_group)  + " ;"
            cursor.execute(command)
            rows = cursor.fetchall()
            predition_table = pd.DataFrame(rows)
            conn.commit()
            logging.info("get prediction record done!")
        except Exception as e :
            logging.info(e) 
            logging.info("get prediction record fail")
            dic = {}
            dictionary = {"response_code":400,"response_content":dic}
            return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
        conn.close()
    except Exception as e :
        logging.info(e) 
        logging.info("sql connection fail")
        dic = {}
        dictionary = {"response_code":400,"response_content":dic}
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    
    
    target_table = pd.merge(predition_table,MQ_record,on = "member_id",how = "inner")
    logging.info(target_table)

    # compare with now_time if predict duration less than duration than they are our target
    now_time = datetime.now()
    logging.info('======calculating duration======')
    target_list = []
    for i in range(len(target_table)):
        # get transaction duration
        transaction_time_range = now_time - datetime.strptime(target_table["latest_transaction_time"].iloc[i], "%Y-%m-%d %H:%M:%S")
        transaction_duration = transaction_time_range.days * 24 * 60 * 60  + transaction_time_range.seconds # days + seconds = total duration(seconds)

        # get trigger duration
        trigger_time_range = now_time - target_table["trigger_time"].iloc[i]
        trigger_duration = trigger_time_range.days * 24 * 60 * 60  + trigger_time_range.seconds # days + seconds = total duration(seconds)

        #find duration > predict+duration
        if ((target_table["predict_duration"].iloc[i])*24*60*60 < transaction_duration) & (trigger_duration > traffic_control*24*60*60): # in our predict duration
            target_list.append(1) # target : 1
        else:
            target_list.append(0) #not our target : 0

    target_table['target'] = target_list
    member_list = target_table[target_table['target']==1]["member_id"].to_list()

    dic = {"member_list":member_list}
    dictionary = {"response_code":200,"response_content":dic}
    logging.info('======member_list : {}======'.format(dic))
    logging.info('======get_duration_done======')
    return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

# write trigger record into sql to avoid traffic
@app.route('/write_trigger_record',methods=["GET","POST"])
def write_trigger_record():
    logging.info('======write_trigger_record_start======')
    member_id = request.json["member_list"]
    trigger_time = request.json["trigger_time"]

    data = {"member_id":member_id}
    data['trigger_time'] = list(len(data['member_id'])*[trigger_time]) # for insert trigger time

    # for new member who do not have trigger time yet
    upsert_trigger_time_list = []
    for upsert_trigger_time in zip(data['member_id'],data['trigger_time']):
        upsert_trigger_time_list.append(upsert_trigger_time)

    server = '10.11.36.178' # server ip
    user = 'user' # user name
    password = '12345678'
    database = "test" # select db name
    # upsert sql infomation
    try:
        conn = pymssql.connect(server=server,
                                user=user,
                                password=password,
                                database=database)
        cursor = conn.cursor(as_dict=True) 
        
        # upsert prediction record
        try:
            command = """MERGE INTO dbo.flasktest2 AS target
            USING (VALUES (%s, %s)) AS source (member_id, trigger_time)
            ON target.member_id = source.member_id
            WHEN MATCHED THEN
                UPDATE SET target.trigger_time = source.trigger_time;"""
            cursor.executemany(command ,upsert_trigger_time_list)
            conn.commit()
            logging.info("upsert trigger time done!")
        except Exception as e :
            logging.info(e) 
            logging.info("upsert trigger time fail")
            dictionary = {"response_code":400} # upsert prediction record fail
            return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    except Exception as e :
        logging.info(e) 
        logging.info("sql connect fail")
        dictionary = {"response_code":400} # sql connect fail
        return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})
    dic = {}
    dictionary = {"response_code":200,"response_content":dic}
    logging.info('======write_trigger_record_done======')
    return Response(json.dumps(dictionary),headers ={'Content-Type':'application/json'})

app.run('0.0.0.0',port = 8090) # for docker

