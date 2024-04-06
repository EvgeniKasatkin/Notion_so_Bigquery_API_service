from flask import Flask, send_file, jsonify, request, render_template
import APIClasses
import time
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import traceback
import requests
import json

app = Flask(__name__)
load_dotenv(find_dotenv())


@app.route('/table-list-update')
def simple_form():
    return render_template('input_template.html')

@app.route('/table-list-update', methods=['POST'])
def form_post():
    while 'column_variable' not in locals():
        try:
            try:
                column_variable = request.form['column_variable']
                df_to_insert = pd.DataFrame({'column_variable': [column_variable]})
                APIClasses.SQLQuery(dataset_id = str(os.getenv('dataset_id')), table_id = str(os.getenv('table_id')), destination = 'APPEND').insert_to_bq(df_to_insert)
                APIClasses.Tgmessage(chatid = os.getenv('telegram_id'), send = column_variable).message_alarm()
                return 'status-200 ' + column_variable + ' - downloaded'
            except Exception as e:
                APIClasses.Tgmessage(chatid=os.getenv('telegram_id'), send=str(traceback.format_exc())).message_alarm()
        except:
            time.sleep(1)


@app.route('/insert-to-notion')
def notion_updating():
    try:
        df_test_value, df_test_value_id  = [], []
        request_parameter = ''
        NOTION_TOKEN = str(os.getenv('NOTION_TOKEN'))
        DATABASE_ID = str(os.getenv('DATABASE_ID'))
        notion_column_test_id = str(os.getenv('column_test_id'))
        headers = {
            "Authorization": "Bearer " + NOTION_TOKEN,
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        df_for_download = APIClasses.SQLQuery(dataset_id='', table_id='', destination='').table_for_insert()
        test_value, notion_df_for_update = APIClasses.SQLQuery(dataset_id = '', table_id = '', destination = '').exists_table()
        if str(test_value) == '':
            list_of_test_value_for_insert = list(df_for_download['test_value'])
            for request_number in range(len(list_of_test_value_for_insert)):
                request_parameter = 'insert'
                if request_number % 3 == 0:
                    time.sleep(1)
                json_data = APIClasses.Configuration().json_form(request_type=request_parameter, column_test=list_of_test_value_for_insert[request_number],  DATABASE_ID = DATABASE_ID, column_test_id = notion_column_test_id)
                response = requests.post(APIClasses.NotionPagesRequest(request_type=request_parameter, page_id='').notion_request(), headers=headers, json=json_data)

                df_list_of_pages_id.append(json.loads(response.text)['id'])
                df_list_of_test_column_id.append(notion_column_test_id)
                df_list_of_test_column.append(list_of_test_value_for_insert[request_number])

            df_to_bigquery = pd.DataFrame(
                {
                    'pages_id': df_list_of_pages_id,
                    'test_column_id': df_list_of_test_column_id,
                    'test_column': df_list_of_test_column
                }
            )
            for col in list(df_to_bigquery.columns.values):
                df_to_bigquery[col] = df_to_bigquery[col].astype(str)
            result = APIClasses.SQLQuery(str(os.getenv('dataset_id')), str(os.getenv('insert_table_id')), destination='WRITE_APPEND').insert_to_bq(df_to_bigquery)
            APIClasses.Tgmessage(chatid=os.getenv('telegram_id'), send= 'Notion update sucsessfully').message_alarm()

        elif str(upload_dt) != '':
            list_of_pages_ids_exists = list(notion_df_for_update['pages_id'])
            list_of_test_value_for_update = list(df_for_download['test_value'])

            for request_number in range(len(list_of_test_value_for_update)):
                if request_number % 3 == 0:
                    time.sleep(1)

                if request_number < len(list_of_pages_ids_exists):
                    request_parameter = 'update'
                    json_data = APIClasses.Configuration().json_form(request_type=request_parameter, column_test=list_of_test_value_for_update[request_number],  DATABASE_ID = DATABASE_ID, column_test_id = notion_column_test_id)
                    response = requests.patch(APIClasses.NotionPagesRequest(request_type=request_parameter, page_id=list_of_pages_ids_exists[request_number]).notion_request(), headers=headers, json=json_data)

                    df_list_of_pages_id.append(list_of_pages_ids_exists[request_number])
                    df_list_of_test_column_id.append(notion_column_test_id)
                    df_list_of_test_column.append(list_of_test_value_for_update[request_number])

                else:
                    request_parameter = 'insert'
                    json_data = APIClasses.Configuration().json_form(request_type=request_parameter, column_test=list_of_test_value_for_update[request_number],  DATABASE_ID = DATABASE_ID, column_test_id = notion_column_test_id)
                    response = requests.post(APIClasses.NotionPagesRequest(request_type=request_parameter, page_id='').notion_request(), headers=headers, json=json_data)

                    df_list_of_pages_id.append(json.loads(response.text)['id'])
                    df_list_of_test_column_id.append(notion_column_test_id)
                    df_list_of_test_column.append(list_of_test_value_for_update[request_number])

            df_to_bigquery = pd.DataFrame(
                {
                    'pages_id': df_list_of_pages_id,
                    'test_column_id': df_list_of_test_column_id,
                    'test_column': df_list_of_test_column
                }
            )

            for col in list(df_to_bigquery.columns.values):
                df_to_bigquery[col] = df_to_bigquery[col].astype(str)

            result = APIClasses.SQLQuery(str(os.getenv('dataset_id')), str(os.getenv('insert_table_id')), destination='WRITE_APPEND').insert_to_bq(df_to_bigquery)
            APIClasses.Tgmessage(chatid = os.getenv('telegram_id'), send = 'Notion update sucsessfully').message_alarm()
        return 'status-200, ok'
    except Exception as e:
        APIClasses.Tgmessage(chatid=os.getenv('telegram_id'), send=str(traceback.format_exc())).message_alarm()
        return 'status-400'
