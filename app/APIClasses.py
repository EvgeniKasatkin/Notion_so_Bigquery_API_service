import telebot
from telebot import types
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import json

load_dotenv(find_dotenv())

class Tgmessage:
    def __init__(self, send, chatid):
        self.send = send
        self.chatid = chatid

    def message_alarm(self):
        bot = telebot.TeleBot(str(os.getenv('bot_id')))
        bot.send_message(self.chatid, self.send)

class NotionPagesRequest:
    def __init__(self, request_type, page_id):
        self.request_type = request_type
        self.page_id = page_id

    def notion_request(self):
        if self.request_type == 'insert':
            return 'https://api.notion.com/v1/pages'
        elif self.request_type == 'update':
            return 'https://api.notion.com/v1/pages/' + self.page_id

class SQLQuery:
    def __init__(self, dataset_id, table_id, destination):
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.destination = destination
        self.page_id_list = []

    def insert_to_bq(self, df_for_insert):
        self.file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(self.file_of_creds.read())
        self.file_of_creds.close()
        self.cred = service_account.Credentials.from_service_account_info(json_account_info)
        self.client = bigquery.Client(credentials=self.cred)
        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table(self.table_id)
        job_config = bigquery.LoadJobConfig()
        if self.destination == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif self.destination == 'WRITE_APPEND':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = self.client.load_table_from_dataframe(df_for_insert, table, job_config=job_config, parquet_compression='snappy')

    def exists_table(self):
        self.file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(self.file_of_creds.read())
        self.file_of_creds.close()
        self.cred = service_account.Credentials.from_service_account_info(json_account_info)
        self.client = bigquery.Client(credentials=self.cred)
        self.exists_table_name = str(os.getenv('exists_table'))
        query_string = """SELECT * FROM `%s`;""" % (self.exists_table_name)
        need_string_value = self.client.query(query_string).result().to_dataframe(create_bqstorage_client=True)
        if len(need_string_value['test_value']) > 0:
            test_value = str(need_string_value['test_value'][0])
            query_string = """SELECT * FROM `%s` where test_value = '%s';""" % (self.exists_table_name, test_value)
            notion_df_for_update = self.client.query(query_string).result().to_dataframe(create_bqstorage_client=True)
            return test_value, notion_df_for_update
        else:
            notion_df_for_update = pd.DataFrame()
            test_value = ''
            return test_value, notion_df_for_update

    def table_for_insert(self):
        self.file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(self.file_of_creds.read())
        self.file_of_creds.close()
        self.cred = service_account.Credentials.from_service_account_info(json_account_info)
        self.client = bigquery.Client(credentials=self.cred)
        self.table_name_for_insert = str(os.getenv('insert_table'))
        query_string = """select * from `%s` ;"""%(self.table_name_for_insert)
        date_of_bq = self.client.query(query_string).result().to_dataframe(create_bqstorage_client=True)
        return date_of_bq


class Configuration:
    def json_form(self, request_type, test_column, DATABASE_ID, test_column_id):
        if request_type == 'update' or request_type == 'insert':
            json_data = {
                'parent': {
                    'type': 'database_id',
                    'database_id': DATABASE_ID,
                },
                'properties': {
                    'batch_date': {
                        "id": "title",
                        'type': 'title',
                        'title': [
                            {
                                'type': 'text',
                                'text': {
                                    'content': str('test_of_title')
                                },
                            }
                        ],
                    },
                    'test_column': {
                        'id': test_column_id,
                        'type': 'rich_text',
                        'rich_text': [
                            {
                                'type': 'text',
                                'text': {
                                    'content': test_column
                                },
                            },
                        ],
                    }
                }}
        return json_data
