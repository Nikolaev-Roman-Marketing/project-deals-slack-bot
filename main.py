import pandas as pd
from datetime import datetime, timedelta

from fast_bitrix24 import *

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from slack_loader import SlackLoader
from transformer import Transformer


def create_delivery_dates_list(**kwargs):
    """
    Находим даты ближайшей Субботы, Воскресенья, Понедельника
    На эти даты в CRM создаются сделки текущей недели
    """

    ti = kwargs['ti']

    current_weekday = int(datetime.now().weekday())
    current_date = datetime.now()

    delivery_weekdays = [5, 6, 7]
    delivery_dates = []

    for weekday in delivery_weekdays:
        delta = weekday - current_weekday
        delivery_date = (current_date + timedelta(days=delta)).strftime('%d.%m.%y')
        delivery_dates.append(delivery_date)

    ti.xcom_push(key='dates_list', value=delivery_dates)


def extract_data(**kwargs):
    """
    Получаем сделки и товары этих сделок из CRM, используя API
    """

    ti = kwargs['ti']

    dates_list = ti.xcom_pull(key='dates_list', task_ids=['create_delivery_dates_list'])[0]

    bitrix_token = Variable.get("bitrix_token")

    webhook = f"https://uzhin-doma.bitrix24.ru/rest/128/{bitrix_token}/"
    bitrix = Bitrix(webhook)

    deals = bitrix.get_all('crm.deal.list',
                           params={
                               'select': ['id'],
                               'filter': {'CLOSED': 'N', 'TITLE': dates_list}
                           })

    products = bitrix.get_by_ID('crm.deal.productrows.get', [d['ID'] for d in deals])

    ti.xcom_push(key='data_json', value=products)


def transform_data(**kwargs):
    """
    Трансформируем данные
    Считаем количество сделок, промо-наборов
    Составляем табличку проданных товаров
    """

    ti = kwargs['ti']

    raw_data = ti.xcom_pull(key='data_json', task_ids=['extract_data'])[0]

    transformer = Transformer(raw_data)

    deals_amount, promo_amount, dishes_table = transformer.run()

    ti.xcom_push(key='deals_amount', value=deals_amount)
    ti.xcom_push(key='promo_amount', value=promo_amount)
    ti.xcom_push(key='dishes_table', value=dishes_table)


def send_data_to_slack(**kwargs):
    """
    Используя API, отправляем в нужный Slack чат всю статистику
    """

    ti = kwargs['ti']

    deals_amount = ti.xcom_pull(key='deals_amount', task_ids=['transform_data'])[0]
    promo_amount = ti.xcom_pull(key='promo_amount', task_ids=['transform_data'])[0]
    dishes_table = ti.xcom_pull(key='dishes_table', task_ids=['transform_data'])[0]

    loader = SlackLoader()

    weekdays = {0: "Понедельник", 1: "Вторник", 2: "Среда",
                3: "Четверг", 4: "Пятница", 5: "Суббота",
                6: "Воскресенье"}
    current_time = str((datetime.now() + timedelta(hours=3)).strftime('%H:%M'))
    current_weekday_string = weekdays[datetime.now().weekday()]

    loader.send_message(f"{current_weekday_string} {current_time}")

    loader.send_statistic(deals_amount, promo_amount)

    loader.send_products_table_file(dishes_table)


def load_data_to_mysql(**kwargs):
    """
    Сохраняем кол-во сделок на основном сайте в MySQL
    Для Субботы, Воскресенья и Понедельника сохраняем только вечерние данные
    """

    current_weekday = int(datetime.now().weekday())
    current_hour = datetime.now().hour + 3

    if (current_weekday in [0, 6, 7]) and (current_hour < 21):

        print("we don't need data for this time")

    else:

        ti = kwargs['ti']

        deals_amount = ti.xcom_pull(key='deals_amount', task_ids=['transform_data'])[0]
        promo_amount = ti.xcom_pull(key='promo_amount', task_ids=['transform_data'])[0]

        main_site_deals = deals_amount - promo_amount

        new_data = pd.DataFrame([{'input_date': datetime.now(), 'deals': main_site_deals}])

        local_mysql_login_pass = Variable.get("local_mysql_login_pass")

        conn = create_engine(f'mysql+pymysql://{local_mysql_login_pass}@host.docker.internal/projects', echo=False)

        query = text("""
                CREATE TABLE IF NOT EXISTS deals_stats(
                    input_date DATETIME, 
                    deals INT UNSIGNED
                )""")

        conn.execute(query)

        new_data.to_sql(name='deals_stats', con=conn, if_exists='append', index=False)


args = {
    'owner': 'roma',
    'start_date': days_ago(1),
    'task_concurency': 1,
    'provide_context':  True
}


with DAG('Deals_Slack_Bot', description='Deals_Slack_Bot',
         schedule_interval='45 4,13,18 * * *', catchup=False,
         default_args=args) as dag:

    create_delivery_dates_list = PythonOperator(task_id='create_delivery_dates_list',
                                                python_callable=create_delivery_dates_list)

    extract_data = PythonOperator(task_id='extract_data',
                                  python_callable=extract_data)

    transform_data = PythonOperator(task_id='transform_data',
                                    python_callable=transform_data)

    send_data_to_slack = PythonOperator(task_id='send_data_to_slack',
                                        python_callable=send_data_to_slack)

    load_data_to_mysql = PythonOperator(task_id='load_data_to_mysql',
                                        python_callable=load_data_to_mysql)

    create_delivery_dates_list >> extract_data >> transform_data >> [send_data_to_slack, load_data_to_mysql]











