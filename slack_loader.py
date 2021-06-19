from datetime import datetime, timedelta
import time
import json

import pandas as pd

import slack

from airflow.models import Variable


class SlackLoader:

    def __init__(self):

        token = Variable.get("slack_token")

        self.client = slack.WebClient(token=token)
        self.channel = "#bot_environment"

    def send_message(self, message):
        self.client.chat_postMessage(channel=self.channel, text=str(message))

    def send_statistic(self, deals, promo):

        self.client.chat_postMessage(
            channel=self.channel,
            text=f"""Всего сделок : {int(deals)}. Сделок на основном сайте: {int(deals - promo)}. Промо-наборов: {int(promo)}.""")

    def send_products_table_file(self, dishes_table):
        dishes_table.to_excel("sold_products.xlsx", index=False)
        self.client.files_upload(channels=self.channel, file="sold_products.xlsx")
