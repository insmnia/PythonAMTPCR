from celery import Celery
import pandas as pd

celery = Celery(__name__)
celery.conf.broker_url = 'redis://localhost:6379'  # "amqps://zgyhmyvr:DZ36NiWH98hK2SaVNyxi0wdcTDSbdUua@chimpanzee.rmq.cloudamqp.com/zgyhmyvr"
celery.conf.result_backend = 'redis://localhost:6379'


@celery.task()
def main_task_celery(row):
    row = pd.read_json(row)
    row = row.replace('Артур', "Король Артур").replace("Гриша", "Григорий Лепс")
