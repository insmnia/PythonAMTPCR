import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import nest_asyncio
import pandas
from celery import group
from fastapi import FastAPI
from redis import Redis
from rq import Queue

from celery_object import main_task_celery
from logics import async_main_task, main_task, generate_csv_file, split_df

app = FastAPI()


@app.get("/generate")
async def generate(rows: int):
    generate_csv_file(rows)
    return {"message": "File generated"}


@app.get('/thread')
async def thread_bench():
    rows = split_df()
    pool = ThreadPoolExecutor(5)
    start = time.time()
    pool.map(main_task, rows)
    end = time.time()
    return {"Threading time": end - start}


@app.get('/proc')
async def proc_bench():
    rows = split_df()
    pool = Pool(5)
    start = time.time()
    pool.map(main_task, (row for row in rows))
    pool.close()
    pool.join()
    end = time.time()
    return {"Multiproccessing time": end - start}


@app.get('/async')
async def async_bench():
    nest_asyncio.apply()
    rows = split_df()
    tasks = []
    for row in rows:
        tasks.append(
            asyncio.create_task(async_main_task(row))
        )
    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))
    end = time.time()
    return {"Async time": end - start}


@app.get('/celery')
async def celery_bench():
    funcs = []
    for idx, row in pandas.read_csv("benchmark.csv").iterrows():
        funcs.append(main_task_celery.s(row.to_json()))
    job = group(funcs)
    start = time.time()
    job()
    end = time.time()
    return {"Celery time": end - start}


@app.get('/redis')
async def redis_bench():
    df = pandas.read_csv('benchmark.csv')
    start = time.time()
    q = Queue(connection=Redis())
    result = q.enqueue(main_task, df)
    end = time.time()
    return {"Redis time": end - start}
