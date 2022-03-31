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
    total_time = 0
    for _ in range(10):
        rows = split_df()
        pool = ThreadPoolExecutor(5)
        start = time.time()
        pool.map(main_task, rows)
        end = time.time()
        total_time += end-start
    return {"Threading time": total_time/10}


@app.get('/proc')
async def proc_bench():
    total_time = 0
    for _ in range(10):
        rows = split_df()
        pool = Pool(5)
        start = time.time()
        pool.map(main_task, (row for row in rows))
        pool.close()
        pool.join()
        end = time.time()
        total_time += end-start
    return {"Multiproccessing time": total_time/10}


@app.get('/async')
async def async_bench():
    total_time = 0
    for _ in range(10):
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
        total_time += end-start
    return {"Async time": total_time/10}


@app.get('/celery')
async def celery_bench():
    total_time = 0
    for _ in range(10):
        funcs = []
        for row in split_df():
            funcs.append(main_task_celery.s(row.to_json()))
        job = group(funcs)
        start = time.time()
        job()
        end = time.time()
        total_time += end-start
    return {"Celery time": total_time/10}


@app.get('/redis')
async def redis_bench():
    total_time = 0
    for _ in range(10):
        df = pandas.read_csv('benchmark.csv')
        q = Queue(connection=Redis())
        start = time.time()
        result = q.enqueue(main_task, df)
        end = time.time()
        total_time += end-start
    return {"Redis time": total_time/10}
