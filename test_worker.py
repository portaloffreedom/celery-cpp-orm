import sys
import os
# sys.path.remove(os.path.dirname(__file__))
# import celery
from celery import Celery

app = Celery('task', backend='rpc://', broker='pyamqp://guest@localhost//')
# app = Celery('task', broker='pyamqp://guest@localhost//')


@app.task
def add(x, y):
    print(f"working with values {x} and {y}")
    return x + y
