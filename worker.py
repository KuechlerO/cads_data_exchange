import datetime
from data_exchange import run, DATA_EXCHANGE_VERSION
from celery import Celery

from io import BytesIO
import dill
import kombu
import redis
r = redis.Redis(host="localhost", port=6379, decode_responses=False)

def add_dill():
    registry = kombu.serialization.registry
    kombu.serialization.pickle = dill

    registry.unregister('pickle')

    def pickle_loads(s, load=dill.load):
    # used to support buffer objects
        return load(BytesIO(s))

    def pickle_dumps(obj, dumper=dill.dumps):
        return dumper(obj, protocol=kombu.serialization.pickle_protocol)

    registry.register('pickle', pickle_dumps, pickle_loads,
                      content_type='application/x-python-serialize',
                      content_encoding='binary')

add_dill()
app = Celery("tasks", backend="rpc://", broker="amqp://localhost")

app.conf.result_serializer = "pickle"
app.conf.accept_content = ["application/json", "application/x-python-serialize"]


@app.task
def run_data_exchange(dry_run: bool = True):
    result = run(dry_run=dry_run)
    return result


@app.task
def update_cached_run(dry_run: bool = True):
    r.set("run_data_exchange_submitted", dill.dumps(datetime.datetime.now()))
    result = run(dry_run=dry_run)
    result_package = {
        "time": datetime.datetime.now(),
        "data": result,
        "version": DATA_EXCHANGE_VERSION,
    }
    r.set("run_data_exchange", dill.dumps(result_package))
