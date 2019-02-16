import os
import time
import marshal
import json
import operator

import socketio
import asyncio
from aiohttp import web

HOST = os.getenv('MASTER_HOST', '0.0.0.0')
PORT = int(os.getenv('SOCKET_PORT', 5000))
CONNECTION_TIMEOUT = int(os.getenv('CONNECTION_TIMEOUT', 10))
WORKERS_MAP_TIMEOUT = int(os.getenv('WORKERS_MAP_TIMEOUT', 60))
REPLICAS_COUNT = int(os.getenv('REPLICAS_COUNT', 2))
DATA_FOLDER = os.getenv('DATA_FOLDER', 'data')
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', 'output')

# Server
sio = socketio.AsyncServer()
app = web.Application()
sio.attach(app)


def split(a, n):
    k, m = divmod(len(a), n)
    return [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def map_fn(key_in, value_in):
    return value_in, 1


def reduce_fn(key_out, values_out):
    return key_out, sum(values_out)


@sio.on('connect')
def connect(sid, environ):
    workers.append(sid)
    sio.enter_room(sid, sid)
    print('Worker connected: ', sid)


@sio.on('disconnect')
def disconnect(sid):
    if sid in workers:
        workers.remove(sid)
    # sio.leave_room(sid, sid)
    print('Worker disconnected ', sid)


@sio.on('worker_finished')
def worker_finished(sid):
    workers_finished_list.append(sid)
    sio.disconnect(sid)
    if sid in workers:
        workers.remove(sid)


async def set_workers():
    splited_data = split(
        [os.path.join(DATA_FOLDER, filename) for filename in os.listdir(DATA_FOLDER)[0:2]],
        REPLICAS_COUNT
    )
    map_fn_code = marshal.dumps(map_fn.__code__)
    reduce_fn_code = marshal.dumps(reduce_fn.__code__)

    await asyncio.gather(
        *(
            asyncio.ensure_future(
                sio.emit(
                    'set_worker',
                    data={
                        'worker_id': idx,
                        'files': splited_data[idx],
                        'map_fn': map_fn_code,
                        'reduce_fn': reduce_fn_code
                    },
                    room=workers[idx]
                )
            )
            for idx in range(REPLICAS_COUNT)
        )
    )


def process_workers_output():
    workers_result = []
    for file in os.listdir(OUTPUT_FOLDER):
        with open(os.path.join(OUTPUT_FOLDER, file), 'r') as worker_output:
            workers_result.extend(json.load(worker_output))

    # Group by key
    grouped = {}
    for key_out, value_out in workers_result:
        if key_out in grouped:
            grouped[key_out].append(value_out)
        else:
            grouped[key_out] = [value_out]

    # Master reduce part
    reduce_result = []
    for key_out, values_out in grouped.items():
        reduce_result.append(reduce_fn(key_out, values_out))

    reduce_result_sorted = sorted(reduce_result, key=operator.itemgetter(1), reverse=True)

    with open(os.path.join(OUTPUT_FOLDER, 'master.json'), 'w') as output_file:
        json.dump(reduce_result_sorted, output_file)


async def map_reduce_manager():
    timeout = time.time() + CONNECTION_TIMEOUT

    # Wait for workers connection
    while len(workers) < REPLICAS_COUNT:
        # Timeout for workers connection
        if time.time() > timeout:
            raise TimeoutError(f'{len(workers)}/{REPLICAS_COUNT} workers connected')
        await asyncio.sleep(0.5)

    print('All workers connected')
    # Send map/reduce/data to workers
    await set_workers()

    # Wait for workers to finish the job
    timeout = time.time() + WORKERS_MAP_TIMEOUT
    while len(workers_finished_list) < REPLICAS_COUNT:
        # Timeout for workers connection
        if time.time() > timeout:
            raise TimeoutError(f'{len(workers_finished_list)}/{REPLICAS_COUNT} workers finished successfully')
        await asyncio.sleep(0.5)

    print('All workers finished')
    # Process workers output
    process_workers_output()
    print('Master finished')


if __name__ == '__main__':
    global workers
    global workers_finished_list
    workers = []
    workers_finished_list = []

    asyncio.ensure_future(map_reduce_manager())
    web.run_app(app, host=HOST, port=PORT)
