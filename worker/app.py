import os
import csv
import json
import re
import operator
import marshal
import types
import socketio

HOST = os.getenv('WORKER_HOST', '127.0.0.1')
PORT = os.getenv('SOCKET_PORT', 5000)
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', 'output')

sio = socketio.Client()


def perform_map_combine(files, map_fn, reduce_fn, worker_id):
    word_regex = r'(\w*)'

    # Map each record
    map_result = []

    for idx, file in enumerate(files):
        with open(file, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                words = re.findall(word_regex, row.popitem()[1])
                words = [word.lower() for word in words if word]
                for word in words:
                    map_result.append(map_fn(word, word))
        print(f'Worker mapped {idx + 1}/{len(files)} file')

    # Group by key
    grouped = {}
    for key_out, value_out in map_result:
        if key_out in grouped:
            grouped[key_out].append(value_out)
        else:
            grouped[key_out] = [value_out]

    # This is the so-called combiner
    # Combiner reduces the data on each mapper before sending to master
    reduce_result = []
    for key_out, values_out in grouped.items():
        reduce_result.append(reduce_fn(key_out, values_out))

    reduce_result_sorted = sorted(reduce_result, key=operator.itemgetter(0))

    with open(os.path.join(OUTPUT_FOLDER, str(worker_id) + '.json'), 'w') as output_file:
        json.dump(reduce_result_sorted, output_file)

    sio.emit('worker_finished')
    print('Worker finished')


@sio.on('connect')
def on_connect():
    print('Connected to master')


@sio.on('set_worker')
def set_worker(data):
    print('Worker got job')
    files = data['files']
    worker_id = data['worker_id']

    map_code = marshal.loads(data['map_fn'])
    map_fn = types.FunctionType(map_code, globals(), 'map_fn')

    reduce_code = marshal.loads(data['reduce_fn'])
    reduce_fn = types.FunctionType(reduce_code, globals(), 'reduce_fn')

    perform_map_combine(files, map_fn, reduce_fn, worker_id)


@sio.on('disconnect')
def on_disconnect():
    print('disconnected from server')
    exit(0)


sio.connect(f'http://{HOST}:{PORT}')
sio.wait()
