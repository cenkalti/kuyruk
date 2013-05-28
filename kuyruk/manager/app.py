import json
import socket
from datetime import datetime

from flask import Flask, render_template, redirect, request, url_for

from kuyruk.requeue import requeue
from kuyruk.channel import LazyChannel
from kuyruk.helpers import human_time
from kuyruk.helpers.json_datetime import JSONDecoder

app = Flask(__name__)


def get_redis():
    import redis
    return redis.StrictRedis(
        host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'],
        password=app.config['REDIS_PASSWORD'])


@app.route('/')
def index():
    return redirect(url_for('masters'))


@app.route('/masters')
def masters():
    return render_template('masters.html', sockets=get_sockets('master'))


@app.route('/workers')
def workers():
    return render_template('workers.html', sockets=get_sockets('worker'))


@app.route('/failed-tasks')
def failed_tasks():
    tasks = get_redis().hvals('failed_tasks')
    decoder = JSONDecoder()
    tasks = map(decoder.decode, tasks)
    return render_template('failed_tasks.html', tasks=tasks)


def get_sockets(type_):
    def gen():
        for addr, client in app.manager.clients.iteritems():
            if client.get_stat('type') == type_:
                yield addr, client
    return dict(gen())


@app.route('/queues')
def queues():
    queues = {}
    for addr, client in get_sockets('worker').iteritems():
        queue = client.get_stat('queue')
        queues[queue['name']] = queue
    return render_template('queues.html', queues=queues.values())


@app.route('/action', methods=['POST'])
def action():
    addr = str(request.args['host']), int(request.args['port'])
    master = app.manager.clients[addr]
    master.actions.put((request.form['action'], (), {}))
    return redirect_back()


@app.route('/action_all', methods=['POST'])
def action_all():
    for addr, client in get_sockets(request.args['type']).iteritems():
        client.actions.put((request.form['action'], (), {}))
    return redirect_back()


@app.route('/requeue', methods=['POST'])
def requeue_task():
    task_id = request.form['task_id']
    redis = get_redis()

    if task_id == 'ALL':
        tasks = redis.hvals('failed_tasks')
    else:
        tasks = [redis.hget('failed_tasks', task_id)]

    channel = LazyChannel(
        app.config['RABBIT_HOST'],
        app.config['RABBIT_PORT'],
        app.config['RABBIT_VIRTUAL_HOST'],
        app.config['RABBIT_USER'],
        app.config['RABBIT_PASSWORD'])
    with channel:
        for desc in tasks:
            desc = json.loads(desc)
            requeue(desc, channel)

    return redirect_back()


@app.context_processor
def inject_helpers():
    return {
        'now': str(datetime.utcnow())[:19],
        'hostname': socket.gethostname(),
        'human_time': human_time,
    }


@app.template_filter('sentry_url')
def do_sentry_url(sentry_id):
    if sentry_id:
        url = app.config['SENTRY_PROJECT_URL']
        if not url.endswith('/'):
            url += '/'
        url += 'search?q=%s' % sentry_id
        return url


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
