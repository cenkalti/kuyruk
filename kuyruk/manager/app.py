import socket
from datetime import datetime
from flask import Flask, render_template, redirect, request, url_for
from kuyruk.helpers import human_time

app = Flask(__name__)


@app.route('/')
def index():
    return redirect(url_for('masters'))


@app.route('/masters')
def masters():
    return render_template('masters.html', sockets=get_sockets('master'))


@app.route('/workers')
def workers():
    return render_template('workers.html', sockets=get_sockets('worker'))


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
    master = app.manager.sockets[addr]
    master['actions'].put((request.form['action'], (), {}))
    return redirect_back()


@app.context_processor
def inject_helpers():
    return {
        'now': str(datetime.utcnow())[:19],
        'hostname': socket.gethostname(),
        'human_time': human_time,
    }


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
