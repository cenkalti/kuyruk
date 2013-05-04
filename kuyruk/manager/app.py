from flask import Flask, render_template, redirect, request

from kuyruk.helpers import human_time


def create_app(manager):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/masters')
    def masters():
        masters = {}
        for addr, struct in manager.sockets.iteritems():
            if struct['stats']['type'] == 'master':
                masters[addr] = struct
        return render_template('masters.html', sockets=masters)

    @app.route('/action/<name>')
    def action(name):
        addr = str(request.args['host']), int(request.args['port'])
        master = manager.sockets[addr]
        master['actions'].put((name, (), {}))
        return redirect_back()

    @app.context_processor
    def inject_helpers():
        return {'human_time': human_time}

    return app


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
