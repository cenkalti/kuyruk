from flask import Flask, render_template, redirect, request

from kuyruk.helpers import human_time


def create_app(manager):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/masters')
    def masters():
        return render_template('masters.html', sockets=manager.sockets)

    @app.route('/reload')
    def reload():
        addr = str(request.args['host']), int(request.args['port'])
        master = manager.sockets[addr]
        master['actions'].put(('reload', (), {}))
        return redirect_back()

    @app.context_processor
    def context_processor():
        return {'human_time': human_time}

    return app


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
