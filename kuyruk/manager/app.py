from flask import Flask, render_template, redirect, request, url_for


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
        return redirect_back()

    return app


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
