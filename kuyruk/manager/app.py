from flask import Flask, render_template


def create_app(manager):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/masters')
    def masters():
        return render_template('masters.html', sockets=manager.sockets)

    return app
