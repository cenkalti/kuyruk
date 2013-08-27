import json
import socket
import logging
from datetime import datetime

from flask import Flask, render_template, redirect, request, url_for
from werkzeug.serving import run_simple

from kuyruk.requeue import Requeuer
from kuyruk.helpers import human_time, start_daemon_thread
from kuyruk.manager.server import ManagerServer
from kuyruk.helpers.json_datetime import JSONDecoder


logger = logging.getLogger(__name__)


class Manager(Flask):

    def __init__(self, kuyruk):
        from kuyruk import __version__
        super(Manager, self).__init__(__name__)
        self.debug = True
        self.kuyruk = kuyruk

        self.config.from_object(kuyruk.config)
        if self.config['MANAGER_HOST'] is None:
            self.config['MANAGER_HOST'] = '127.0.0.1'

        self.server = ManagerServer(
            self.config['MANAGER_HOST'],
            self.config['MANAGER_PORT'])

        @self.route('/')
        def index():
            return redirect(url_for('masters'))

        @self.route('/masters')
        def masters():
            return render_template('masters.html',
                                   sockets=get_sockets('master'))

        @self.route('/workers')
        def workers():
            sockets = get_sockets('worker')
            sockets2 = {}
            ppid = request.args.get('ppid', None, int)
            if ppid:
                for addr, worker in sockets.iteritems():
                    if worker.stats.get('ppid', '') == ppid:
                        sockets2[addr] = worker
            else:
                sockets2 = sockets
            return render_template('workers.html', sockets=sockets2)

        @self.route('/failed-tasks')
        def failed_tasks():
            tasks = self.get_redis().hvals('failed_tasks')
            decoder = JSONDecoder()
            tasks = map(decoder.decode, tasks)
            return render_template('failed_tasks.html', tasks=tasks)

        def get_sockets(type_):
            def gen():
                for addr, client in self.server.clients.iteritems():
                    if client.get_stat('type') == type_:
                        yield addr, client
            return dict(gen())

        @self.route('/queues')
        def queues():
            queues = {}
            for addr, client in get_sockets('worker').iteritems():
                queue = client.get_stat('queue')
                queues[queue['name']] = queue
            return render_template('queues.html', queues=queues.values())

        @self.route('/action', methods=['POST'])
        def action():
            addr = str(request.args['host']), int(request.args['port'])
            master = self.server.clients[addr]
            master.actions.put((request.form['action'], (), {}))
            return redirect_back()

        @self.route('/action_all', methods=['POST'])
        def action_all():
            for addr, client in get_sockets(request.args['type']).iteritems():
                client.actions.put((request.form['action'], (), {}))
            return redirect_back()

        @self.route('/requeue', methods=['POST'])
        def requeue_task():
            task_id = request.form['task_id']
            redis = self.get_redis()

            if task_id == 'ALL':
                tasks = redis.hvals('failed_tasks')
            else:
                tasks = [redis.hget('failed_tasks', task_id)]

            channel = self.kuyruk.channel()
            for desc in tasks:
                desc = json.loads(desc)
                Requeuer.requeue(desc, channel, redis)

            return redirect_back()

        @self.route('/delete', methods=['POST'])
        def delete_task():
            task_id = request.form['task_id']
            redis = self.get_redis()
            redis.hdel('failed_tasks', task_id)
            return redirect_back()

        @self.context_processor
        def inject_helpers():
            return {
                'now': str(datetime.utcnow())[:19],
                'hostname': socket.gethostname(),
                'human_time': human_time,
                'version': __version__,
            }

        @self.template_filter('sentry_url')
        def do_sentry_url(sentry_id):
            if sentry_id:
                url = self.config['SENTRY_PROJECT_URL']
                if not url.endswith('/'):
                    url += '/'
                url += 'search?q=%s' % sentry_id
                return url

    def get_redis(self):
        import redis
        return redis.StrictRedis(
            host=self.config['REDIS_HOST'],
            port=self.config['REDIS_PORT'],
            db=self.config['REDIS_DB'],
            password=self.config['REDIS_PASSWORD'])

    def run(self):
        t = start_daemon_thread(self.server.serve_forever)
        logger.info("Manager running in thread: %s", t.name)

        run_simple(self.config['MANAGER_HOST'],
                   self.config['MANAGER_HTTP_PORT'],
                   self, threaded=True, use_debugger=True)


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'
