RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672
RABBIT_USER = 'guest'
RABBIT_PASSWORD = 'guest'
IMPORT_PATH = None
EAGER = False
MAX_LOAD = 20
MAX_WORKER_RUN_TIME = None
SAVE_FAILED_TASKS = False
MANAGER_HOST = 'localhost'
MANAGER_PORT = 16501
MANAGER_HTTP_PORT = 16500
QUEUES = {
    'aslan.local': 'a, 2*b, c*3, @d'
}
IMPORTS = [
    'kuyruk.test.config',
    'kuyruk',
]
