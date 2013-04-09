# coding=utf-8
from setuptools import setup

install_requires = [
    'pika>=0.9.9, <1',
    'setproctitle>=1.1.7, <2',
]

try:
    # not available in python 2.6
    import importlib
except ImportError:
    install_requires.append('importlib>=1.0.2, <2')

setup(
    name='Kuyruk',
    version='0.1.0',
    author=u'Cenk Altı',
    license='Apache License 2.0',
    keywords='rabbitmq distributed task queue',
    url='http://github.com/cenkalti/kuyruk',
    packages=['kuyruk'],
    install_requires=install_requires,
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'kuyruk = kuyruk.__main__:main'
        ],
    },
)
