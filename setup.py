# coding=utf-8
import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

install_requires = [
    'pika>=0.9.12, <1',
    'setproctitle>=1.1.7, <2',
]

try:
    # not available in python 2.6
    import importlib
except ImportError:
    install_requires.append('importlib>=1.0.2, <2')

setup(
    name='Kuyruk',
    version='0.3.2',
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='rabbitmq distributed task queue',
    url='http://github.com/cenkalti/kuyruk',
    packages=['kuyruk'],
    install_requires=install_requires,
    description='A distributed task runner',
    long_description=read('README.rst'),
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'kuyruk = kuyruk.__main__:main',
            'kuyruk-requeue-failed-tasks = kuyruk.requeue:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
