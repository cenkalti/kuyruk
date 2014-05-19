# coding=utf-8
import os
import re
from setuptools import setup, find_packages


def read(*fname):
    with open(os.path.join(os.path.dirname(__file__), *fname)) as f:
        return f.read()


def get_version():
    for line in read('kuyruk', '__init__.py').splitlines():
        m = re.match(r"__version__\s*=\s'(.*)'", line)
        if m:
            return m.groups()[0].strip()
    raise Exception('Cannot find version')


setup(
    name='Kuyruk',
    version=get_version(),
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='rabbitmq distributed task queue',
    url='http://github.com/cenkalti/kuyruk',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Flask>=0.9',
        'pika>=0.9.12',
        'setproctitle>=1.1.7',
        'blinker>=1.2',
        'argparse>=1.2.1',
        'rpyc>=3.2.3',
    ],
    description='A distributed task runner',
    long_description=read('README.rst'),
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'kuyruk = kuyruk.__main__:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
