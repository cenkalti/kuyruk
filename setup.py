# coding=utf8
import os
import re
from setuptools import setup


def read(*fname):
    with open(os.path.join(os.path.dirname(__file__), *fname)) as f:
        return f.read()


def get_version():
    for line in read('kuyruk', '__init__.py').splitlines():
        m = re.match(r"""__version__\s*=\s*['"](.*)['"]""", line)
        if m:
            return m.groups()[0].strip()
    raise Exception('Cannot find version')


setup(
    name='Kuyruk',
    version=get_version(),
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='rabbitmq distributed task queue',
    url='https://github.com/cenk/kuyruk',
    packages=['kuyruk'],
    include_package_data=True,
    install_requires=[
        'amqp>=1.4.6',
        'setproctitle>=1.1.8',
        'blinker>=1.3',
        'argparse>=1.2.1',
    ],
    description='Simple task queue',
    long_description=read('README.rst'),
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'kuyruk = kuyruk.__main__:main',
        ],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
