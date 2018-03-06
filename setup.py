# coding=utf-8
import os
from setuptools import setup


def read(*fname):
    with open(os.path.join(os.path.dirname(__file__), *fname)) as f:
        return f.read()


setup(
    name='Kuyruk',
    version='8.5.1',
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='rabbitmq distributed task queue',
    url='https://github.com/cenkalti/kuyruk',
    packages=['kuyruk'],
    include_package_data=True,
    install_requires=[
        'amqp>=1.4.6',
        'blinker>=1.3',
        'monotonic>=1.2',
        'six>=1.10.0',
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
