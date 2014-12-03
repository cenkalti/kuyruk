# coding=utf8
from setuptools import setup

setup(
    name='Kuyruk',
    version='2.0.0',
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='rabbitmq distributed task queue',
    url='https://github.com/cenkalti/kuyruk',
    packages=['kuyruk'],
    include_package_data=True,
    install_requires=[
        'amqp>=1.4.6',
        'setproctitle>=1.1.8',
        'blinker>=1.3',
        'argparse>=1.2.1',
    ],
    description='Simple task queue',
    long_description=open('README.rst').read(),
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
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
