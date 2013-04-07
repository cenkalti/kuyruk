from setuptools import setup

install_requires = ['pika>=0.9.9,<1']
try:
    # not available in python 2.6
    import importlib
except ImportError:
    install_requires.append('importlib>=1.0.2,<2')

setup(
    name='Kuyruk',
    version='0.1',
    packages=['kuyruk'],
    install_requires=install_requires,
    tests_require=['scripttest'],
    entry_points={
        'console_scripts': [
            'kuyruk = kuyruk.__main__:main'
        ],
    }
)
