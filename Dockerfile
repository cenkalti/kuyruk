FROM ubuntu:xenial

RUN apt-get update && \
    apt-get -y install \
        python3 \
        python3-pip

WORKDIR /kuyruk

# install test requirements
ADD requirements_test.txt .
RUN pip3 install -r requirements_test.txt

# install project requirements
ADD setup.py MANIFEST.in README.rst ./
RUN mkdir kuyruk && touch kuyruk/__init__.py
RUN pip3 install -e .

# add test and package files
ADD tests tests
ADD kuyruk kuyruk
ADD setup.cfg setup.cfg
ADD test_config_docker.py /tmp/kuyruk_config.py

# run tests
CMD python3 tests/integration/wait_rabbitmq.py && pytest -v --cov=kuyruk tests/
