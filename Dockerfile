FROM ubuntu:xenial

RUN apt-get update && \
    apt-get -y install \
        python3 \
        python3-pip

WORKDIR /kuyruk

# install test requirements
ADD requirements.txt .
RUN pip3 install -r requirements.txt

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
ENTRYPOINT ["pytest", "-v", "--cov=kuyruk"]
CMD ["tests/"]
