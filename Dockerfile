FROM ubuntu:focal

RUN apt update && \
    DEBIAN_FRONTEND=noninteractive  apt install -y \
        python3 \
        python3-pip \
        docker.io

WORKDIR /kuyruk

# install test requirements
ADD requirements.txt .
RUN pip3 install -r requirements.txt

# install project requirements
ADD setup.py MANIFEST.in README.rst ./
RUN mkdir kuyruk && touch kuyruk/__init__.py
RUN pip3 install -e .

# add test and package files
ADD setup.cfg setup.cfg
ADD tests tests
ADD kuyruk kuyruk

# run tests
ENTRYPOINT ["pytest", "-v", "--full-trace", "--cov=kuyruk"]
CMD ["tests/"]
