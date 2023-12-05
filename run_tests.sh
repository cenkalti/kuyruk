#!/bin/bash -euxo pipefail
docker build -t kuyruk-test .
exec docker run -it -v "/var/run/docker.sock:/var/run/docker.sock" kuyruk-test
