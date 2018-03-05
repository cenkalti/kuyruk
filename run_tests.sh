#!/bin/bash -e
docker-compose rm -fsv
docker-compose up --build --exit-code-from test --abort-on-container-exit
