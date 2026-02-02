#!/bin/bash
set -e

export PATH=/usr/local/bin:/usr/bin:/bin

cd /home/mahdi/Documents/newspaper_service/docker

docker compose run --rm app >> /home/mahdi/Documents/newspaper_service/cron.log 2>&1
