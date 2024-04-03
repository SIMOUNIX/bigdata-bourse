#!/bin/sh
cd analyzer
make
cd ../dashboard
make
cd ..
docker compose up