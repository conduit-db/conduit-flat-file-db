@echo off
cd /d %~dp0
cd ..
docker build -f ./contrib/python_base/Dockerfile . -t python_base_ffdb
