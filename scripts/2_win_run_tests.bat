@echo off
cd /d %~dp0
cd ..
py -m pytest tests -v
