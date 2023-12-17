@echo off
cd /d %~dp0
cd ..
docker build -t conduit-flat-file-db -f ./scripts/Dockerfile --no-cache . || exit /b
docker run conduit-flat-file-db ./scripts/docker_run_static_checks.sh
