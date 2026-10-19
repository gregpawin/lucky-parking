@echo off
cd /d "%~dp0.."
docker compose -f docker-compose.yml -f deploy/local/compose.yml %*
exit /b %ERRORLEVEL%
