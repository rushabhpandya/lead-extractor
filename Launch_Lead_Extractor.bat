@echo off
title AI Based Location Scraper Server
cd /d "%~dp0"
echo Starting ASGI Server on port 8000...
start "" /b python -m uvicorn main:app --host 127.0.0.1 --port 8000
timeout /t 2 /nobreak >nul
echo Launching application in default browser...
start "" "http://127.0.0.1:8000/?v=clean2026"
exit
