@echo off
REM bandb 服务端启动脚�?
echo ========================================
echo    bandb Server
echo ========================================
echo.

cd ..\..
go run cmd/server/server.go
