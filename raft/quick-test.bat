@echo off
echo ========================================
echo Quick Raft Test Validation
echo ========================================
echo.

cd /d "%~dp0"

echo Running all tests...
go test -v -timeout 30s

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo ✓ ALL TESTS PASSED!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ✗ SOME TESTS FAILED
    echo ========================================
)

pause
