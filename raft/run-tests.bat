@echo off
echo ========================================
echo Running Raft Persistence Tests
echo ========================================
echo.

cd /d "%~dp0"

echo [1/8] Testing Term and VotedFor Persistence...
go test -v -run TestPersistenceTermAndVotedFor
echo.

echo [2/8] Testing Log Persistence...
go test -v -run TestPersistenceLog
echo.

echo [3/8] Testing Snapshot Creation...
go test -v -run TestSnapshotCreation
echo.

echo [4/8] Testing Snapshot Persistence and Recovery...
go test -v -run TestSnapshotPersistence
echo.

echo [5/8] Testing InstallSnapshot RPC...
go test -v -run TestInstallSnapshotRPC
echo.

echo [6/8] Testing Election Persistence...
go test -v -run TestPersistAfterElection
echo.

echo [7/8] Testing AppendEntry Persistence...
go test -v -run TestPersistAfterAppendEntry
echo.

echo [8/8] Running All Basic Tests...
go test -v -run "TestNewRaft|TestGetState|TestGetLog|TestAppendEntry|TestElectionTimeout|TestLeaderAppendsLog|TestLeaderSendsHeartbeats"
echo.

echo ========================================
echo All tests completed!
echo ========================================
pause
