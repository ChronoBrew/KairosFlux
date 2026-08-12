package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	MagicNumber uint32 = 0x52415654 // "RAFT" in ASCII
	Version     uint32 = 1
	StateFile          = "raft_state.dat"
	LogFile            = "raft_log.dat"
	SnapshotDir        = "snapshots"

	// 文件系统权限：目录 0o755、文件 0o644（标准约定，抽成命名常量避免散落魔法数）。
	dirPerm  = 0o755
	filePerm = 0o644
)

type RaftWAL struct {
	file     *os.File
	logPath  string
	metaPath string
}

type WALState struct {
	Term     int64
	VotedFor int64
}

// PersistData 包含所有需要持久化的 Raft 状态
type PersistData struct {
	CurrentTerm       int64
	VotedFor          int64
	Log               []LogEntry
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

func NewRaftWAL(dir string) (*RaftWAL, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}

	wal := &RaftWAL{
		logPath:  filepath.Join(dir, LogFile),
		metaPath: filepath.Join(dir, StateFile),
	}

	f, err := os.OpenFile(wal.logPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	wal.file = f

	return wal, nil
}

func (w *RaftWAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *RaftWAL) SaveState(term int64, votedFor int64) error {
	state := WALState{Term: term, VotedFor: votedFor}

	f, err := os.OpenFile(w.metaPath, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	// 从文件开头覆盖写入，避免 os.Create 的删除+重建开销
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, MagicNumber); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, Version); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, state.Term); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, state.VotedFor); err != nil {
		return err
	}

	return f.Sync()
}

func (w *RaftWAL) LoadState() (int64, int64, error) {
	f, err := os.Open(w.metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, -1, nil
		}
		return 0, -1, err
	}
	defer f.Close()

	var magic uint32
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return 0, -1, err
	}
	if magic != MagicNumber {
		return 0, -1, nil
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return 0, -1, err
	}

	var term int64
	if err := binary.Read(f, binary.BigEndian, &term); err != nil {
		return 0, -1, err
	}

	var votedFor int64
	if err := binary.Read(f, binary.BigEndian, &votedFor); err != nil {
		return 0, -1, err
	}

	return term, votedFor, nil
}

// writeEntry 写入单条日志的字节，但不 fsync。供 AppendLog 与 AppendLogs 复用。
func (w *RaftWAL) writeEntry(entry LogEntry) error {
	if err := binary.Write(w.file, binary.BigEndian, int64(entry.Index)); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, int64(entry.Term)); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, int64(len(entry.Command))); err != nil {
		return err
	}
	if _, err := w.file.Write(entry.Command); err != nil {
		return err
	}
	return nil
}

func (w *RaftWAL) AppendLog(entry LogEntry) error {
	if err := w.writeEntry(entry); err != nil {
		return err
	}
	return w.file.Sync()
}

// AppendLogs 批量追加日志：全部写完后只 fsync 一次（group commit），
// 将整批 N 条的 N 次 fsync 摊销为 1 次。空批次为 no-op。
func (w *RaftWAL) AppendLogs(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if err := w.writeEntry(entry); err != nil {
			return err
		}
	}
	return w.file.Sync()
}

func (w *RaftWAL) LoadLogs() ([]LogEntry, error) {
	f, err := os.Open(w.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var logs []LogEntry
	for {
		var index int64
		if err := binary.Read(f, binary.BigEndian, &index); err != nil {
			break
		}
		var term int64
		if err := binary.Read(f, binary.BigEndian, &term); err != nil {
			break
		}
		var cmdLen int64
		if err := binary.Read(f, binary.BigEndian, &cmdLen); err != nil {
			break
		}
		cmd := make([]byte, cmdLen)
		if _, err := io.ReadFull(f, cmd); err != nil {
			break
		}

		logs = append(logs, LogEntry{
			Index:   int(index),
			Term:    int(term),
			Command: cmd,
		})
	}

	return logs, nil
}

func (w *RaftWAL) Clear() error {
	if err := w.Close(); err != nil {
		return err
	}
	w.file = nil

	if err := os.Remove(w.logPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(w.metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.Create(w.logPath)
	if err != nil {
		return err
	}
	w.file = f

	return nil
}

// 从 data 中获得数据转换成快照

func (w *RaftWAL) SaveSnapshot(data []byte, lastIncludedIndex int64, lastIncludedTerm int64) error {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)
	if err := os.MkdirAll(snapshotDir, dirPerm); err != nil {
		return err
	}
	snapshotPath := filepath.Join(snapshotDir, fmt.Sprintf("%d_%d.snap", lastIncludedIndex, lastIncludedTerm))
	f, err := os.Create(snapshotPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.BigEndian, MagicNumber); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, Version); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, lastIncludedIndex); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, lastIncludedTerm); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, int64(len(data))); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// LoadLatestSnapshot 加载最新的快照
// 返回: data, lastIndex, lastTerm, error
func (w *RaftWAL) LoadLatestSnapshot() ([]byte, int64, int64, error) {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}

	if len(files) == 0 {
		return nil, 0, 0, nil
	}

	var latestFile string
	var latestIndex int64

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".snap") {
			name := file.Name()[:len(file.Name())-5]
			parts := strings.Split(name, "_")
			if len(parts) == 2 {
				index, err1 := strconv.ParseInt(parts[0], 10, 64)
				if err1 == nil && index > latestIndex {
					latestIndex = index
					latestFile = filepath.Join(snapshotDir, file.Name())
				}
			}
		}
	}

	if latestFile == "" {
		return nil, 0, 0, nil
	}

	f, err := os.Open(latestFile)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()

	var magic uint32
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return nil, 0, 0, err
	}
	if magic != MagicNumber {
		return nil, 0, 0, errors.New("invalid snapshot file")
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return nil, 0, 0, err
	}

	var lastIndex int64
	if err := binary.Read(f, binary.BigEndian, &lastIndex); err != nil {
		return nil, 0, 0, err
	}

	var lastTerm int64
	if err := binary.Read(f, binary.BigEndian, &lastTerm); err != nil {
		return nil, 0, 0, err
	}

	var dataLen int64
	if err := binary.Read(f, binary.BigEndian, &dataLen); err != nil {
		return nil, 0, 0, err
	}

	data := make([]byte, dataLen)
	if _, err := f.Read(data); err != nil {
		return nil, 0, 0, err
	}

	return data, lastIndex, lastTerm, nil
}

// DeleteOldSnapshots 删除旧版本的快照文件（保留指定的最新快照）
func (w *RaftWAL) DeleteOldSnapshots(keepIndex int64) error {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".snap") {
			name := file.Name()[:len(file.Name())-5]
			parts := strings.Split(name, "_")
			if len(parts) == 2 {
				index, err := strconv.ParseInt(parts[0], 10, 64)
				if err == nil && index < keepIndex {
					os.Remove(filepath.Join(snapshotDir, file.Name()))
				}
			}
		}
	}

	return nil
}

func (w *RaftWAL) TruncateLogs(lastIncludedIndex int64) error {
	logs, err := w.LoadLogs()
	if err != nil {
		return err
	}

	var remainingLogs []LogEntry
	for _, log := range logs {
		if int64(log.Index) > lastIncludedIndex {
			remainingLogs = append(remainingLogs, log)
		}
	}

	w.Close()

	f, err := os.Create(w.logPath)
	if err != nil {
		return err
	}
	w.file = f

	if err := w.AppendLogs(remainingLogs); err != nil {
		return err
	}

	return nil
}

// RebuildLogFile 从内存日志重建整个日志文件（仅用于日志冲突截断）
func (w *RaftWAL) RebuildLogFile(entries []LogEntry) error {
	w.Close()

	f, err := os.Create(w.logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	w.file = f

	if err := w.AppendLogs(entries); err != nil {
		return fmt.Errorf("failed to append log entries: %w", err)
	}
	return nil
}

// SavePersist 保存所有 Raft 持久化状态（currentTerm, votedFor, log）
func (w *RaftWAL) SavePersist(data PersistData) error {
	// 1. 保存 currentTerm 和 votedFor
	if err := w.SaveState(data.CurrentTerm, data.VotedFor); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	// 2. 重建日志文件
	w.Close()

	f, err := os.Create(w.logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	w.file = f

	// 3. 写入所有日志条目
	if err := w.AppendLogs(data.Log); err != nil {
		return fmt.Errorf("failed to append log entries: %w", err)
	}

	return nil
}

// LoadPersist 加载所有 Raft 持久化状态
func (w *RaftWAL) LoadPersist() (PersistData, error) {
	data := PersistData{}

	// 1. 加载 currentTerm 和 votedFor
	term, votedFor, err := w.LoadState()
	if err != nil {
		return data, fmt.Errorf("failed to load state: %w", err)
	}
	data.CurrentTerm = term
	data.VotedFor = votedFor

	// 2. 加载日志
	logs, err := w.LoadLogs()
	if err != nil {
		return data, fmt.Errorf("failed to load logs: %w", err)
	}
	data.Log = logs

	// 3. 加载快照元数据
	snapshotData, lastIndex, lastTerm, err := w.LoadLatestSnapshot()
	if err == nil && snapshotData != nil {
		data.LastIncludedIndex = lastIndex
		data.LastIncludedTerm = lastTerm
	}

	return data, nil
}

// ... existing code ...
