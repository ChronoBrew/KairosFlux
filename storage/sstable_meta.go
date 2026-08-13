// 本文件是 SSTable 的元信息管理：启动时扫描目录重建元信息，以及增删查与不可变快照发布。
package storage

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
)

func (ss *SSTable) publishMetas() {
	snap := make([]*SSTableMeta, len(ss.metas))
	copy(snap, ss.metas)
	ss.snapshot.Store(&snap)
}

// openFile 取该路径的常驻只读句柄，未缓存则打开并缓存。返回 nil 表示打开失败。
func (ss *SSTable) LoadSSTableMetaList() {
	dir := ss.dir

	if err := os.MkdirAll(dir, dirPerm); err != nil {
		slog.Error("cannot create SSTable directory", "error", err)
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("cannot read SSTable directory", "error", err)
		return
	}

	metas := make([]*SSTableMeta, 0)
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())

		file, err := os.Open(fullPath)
		if err != nil {
			slog.Warn("failed to open SSTable", "file", entry.Name(), "error", err)
			continue
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			slog.Warn("failed to read key length", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			slog.Warn("failed to read key", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		file.Close()

		info, err := os.Stat(fullPath)
		if err != nil {
			slog.Warn("failed to stat SSTable", "file", entry.Name(), "error", err)
			continue
		}

		meta := &SSTableMeta{
			Level:       parseLevelFromName(entry.Name()), // 从文件名恢复 level，避免重启塌缩到 L0
			Filepath:    fullPath,
			MinKey:      keyBytes,
			MaxKey:      nil,
			Size:        info.Size(),
			MaxKeyKnown: false,
		}

		// 从块索引末项直接取 MaxKey（新格式）。不能用 EnsureMeta 的顺序扫描：它把数据段
		// 之后的块索引/布隆/footer 也当记录读，导致 MaxKey 错乱（实测退化成空串），
		// 使 getFromSSTables 的 [MinKey,MaxKey] 范围过滤把命中 key 整段跳过 → 重启后
		// 已 flush 数据全部读不到。老格式无 footer，loadBlockIndexFromFile 返回 nil，
		// 保留 MaxKeyLoaded=false 由 EnsureMeta 顺序扫描兜底（对纯数据文件正确）。
		if idx := ss.loadBlockIndexFromFile(fullPath); idx != nil && len(idx.entries) > 0 {
			meta.MaxKey = idx.entries[len(idx.entries)-1].LastKey
			meta.MaxKeyKnown = true
		}

		metas = append(metas, meta)
		count++
	}

	// 按创建时间戳升序重建 metas，等价于内存中的创建（append）序——读路径靠 metas 逆序判定
	// newest-wins，按文件名字符串排序会让旧 merged 盖过新 L0 而返回陈旧值。时间戳相同时
	// 退回文件名比较以保证确定性。
	sort.Slice(metas, func(i, j int) bool {
		ti := parseCreateTsFromName(filepath.Base(metas[i].Filepath))
		tj := parseCreateTsFromName(filepath.Base(metas[j].Filepath))
		if ti != tj {
			return ti < tj
		}
		return metas[i].Filepath < metas[j].Filepath
	})

	ss.mu.Lock()
	ss.metas = metas
	ss.publishMetas()
	ss.mu.Unlock()

	for _, meta := range metas {
		go ss.getBlockIndex(meta.Filepath) // 异步预热块索引
		go ss.getBloom(meta.Filepath)      // 异步预热布隆过滤器
	}

	slog.Info("SSTable index loaded", "files", count, "dir", dir)
}

// WriteToSSTable 将有序 entries 写入 SSTable 文件（含块索引）
func (ss *SSTable) Metas() []*SSTableMeta {
	if snap := ss.snapshot.Load(); snap != nil {
		return *snap
	}
	return nil
}

func (ss *SSTable) AddMeta(meta *SSTableMeta) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.metas = append(ss.metas, meta)
	ss.publishMetas()
}

func (ss *SSTable) RemoveMeta(target *SSTableMeta) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for i, meta := range ss.metas {
		if meta == target {
			ss.metas = append(ss.metas[:i], ss.metas[i+1:]...)
			ss.publishMetas()
			return
		}
	}
}

// LevelFiles 获取指定层级的文件列表
func (ss *SSTable) LevelFiles(level int) []*SSTableMeta {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var result []*SSTableMeta
	for _, meta := range ss.metas {
		if meta.Level == level {
			result = append(result, meta)
		}
	}
	return result
}

func (ss *SSTable) DeleteSSTable(meta *SSTableMeta) {
	if err := os.Remove(meta.Filepath); err != nil {
		slog.Warn("failed to delete SSTable", "file", meta.Filepath, "error", err)
	}
	// 与索引/布隆缓存同时剔除常驻句柄与已缓存数据块，否则每轮 compaction 泄漏一个 fd、
	// 并让已删文件的块长期占用缓存预算。
	ss.closeFile(meta.Filepath)
	ss.blocks.dropFile(meta.Filepath)
	ss.idxMu.Lock()
	delete(ss.indexCache, meta.Filepath)
	ss.idxMu.Unlock()
	ss.bloomMu.Lock()
	delete(ss.bloomCache, meta.Filepath)
	ss.bloomMu.Unlock()
}
