package config

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// TestShippedConfigHasNoUnknownKeys 守卫「配置键静默失效」这一类缺陷。
//
// FromJSONFile 用 json.Unmarshal 直接反序列化到 GlobalConfig：键名与字段名匹配不上时
// 既不报错也无日志，配置项静默退回默认值。历史上 WorkPoolSize / MaxWorkPoolTaskLen
// 即因此失效，使 worker 池实际为 10 而非配置的 100，把在途请求数封顶、写吞吐降低约 5 倍。
//
// 本测试以严格模式（DisallowUnknownFields）解码随仓库发布的 config.json：任何拼错或
// 已被重命名的键都会立即失败，而非在运行时无声降级。
func TestShippedConfigHasNoUnknownKeys(t *testing.T) {
	data, err := os.ReadFile("config.json")
	if err != nil {
		t.Fatalf("读取 config.json 失败: %v", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var c GlobalConfig
	if err := dec.Decode(&c); err != nil {
		t.Fatalf("config.json 含无法映射到 GlobalConfig 的键（该键将被静默忽略并退回默认值）: %v", err)
	}
}

// TestUnknownKeyIsDetectable 证明上述守卫确有效力：一个拼错的键必须被严格解码拒绝。
// 缺少本用例时，上面的测试可能仅仅因为「恒为真」而通过。
func TestUnknownKeyIsDetectable(t *testing.T) {
	// WorkPoolSize 正是历史上失效的那个拼写。
	const misspelled = `{"WorkPoolSize": 100}`

	dec := json.NewDecoder(bytes.NewReader([]byte(misspelled)))
	dec.DisallowUnknownFields()

	var c GlobalConfig
	if err := dec.Decode(&c); err == nil {
		t.Fatal("拼错的键应被严格解码拒绝，否则守卫无效")
	}

	// 同时确认非严格解码（即生产加载路径 FromJSONFile 的行为）确实静默忽略它——
	// 这正是该缺陷得以长期潜伏的原因。
	var loose GlobalConfig
	if err := json.Unmarshal([]byte(misspelled), &loose); err != nil {
		t.Fatalf("非严格解码不应报错: %v", err)
	}
	if loose.WorkerPoolSize != 0 {
		t.Fatalf("拼错的键不应被赋值，得到 WorkerPoolSize=%d", loose.WorkerPoolSize)
	}
}
