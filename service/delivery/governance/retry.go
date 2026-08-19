package governance

import (
	"context"
	"time"
)

// WithRetry 以固定退避重试 fn 至多 attempts 次（骨架）：fn 返回 nil 即成功返回；
// 失败则等待 backoff 后重试，直到 attempts 用尽返回最后一次错误。
// 退避策略此处用固定间隔（fixed backoff）；如需指数退避可在此改为 backoff<<i。
// ctx 取消时立即返回 ctx.Err()，不再重试。attempts<=0 视为 1。
func WithRetry(ctx context.Context, attempts int, backoff time.Duration, fn func() error) error {
	if attempts <= 0 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break // 最后一次失败，不再等待
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}
