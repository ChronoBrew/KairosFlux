package jobctl

import (
	"errors"
	"os"
)

// errEmptyCommand 是 CmdExecutor.Run 在 spec.Command 为空时返回的错误。
// spec.Validate() 已经在 apply 阶段拒绝空 Command，这里是纵深防御——不
// 应该发生，但发生时要显式报错而不是 panic 或静默跳过。
var errEmptyCommand = errors.New("jobctl: job spec 的 command 为空")

// osEnviron 是 os.Environ 的一层薄封装，只为了让 executor_test.go 在需要时
// 可以不依赖真实进程环境做断言（当前实现直接转发，未来如需要固定测试环境
// 可在测试文件里替换本函数指向的变量——保留这一个缝即可，不为了这一点
// 提前引入完整的依赖注入接口）。
var osEnviron = os.Environ
