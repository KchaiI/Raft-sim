package sim

import (
	"bytes"
	"fmt"
)

// Trace はシミュレーションのイベントトレース。決定論の証明(同一シード2回実行の
// バイト一致)と、失敗シードの replay 出力に使う。enabled=false のときは
// 何も記録しない(ソーク実行の高速化)。
type Trace struct {
	enabled bool
	buf     bytes.Buffer
}

func NewTrace(enabled bool) *Trace { return &Trace{enabled: enabled} }

func (t *Trace) Enabled() bool { return t != nil && t.enabled }

func (t *Trace) Logf(now int64, format string, args ...interface{}) {
	if !t.Enabled() {
		return
	}
	fmt.Fprintf(&t.buf, "[%10d] ", now)
	fmt.Fprintf(&t.buf, format, args...)
	t.buf.WriteByte('\n')
}

// Bytes はトレース全体のバイト列。
func (t *Trace) Bytes() []byte { return t.buf.Bytes() }
