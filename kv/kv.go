// Package kv は Raft の上に載せる検証対象アプリケーション:
// 線形化可能な KV ストア (Get / Put / CAS) + クライアントセッション
// (clientID + seqNo による exactly-once セマンティクス)。
//
// Raft コアと同じく純粋・決定論的で、I/O も時計も持たない。
package kv

// OpType は操作種別。
type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpCAS
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpCAS:
		return "CAS"
	}
	return "?"
}

// Command はログエントリに載せるコマンド。
type Command struct {
	ClientID uint64
	Seq      uint64
	Op       OpType
	Key      string
	Value    string // Put の値 / CAS の新値
	Expect   string // CAS の期待値
}

// Result は操作結果。
type Result struct {
	Value string // Get: 現在値
	OK    bool   // Get: key 存在 / CAS: 成功 / Put: 常に true
}

// session はクライアントごとの重複排除状態。
type session struct {
	lastSeq    uint64
	lastResult Result
}

// Store は KV ステートマシン。Apply は決定論的:
// 同じコマンド列 → 同じ状態・同じ結果列。
type Store struct {
	data     map[string]string
	sessions map[uint64]*session
}

func NewStore() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]*session{}}
}

// Apply はコマンドを 1 つ適用する。fresh=false は セッションによる重複排除
// (再送されたコマンドの 2 度目以降の apply) を表し、状態は変化しない。
func (s *Store) Apply(c Command) (res Result, fresh bool) {
	sess := s.sessions[c.ClientID]
	if sess == nil {
		sess = &session{}
		s.sessions[c.ClientID] = sess
	}
	if c.Seq == sess.lastSeq && c.Seq != 0 {
		return sess.lastResult, false // 重複: キャッシュ済み応答を返す
	}
	if c.Seq < sess.lastSeq {
		// さらに古い重複。応答キャッシュは残っていないが、これを待つ
		// クライアントも存在しない (クライアントは seq を単調に進める)
		return Result{}, false
	}
	switch c.Op {
	case OpGet:
		v, ok := s.data[c.Key]
		res = Result{Value: v, OK: ok}
	case OpPut:
		s.data[c.Key] = c.Value
		res = Result{OK: true}
	case OpCAS:
		cur, ok := s.data[c.Key]
		if ok && cur == c.Expect {
			s.data[c.Key] = c.Value
			res = Result{OK: true}
		} else {
			res = Result{Value: cur, OK: false}
		}
	}
	sess.lastSeq = c.Seq
	sess.lastResult = res
	return res, true
}

// CachedResult は (clientID, seq) の応答がセッションキャッシュにあれば返す。
// commit 済みの結果のみがキャッシュされるため、どのノードから返しても安全。
func (s *Store) CachedResult(clientID, seq uint64) (Result, bool) {
	sess := s.sessions[clientID]
	if sess != nil && sess.lastSeq == seq && seq != 0 {
		return sess.lastResult, true
	}
	return Result{}, false
}

// Get は検査用の読み取り (ステートマシンを変更しない)。
func (s *Store) Get(key string) (string, bool) {
	val, ok := s.data[key]
	return val, ok
}

// Len はキー数 (検査用)。
func (s *Store) Len() int { return len(s.data) }
