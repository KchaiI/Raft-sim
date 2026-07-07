// Package raft は Raft 合意アルゴリズム (Ongaro & Ousterhout 2014, 博士論文) の
// 純粋決定論的ステートマシン実装。goroutine・wall clock・グローバル乱数を持たず、
// 入力イベントを Step で受けて出力(送信メッセージ、永続化指示、apply すべき
// エントリ)を返す。I/O・時間・乱数はすべて呼び出し側(シミュレータ)が注入する。
package raft

// None は「ノードなし」を表す ID。
const None uint64 = 0

// RNG は注入される乱数源 (SPEC §6: math/rand を import しないための抽象)。
type RNG interface {
	Intn(n int) int
}

// StateType はノードの役割。
type StateType uint8

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StatePreCandidate:
		return "PreCandidate"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	return "Unknown"
}

// EntryType はログエントリの種別。
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoop             // リーダー就任時の空エントリ (§5.4.2, Figure 8 対策)
	EntryConfig           // 構成変更 (博士論文 §4, single-server change)
)

// Entry はログエントリ。Data は不変として扱う(コピーせず共有してよい)。
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState は fsync が必要な永続状態 (論文 Figure 2)。
// commitIndex は永続化しない (DECISIONS.md D-014)。
type HardState struct {
	Term     uint64
	VotedFor uint64
}

// Config はクラスタ構成。Voters は昇順を維持する (決定論のため map を使わない)。
type Config struct {
	Voters []uint64
}

// Clone は Config の深いコピー。
func (c Config) Clone() Config {
	v := make([]uint64, len(c.Voters))
	copy(v, c.Voters)
	return Config{Voters: v}
}

// Contains は id が投票メンバーかを返す。
func (c Config) Contains(id uint64) bool {
	for _, v := range c.Voters {
		if v == id {
			return true
		}
	}
	return false
}

// Quorum は過半数のサイズ。
func (c Config) Quorum() int { return len(c.Voters)/2 + 1 }

// Snapshot はログ圧縮の単位。Data はアプリ状態の決定論的エンコード。
type Snapshot struct {
	Index  uint64
	Term   uint64
	Config Config
	Data   []byte
}

// MessageType は RPC の種別。要求/応答を非同期メッセージとしてモデル化する。
type MessageType uint8

const (
	MsgVote       MessageType = iota // RequestVote 要求 (PreVote フラグで Pre-Vote)
	MsgVoteResp                      // RequestVote 応答
	MsgApp                           // AppendEntries 要求 (ハートビート兼用)
	MsgAppResp                       // AppendEntries 応答
	MsgSnap                          // InstallSnapshot 要求
	MsgSnapResp                      // InstallSnapshot 応答
	MsgTimeoutNow                    // リーダーシップ移譲 (博士論文 §3.10)
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgSnapResp:
		return "SnapResp"
	case MsgTimeoutNow:
		return "TimeoutNow"
	}
	return "?"
}

// Message はノード間メッセージ。Entries のスライスは送信側でコピー済みであること
// (ログの backing array を共有すると後の truncate/append で破壊される)。
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64

	// 投票 (MsgVote / MsgVoteResp)
	PreVote      bool
	Force        bool // リーダーシップ移譲による選挙: leader stickiness を無視
	LastLogIndex uint64
	LastLogTerm  uint64
	Granted      bool

	// ログ複製 (MsgApp / MsgAppResp)
	PrevLogIndex  uint64
	PrevLogTerm   uint64
	Entries       []Entry
	Commit        uint64
	Success       bool
	MatchIndex    uint64 // 成功時: 複製済み末尾 index
	ConflictIndex uint64 // 失敗時: 高速バックアップ用ヒント (§5.3 末尾)
	ConflictTerm  uint64

	// スナップショット (MsgSnap / MsgSnapResp)
	Snapshot *Snapshot
}

// Persist は 1 fsync 単位の永続化指示 (DECISIONS.md D-003)。
// 適用順: HardState → Snapshot(+ReplaceLog) → Entries。
type Persist struct {
	HardState *HardState
	// Entries: Entries[0].Index 以降の既存エントリを truncate して追記する。
	Entries []Entry
	// Snapshot: 耐久化し、Index 以前のログを破棄する。
	Snapshot *Snapshot
	// ReplaceLog: Snapshot 併用時、既存ログを全破棄する (競合するログを持つ
	// フォロワーへの InstallSnapshot)。
	ReplaceLog bool
}

// Input は Step への入力イベント。
type Input interface{ isInput() }

// Tick は仮想タイマーの1目盛り。シミュレータがクロックスキューを掛けて届ける。
type Tick struct{}

// Receive はメッセージ受信。
type Receive struct{ Msg Message }

// Propose はクライアント要求のログ追加提案 (リーダーのみ受理)。
type Propose struct{ Data []byte }

// ProposeConfChange は single-server 構成変更の提案 (M6)。
type ProposeConfChange struct{ Change ConfChange }

// TransferLeadership はリーダーシップ移譲の開始 (M6)。
type TransferLeadership struct{ Target uint64 }

// CreateSnapshot はアプリがスナップショットを取ったことの通知。
// Index は lastApplied 以下であること。Data はその時点のアプリ状態。
type CreateSnapshot struct {
	Index uint64
	Data  []byte
}

func (Tick) isInput()               {}
func (Receive) isInput()            {}
func (Propose) isInput()            {}
func (ProposeConfChange) isInput()  {}
func (TransferLeadership) isInput() {}
func (CreateSnapshot) isInput()     {}

// ConfChangeType は構成変更の種別。
type ConfChangeType uint8

const (
	ConfAddServer ConfChangeType = iota
	ConfRemoveServer
)

// ConfChange は single-server 構成変更 (博士論文 §4.1)。
type ConfChange struct {
	Type ConfChangeType
	ID   uint64
}

// ProposeReply は Propose / ProposeConfChange / TransferLeadership への即時応答。
type ProposeReply struct {
	OK         bool
	Index      uint64 // OK 時: 割り当てられたログ位置
	Term       uint64
	LeaderHint uint64 // 非リーダー時: 既知のリーダー ID (0=不明)
}

// Output は Step の出力。呼び出し側は Persist を fsync してから Messages を
// 送信しなければならない (シミュレータはこの順序を強制する)。
type Output struct {
	Messages      []Message
	Persist       *Persist
	Applied       []Entry       // ステートマシンに apply すべき commit 済みエントリ
	ApplySnapshot *Snapshot     // スナップショットからアプリ状態を復元すべき指示
	Reply         *ProposeReply // Propose 系入力への応答
}
