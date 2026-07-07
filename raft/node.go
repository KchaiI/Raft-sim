package raft

import "fmt"

// Params はノードの静的パラメータ。
type Params struct {
	ID             uint64
	Voters         []uint64 // 初期構成 (昇順であること)
	ElectionTicks  int      // 選出タイムアウト基準値 ET。実際は [ET, 2ET) にランダム化
	HeartbeatTicks int
	MaxEntriesPerMsg int // 1 メッセージあたりの最大エントリ数 (0 なら 64)
	PreVote        bool
	RNG            RNG
}

// Restore は再起動時に耐久ストレージから復元する状態。
type Restore struct {
	HardState HardState
	Snapshot  *Snapshot // nil 可
	Entries   []Entry   // スナップショット以降のログ
}

// progress はリーダーが保持するフォロワーごとの複製状態 (論文 Figure 2)。
type progress struct {
	next         uint64
	match        uint64
	recentActive bool // CheckQuorum 用: 直近の election timeout 内に応答があったか
	pendingSnap  bool // スナップショット送信中 (応答まで再送しない)
}

// Node は 1 ノード分の Raft 状態。Step は決定論的:
// 同じ状態 + 同じ入力 (+ 同じ RNG 系列) → 同じ出力。
type Node struct {
	id       uint64
	rng      RNG
	et       int // ElectionTicks
	ht       int // HeartbeatTicks
	maxBatch int
	preVote  bool

	state    StateType
	term     uint64
	votedFor uint64
	log      raftLog

	commitIndex uint64
	lastApplied uint64
	leaderID    uint64

	config      Config
	configIndex uint64 // 最新の構成エントリの index (0 = スナップショット/初期構成)

	electionElapsed  int
	randTimeout      int
	heartbeatElapsed int
	quorumElapsed    int

	votes map[uint64]bool
	prs   map[uint64]*progress

	// リーダーシップ移譲 (M6)
	transferee      uint64
	transferElapsed int

	// 新サーバー catch-up (M6, DECISIONS.md D-010)
	learner uint64 // catch-up 中のサーバー (0 = なし)。1 度に 1 台のみ

	// 最新スナップショット (遅延フォロワーへの送信用に保持)
	snapshot *Snapshot

	// 構成の履歴。truncate による構成エントリの巻き戻しと、スナップショットに
	// 収める構成 (index 時点で有効だったもの) の決定に使う。昇順。
	confHistory []confRecord

	// 現在の Step の出力蓄積
	out          *Output
	hsDirty      bool
	appendedFrom uint64 // この Step で新規追記した最小 index (0 = なし)
	snapDirty    *Persist // スナップショット永続化指示 (優先)
}

// NewNode は初期状態 (空ログ・term 0) のノードを作る。
func NewNode(p Params) *Node {
	return newNode(p, Restore{})
}

// RestartNode は耐久ストレージの状態から再起動する。
func RestartNode(p Params, r Restore) *Node {
	return newNode(p, r)
}

func newNode(p Params, r Restore) *Node {
	if p.RNG == nil {
		panic("raft: RNG は必須 (乱数はシミュレータから注入する)")
	}
	if p.ElectionTicks <= 0 || p.HeartbeatTicks <= 0 {
		panic("raft: ElectionTicks / HeartbeatTicks は正であること")
	}
	mb := p.MaxEntriesPerMsg
	if mb == 0 {
		mb = 64
	}
	n := &Node{
		id:       p.ID,
		rng:      p.RNG,
		et:       p.ElectionTicks,
		ht:       p.HeartbeatTicks,
		maxBatch: mb,
		preVote:  p.PreVote,
		votedFor: None,
	}
	n.term = r.HardState.Term
	n.votedFor = r.HardState.VotedFor
	n.initConfHistory(0, Config{Voters: append([]uint64(nil), p.Voters...)})
	if r.Snapshot != nil {
		n.log.reset(r.Snapshot.Index, r.Snapshot.Term)
		n.snapshot = cloneSnap(r.Snapshot)
		n.initConfHistory(r.Snapshot.Index, r.Snapshot.Config)
		n.commitIndex = r.Snapshot.Index
		n.lastApplied = r.Snapshot.Index
	}
	for _, e := range r.Entries {
		n.log.append(e)
		if e.Type == EntryConfig {
			n.setConfig(e.Index, decodeConfig(e.Data))
		}
	}
	n.becomeFollower(n.term, None)
	return n
}

// ---- 公開アクセサ (チェッカー・トレース用。状態を変更しない) ----

func (n *Node) ID() uint64          { return n.id }
func (n *Node) State() StateType    { return n.state }
func (n *Node) Term() uint64        { return n.term }
func (n *Node) VotedFor() uint64    { return n.votedFor }
func (n *Node) LeaderID() uint64    { return n.leaderID }
func (n *Node) CommitIndex() uint64 { return n.commitIndex }
func (n *Node) LastApplied() uint64 { return n.lastApplied }
func (n *Node) FirstIndex() uint64  { return n.log.firstIndex() }
func (n *Node) LastIndex() uint64   { return n.log.lastIndex() }
func (n *Node) SnapIndex() uint64   { return n.log.snapIndex }
func (n *Node) SnapTerm() uint64    { return n.log.snapTerm }
func (n *Node) ConfigVoters() []uint64 {
	return append([]uint64(nil), n.config.Voters...)
}
func (n *Node) ConfigIndex() uint64 { return n.configIndex }

// TermAt は index i のエントリ term (スナップショット地点含む)。
func (n *Node) TermAt(i uint64) (uint64, bool) { return n.log.term(i) }

// EntryAt は index i のエントリ (読み取り専用)。範囲外は nil。
func (n *Node) EntryAt(i uint64) *Entry { return n.log.entry(i) }

// ---- Step 本体 ----

// Step は入力イベントを 1 つ処理し、出力を返す。
func (n *Node) Step(in Input) Output {
	n.out = &Output{}
	n.hsDirty = false
	n.appendedFrom = 0
	n.snapDirty = nil

	switch v := in.(type) {
	case Tick:
		n.tick()
	case Receive:
		n.receive(v.Msg)
	case Propose:
		n.propose(v.Data)
	case ProposeConfChange:
		n.proposeConfChange(v.Change)
	case TransferLeadership:
		n.transferLeadership(v.Target)
	case CreateSnapshot:
		n.createSnapshot(v.Index, v.Data)
	default:
		panic(fmt.Sprintf("raft: 未知の入力 %T", in))
	}

	out := *n.out
	// 永続化指示の組み立て。同一 Step 内の変更 (term バンプ + スナップショット等)
	// は 1 つの Persist (= 1 fsync 単位) に統合する。
	if n.snapDirty != nil || n.hsDirty || n.appendedFrom != 0 {
		p := n.snapDirty
		if p == nil {
			p = &Persist{}
		}
		if n.hsDirty {
			p.HardState = &HardState{Term: n.term, VotedFor: n.votedFor}
		}
		if n.appendedFrom != 0 {
			p.Entries = n.log.slice(n.appendedFrom, n.log.lastIndex()+1, 0)
		}
		out.Persist = p
	}
	n.out = nil
	return out
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.out.Messages = append(n.out.Messages, m)
}

func (n *Node) resetRandTimeout() {
	n.randTimeout = n.et + n.rng.Intn(n.et)
}

func (n *Node) markAppended(i uint64) {
	if n.appendedFrom == 0 || i < n.appendedFrom {
		n.appendedFrom = i
	}
}

// ---- 役割遷移 ----

func (n *Node) becomeFollower(term uint64, leader uint64) {
	if term > n.term {
		n.term = term
		n.votedFor = None
		n.hsDirty = true
	}
	n.state = StateFollower
	n.leaderID = leader
	n.votes = nil
	n.prs = nil
	n.transferee = None
	n.learner = None
	n.electionElapsed = 0
	n.resetRandTimeout()
}

func (n *Node) becomePreCandidate() {
	// Pre-Vote は term を変えず votedFor も消費しない (博士論文 §9.6)
	n.state = StatePreCandidate
	n.leaderID = None
	n.votes = map[uint64]bool{}
	n.electionElapsed = 0
	n.resetRandTimeout()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.votedFor = n.id
	n.hsDirty = true
	n.state = StateCandidate
	n.leaderID = None
	n.votes = map[uint64]bool{}
	n.electionElapsed = 0
	n.resetRandTimeout()
}

func (n *Node) becomeLeader() {
	n.state = StateLeader
	n.leaderID = n.id
	n.votes = nil
	n.heartbeatElapsed = 0
	n.quorumElapsed = 0
	n.transferee = None
	n.prs = map[uint64]*progress{}
	for _, v := range n.config.Voters {
		if v != n.id {
			n.prs[v] = &progress{next: n.log.lastIndex() + 1}
		}
	}
	// 就任 no-op: 過去 term のエントリを間接 commit するため (§5.4.2)
	n.appendEntry(Entry{Type: EntryNoop})
	n.maybeCommit()
	n.broadcastAppend()
}

// campaign は選挙を開始する。pre=true なら Pre-Vote フェーズ。
// force はリーダーシップ移譲による選挙 (stickiness を無視させる)。
func (n *Node) campaign(pre bool, force bool) {
	if !n.promotable() {
		return
	}
	var msgTerm uint64
	if pre {
		n.becomePreCandidate()
		msgTerm = n.term + 1 // 提案 term。自分の term は変えない
	} else {
		n.becomeCandidate()
		msgTerm = n.term
	}
	// 自票
	n.votes[n.id] = true
	if n.countVotes() >= n.config.Quorum() {
		if pre {
			n.campaign(false, force)
		} else {
			n.becomeLeader()
		}
		return
	}
	for _, v := range n.config.Voters {
		if v == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVote,
			To:           v,
			Term:         msgTerm,
			PreVote:      pre,
			Force:        force,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

func (n *Node) promotable() bool { return n.config.Contains(n.id) }

func (n *Node) countVotes() int {
	c := 0
	for _, v := range n.config.Voters { // map を range しない (決定論, D-006)
		if n.votes[v] {
			c++
		}
	}
	return c
}

func (n *Node) countRejects() int {
	c := 0
	for _, v := range n.config.Voters {
		if granted, ok := n.votes[v]; ok && !granted {
			c++
		}
	}
	return c
}

// ---- Tick ----

func (n *Node) tick() {
	if n.state == StateLeader {
		n.tickLeader()
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randTimeout {
		n.campaign(n.preVote, false)
	}
}

func (n *Node) tickLeader() {
	n.heartbeatElapsed++
	n.quorumElapsed++
	if n.transferee != None {
		n.transferElapsed++
		if n.transferElapsed >= n.et {
			// 移譲がタイムアウト: 通常運転に戻る
			n.transferee = None
		}
	}
	if n.quorumElapsed >= n.et {
		n.quorumElapsed = 0
		if !n.quorumActive() {
			// CheckQuorum: 過半数と疎通できないリーダーは退位する (博士論文 §6.2)
			n.becomeFollower(n.term, None)
			return
		}
		for _, v := range n.replicationTargets() {
			if pr := n.prs[v]; pr != nil {
				pr.recentActive = false
				pr.pendingSnap = false // 喪失したスナップショットの再送を許す
			}
		}
	}
	if n.heartbeatElapsed >= n.ht {
		n.heartbeatElapsed = 0
		n.broadcastAppend()
	}
}

func (n *Node) quorumActive() bool {
	c := 0
	for _, v := range n.config.Voters {
		if v == n.id {
			c++
			continue
		}
		if pr := n.prs[v]; pr != nil && pr.recentActive {
			c++
		}
	}
	return c >= n.config.Quorum()
}

// ---- メッセージ受信 ----

func (n *Node) receive(m Message) {
	if m.To != n.id {
		panic(fmt.Sprintf("raft: node %d が宛先 %d のメッセージを受信", n.id, m.To))
	}

	// term の前処理 (論文 §5.1 + Pre-Vote の例外)
	switch {
	case m.Term > n.term:
		switch m.Type {
		case MsgVote:
			if m.PreVote {
				break // Pre-Vote 要求では term を進めない
			}
			// leader stickiness (博士論文 §4.2.3): 現リーダーが生きていると
			// 信じている間は要求を退け、term も進めない。除去済みサーバーの
			// 妨害を防ぐ。Force (移譲) は例外。
			if !m.Force && n.leaderID != None && n.electionElapsed < n.et {
				n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Granted: false})
				return
			}
			n.becomeFollower(m.Term, None)
		case MsgVoteResp:
			if m.PreVote && m.Granted {
				break // Pre-Vote 賛成票は将来 term (term+1) を運ぶ
			}
			n.becomeFollower(m.Term, None)
		case MsgApp, MsgSnap:
			n.becomeFollower(m.Term, m.From)
		default:
			n.becomeFollower(m.Term, None)
		}
	case m.Term < n.term:
		switch m.Type {
		case MsgApp, MsgSnap:
			// 旧 term のリーダーに現 term を知らせて退位させる
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Success: false,
				ConflictIndex: n.log.lastIndex() + 1})
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, PreVote: m.PreVote, Granted: false})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleApp(m)
	case MsgAppResp:
		n.handleAppResp(m)
	case MsgSnap:
		n.handleSnap(m)
	case MsgSnapResp:
		n.handleSnapResp(m)
	case MsgTimeoutNow:
		n.handleTimeoutNow(m)
	}
}

func (n *Node) logUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := n.log.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= n.log.lastIndex()
}

func (n *Node) handleVote(m Message) {
	if m.PreVote {
		// この時点で m.Term > n.term (以下は前処理で除外済み)
		granted := m.Term > n.term && n.logUpToDate(m.LastLogIndex, m.LastLogTerm)
		if !m.Force && n.leaderID != None && n.electionElapsed < n.et {
			granted = false // leader stickiness
		}
		respTerm := n.term
		if granted {
			respTerm = m.Term
		}
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: respTerm, PreVote: true, Granted: granted})
		return
	}
	// 実投票 (m.Term == n.term に正規化済み)
	canVote := n.votedFor == None || n.votedFor == m.From
	granted := canVote && n.logUpToDate(m.LastLogIndex, m.LastLogTerm)
	if granted {
		n.votedFor = m.From
		n.hsDirty = true
		n.electionElapsed = 0 // 投票したら選挙タイマーをリセット (§5.2)
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Granted: granted})
}

func (n *Node) handleVoteResp(m Message) {
	switch n.state {
	case StatePreCandidate:
		if !m.PreVote || m.Term != n.term+1 || !m.Granted {
			// 反対票 (Term は相手の現 term)。より高い term は前処理で follower 化済み
			if m.PreVote && !m.Granted {
				n.votes[m.From] = false
				if n.countRejects() >= n.config.Quorum() {
					n.becomeFollower(n.term, None)
				}
			}
			return
		}
		n.votes[m.From] = true
		if n.countVotes() >= n.config.Quorum() {
			n.campaign(false, m.Force)
		}
	case StateCandidate:
		if m.PreVote || m.Term != n.term {
			return
		}
		n.votes[m.From] = m.Granted
		if n.countVotes() >= n.config.Quorum() {
			n.becomeLeader()
		} else if n.countRejects() >= n.config.Quorum() {
			n.becomeFollower(n.term, None)
		}
	}
}

// ---- AppendEntries ----

func (n *Node) handleApp(m Message) {
	// m.Term == n.term に正規化済み
	if n.state != StateFollower {
		if n.state == StateLeader {
			// 同一 term に別リーダー: Election Safety 違反。チェッカーに検出させる
			return
		}
		n.becomeFollower(m.Term, m.From)
	}
	n.leaderID = m.From
	n.electionElapsed = 0

	prev := m.PrevLogIndex
	ents := m.Entries

	// スナップショット以前は commit 済みで Log Matching により一致している
	if prev < n.log.snapIndex {
		skip := n.log.snapIndex - prev
		if uint64(len(ents)) <= skip {
			n.respondApp(m.From, true, n.log.snapIndex, 0, 0)
			return
		}
		ents = ents[skip:]
		prev = n.log.snapIndex
	}

	t, ok := n.log.term(prev)
	if !ok || t != m.PrevLogTerm {
		// 一貫性チェック失敗: conflict ヒントで高速バックアップ (§5.3)
		var ci, ct uint64
		if !ok {
			ci, ct = n.log.lastIndex()+1, 0
		} else {
			ct = t
			ci = n.log.firstIndexOfTerm(prev, t)
		}
		n.respondApp(m.From, false, 0, ci, ct)
		return
	}

	// 既存と一致するエントリはスキップし、最初の相違点から truncate + 追記
	for i, e := range ents {
		et, exists := n.log.term(e.Index)
		if exists && et == e.Term {
			continue
		}
		if exists {
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft: node %d が commit 済み index %d を truncate しようとした", n.id, e.Index))
			}
			n.log.truncateFrom(e.Index)
			n.truncateConfigsFrom(e.Index) // 未 commit の構成エントリごと削った場合の巻き戻し
		}
		for _, ne := range ents[i:] {
			cp := ne
			n.log.append(cp)
			if ne.Type == EntryConfig {
				n.setConfig(ne.Index, decodeConfig(ne.Data))
			}
		}
		n.markAppended(ents[i].Index)
		break
	}

	lastNew := prev + uint64(len(ents))
	if m.Commit > n.commitIndex {
		c := m.Commit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCommitted()
		}
	}
	n.respondApp(m.From, true, lastNew, 0, 0)
}

func (n *Node) respondApp(to uint64, success bool, match, ci, ct uint64) {
	n.send(Message{Type: MsgAppResp, To: to, Term: n.term, Success: success,
		MatchIndex: match, ConflictIndex: ci, ConflictTerm: ct})
}

func (n *Node) handleAppResp(m Message) {
	if n.state != StateLeader || m.Term != n.term {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.recentActive = true
	pr.pendingSnap = false
	if m.Success {
		if m.MatchIndex > pr.match {
			pr.match = m.MatchIndex
		}
		if pr.match+1 > pr.next {
			pr.next = pr.match + 1
		}
		n.maybeCommit()
		if n.state != StateLeader {
			return // maybeCommit → 構成変更適用で退位した可能性 (M6)
		}
		// 移譲対象が追いついたら TimeoutNow (博士論文 §3.10)
		if n.transferee == m.From && pr.match == n.log.lastIndex() {
			n.send(Message{Type: MsgTimeoutNow, To: m.From, Term: n.term})
			n.transferee = None
		}
		// 学習者 (catch-up 中の新サーバー) が追いついたら構成変更を発行 (D-010)
		n.maybePromoteLearner(m.From)
		if pr.next <= n.log.lastIndex() {
			n.sendAppend(m.From)
		}
	} else {
		// 高速バックアップ: ConflictTerm を持つなら leader 側のその term の
		// 末尾+1、なければ ConflictIndex へ
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			if li := n.log.lastIndexOfTerm(m.ConflictTerm); li != 0 {
				next = li + 1
			}
		}
		if next < 1 {
			next = 1
		}
		if next > n.log.lastIndex()+1 {
			next = n.log.lastIndex() + 1
		}
		if next <= pr.match {
			next = pr.match + 1 // 古い応答による巻き戻しを防ぐ
		}
		pr.next = next
		n.sendAppend(m.From)
	}
}

// replicationTargets はリーダーが複製を送る相手 (自分以外の voter + 学習者)。
func (n *Node) replicationTargets() []uint64 {
	var ts []uint64
	for _, v := range n.config.Voters {
		if v != n.id {
			ts = append(ts, v)
		}
	}
	if n.learner != None && !n.config.Contains(n.learner) {
		ts = append(ts, n.learner)
	}
	return ts
}

func (n *Node) broadcastAppend() {
	for _, v := range n.replicationTargets() {
		n.sendAppend(v)
	}
}

func (n *Node) sendAppend(to uint64) {
	pr := n.prs[to]
	if pr == nil {
		return
	}
	if pr.next <= n.log.snapIndex {
		n.sendSnapshot(to, pr)
		return
	}
	prev := pr.next - 1
	prevTerm, ok := n.log.term(prev)
	if !ok {
		panic(fmt.Sprintf("raft: leader %d のログに prev=%d がない (next=%d, snap=%d)", n.id, prev, pr.next, n.log.snapIndex))
	}
	ents := n.log.slice(pr.next, n.log.lastIndex()+1, n.maxBatch)
	n.send(Message{
		Type:         MsgApp,
		To:           to,
		Term:         n.term,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		Commit:       n.commitIndex,
	})
}

// maybeCommit は matchIndex の過半数中央値まで commit を進める (§5.3, §5.4.2)。
func (n *Node) maybeCommit() {
	if n.state != StateLeader {
		return
	}
	matches := make([]uint64, 0, len(n.config.Voters))
	for _, v := range n.config.Voters {
		if v == n.id {
			matches = append(matches, n.log.lastIndex())
		} else if pr := n.prs[v]; pr != nil {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	// 降順ソート (挿入ソート: 小規模で十分、追加 import 不要)
	for i := 1; i < len(matches); i++ {
		for j := i; j > 0 && matches[j] > matches[j-1]; j-- {
			matches[j], matches[j-1] = matches[j-1], matches[j]
		}
	}
	nCommit := matches[n.config.Quorum()-1]
	if nCommit <= n.commitIndex {
		return
	}
	// Figure 8 対策: 現 term のエントリのみ数えて commit する (§5.4.2)
	if t, ok := n.log.term(nCommit); !ok || t != n.term {
		return
	}
	n.commitIndex = nCommit
	n.applyCommitted()
	n.afterCommit()
	if n.state == StateLeader {
		n.broadcastAppend() // commit の伝播を早める
	}
}

// applyCommitted は (lastApplied, commitIndex] を Applied に積む。
func (n *Node) applyCommitted() {
	if n.commitIndex <= n.lastApplied {
		return
	}
	ents := n.log.slice(n.lastApplied+1, n.commitIndex+1, 0)
	n.out.Applied = append(n.out.Applied, ents...)
	n.lastApplied = n.commitIndex
}

// ---- クライアント提案 ----

func (n *Node) propose(data []byte) {
	if n.state != StateLeader || n.transferee != None {
		// 移譲中は新規提案を受けない (博士論文 §3.10)
		n.out.Reply = &ProposeReply{OK: false, LeaderHint: n.leaderID}
		return
	}
	e := Entry{Type: EntryNormal, Data: data}
	n.appendEntry(e)
	n.out.Reply = &ProposeReply{OK: true, Index: n.log.lastIndex(), Term: n.term}
	n.maybeCommit() // 単一ノードクラスタは即 commit
	if n.state == StateLeader {
		n.broadcastAppend()
	}
}

func (n *Node) appendEntry(e Entry) {
	e.Index = n.log.lastIndex() + 1
	e.Term = n.term
	n.log.append(e)
	n.markAppended(e.Index)
	if e.Type == EntryConfig {
		n.setConfig(e.Index, decodeConfig(e.Data))
	}
}

// ---- M5/M6 で実装するハンドラのプレースホルダ ----

func (n *Node) handleSnap(m Message)       { n.installSnapshot(m) }
func (n *Node) handleSnapResp(m Message)   { n.handleSnapshotResp(m) }
func (n *Node) handleTimeoutNow(m Message) { n.timeoutNow(m) }
