package sim

import (
	"fmt"

	"raftsim/checker"
	"raftsim/kv"
	"raftsim/raft"
)

// クライアント層 (M4)。各クライアントは逐次に Get/Put/CAS を発行し、
// 応答が来るまで同一 (clientID, seq) で再送する (exactly-once の検証を兼ねる)。
// invoke/response の実時間区間と結果を履歴に記録し、実行後に
// 線形化可能性チェッカーで検査する (SPEC §4.2)。
//
// クライアント通信は喪失/重複/遅延の対象だが分断の対象外 (D-007)。

const clientNetBase = 10000 // ネットワーク上のクライアント ID オフセット

type Client struct {
	id  uint64 // clientID (1..)
	s   *Simulator
	seq uint64

	inflight       bool
	histIdx        int
	cmd            kv.Command
	target         uint64 // 次に送る先 (0 = ランダム)
	retryScheduled bool

	lastSeen map[string]string // キーごとの直近観測値 (CAS の期待値生成用)
}

func (s *Simulator) setupClients() {
	for i := 1; i <= s.opt.Clients; i++ {
		c := &Client{id: uint64(i), s: s, lastSeen: map[string]string{}}
		s.clients = append(s.clients, c)
		d := s.clientThinkDelay()
		s.q.At(s.now+d, c.startNextOp)
	}
}

func (s *Simulator) clientThinkDelay() int64 {
	return int64(s.rng.ExpFloat64()*float64(s.opt.ClientThinkMean)) + 1*Millisecond
}

func (c *Client) startNextOp() {
	s := c.s
	c.seq++
	c.inflight = true
	c.target = 0

	key := fmt.Sprintf("k%d", s.rng.Intn(s.opt.Keys))
	op := checker.LinOp{ClientID: c.id, Key: key, Invoke: s.now}
	cmd := kv.Command{ClientID: c.id, Seq: c.seq, Key: key}
	switch r := s.rng.Float64(); {
	case r < 0.40:
		op.Kind = checker.LinGet
		cmd.Op = kv.OpGet
	case r < 0.80:
		op.Kind = checker.LinPut
		cmd.Op = kv.OpPut
		cmd.Value = fmt.Sprintf("v%d.%d", c.id, c.seq)
		op.Arg1 = cmd.Value
	default:
		op.Kind = checker.LinCAS
		cmd.Op = kv.OpCAS
		cmd.Expect = c.lastSeen[key] // 直近に観測した値 (なければ空 = ほぼ失敗する CAS)
		cmd.Value = fmt.Sprintf("v%d.%d", c.id, c.seq)
		op.Arg1 = cmd.Expect
		op.Arg2 = cmd.Value
	}
	c.cmd = cmd
	c.histIdx = len(s.history)
	s.history = append(s.history, op)
	s.tr.Logf(s.now, "client %d: invoke #%d %s", c.id, c.seq, cmd)
	c.sendReq()
	c.armWatchdog(c.seq)
}

// sendReq は現在の op を target (未定ならランダムなサーバー) へ送る。
func (c *Client) sendReq() {
	s := c.s
	target := c.target
	if target == 0 {
		target = uint64(s.rng.Intn(s.opt.Nodes) + 1)
	}
	cmd, seq := c.cmd, c.seq
	s.net.Send(s.now, clientNetBase+c.id, target, true, "creq", func() {
		s.serverHandleClientReq(target, c, seq, cmd)
	})
}

// armWatchdog は応答が来ない場合の定期再送 (別サーバーへ)。
func (c *Client) armWatchdog(seq uint64) {
	s := c.s
	s.q.At(s.now+s.opt.ClientTimeout, func() {
		if c.inflight && c.seq == seq {
			s.tr.Logf(s.now, "client %d: timeout #%d retry", c.id, seq)
			c.target = 0
			c.sendReq()
			c.armWatchdog(seq)
		}
	})
}

// scheduleRetry は redirect 応答による再送 (多重予約はしない)。
func (c *Client) scheduleRetry(seq uint64) {
	if c.retryScheduled {
		return
	}
	c.retryScheduled = true
	s := c.s
	s.q.At(s.now+20*Millisecond, func() {
		c.retryScheduled = false
		if c.inflight && c.seq == seq {
			c.sendReq()
		}
	})
}

// handleResp はサーバーからの応答。
func (c *Client) handleResp(seq uint64, res kv.Result, ok bool, leaderHint uint64) {
	s := c.s
	if !c.inflight || seq != c.seq {
		return // 古い/重複応答
	}
	if !ok {
		// redirect / サーバー障害
		if leaderHint != 0 && leaderHint != c.target {
			c.target = leaderHint
		} else {
			c.target = 0
		}
		c.scheduleRetry(seq)
		return
	}
	op := &s.history[c.histIdx]
	op.Response = s.now
	op.Complete = true
	op.OutValue = res.Value
	op.OutOK = res.OK
	// CAS の期待値生成用に観測値を更新
	switch op.Kind {
	case checker.LinGet:
		if res.OK {
			c.lastSeen[op.Key] = res.Value
		} else {
			delete(c.lastSeen, op.Key)
		}
	case checker.LinPut:
		c.lastSeen[op.Key] = op.Arg1
	case checker.LinCAS:
		if res.OK {
			c.lastSeen[op.Key] = op.Arg2
		} else {
			c.lastSeen[op.Key] = res.Value
		}
	}
	c.inflight = false
	s.tr.Logf(s.now, "client %d: response #%d ok=%v v=%q", c.id, seq, res.OK, res.Value)
	s.q.At(s.now+s.clientThinkDelay(), c.startNextOp)
}

// serverHandleClientReq はサーバー側のクライアント要求処理。
func (s *Simulator) serverHandleClientReq(target uint64, c *Client, seq uint64, cmd kv.Command) {
	sv := s.servers[target]
	if !sv.alive() {
		s.replyToClient(target, c, seq, kv.Result{}, false, 0)
		return
	}
	// commit 済み応答のセッションキャッシュがあればどのノードからでも安全に返せる
	if res, hit := sv.app.CachedResult(cmd.ClientID, cmd.Seq); hit {
		s.replyToClient(target, c, seq, res, true, 0)
		return
	}
	if sv.node.State() != raft.StateLeader {
		s.replyToClient(target, c, seq, kv.Result{}, false, sv.node.LeaderID())
		return
	}
	reply := sv.step(raft.Propose{Data: cmd.Encode()})
	if reply == nil || !reply.OK {
		hint := uint64(0)
		if reply != nil {
			hint = reply.LeaderHint
		}
		s.replyToClient(target, c, seq, kv.Result{}, false, hint)
		return
	}
	sv.pending[cmd.ClientID] = seq // apply 時に応答する
}

// replyToClient はサーバー → クライアントの応答送信。
func (s *Simulator) replyToClient(from uint64, c *Client, seq uint64, res kv.Result, ok bool, hint uint64) {
	s.net.Send(s.now, from, clientNetBase+c.id, true, "cresp", func() {
		c.handleResp(seq, res, ok, hint)
	})
}
