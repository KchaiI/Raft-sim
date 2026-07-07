package sim

import (
	"fmt"

	"raftsim/kv"
	"raftsim/raft"
)

// KV アプリケーション層のフック (M4)。
// Raft が commit したエントリを KV ステートマシンへ適用し、
// pending 中のクライアント要求へ応答する。

func (s *Simulator) appApply(sv *Server, e raft.Entry) {
	if e.Type != raft.EntryNormal || len(e.Data) == 0 {
		return
	}
	cmd := kv.DecodeCommand(e.Data)
	res, fresh := sv.app.Apply(cmd)

	// exactly-once の機械検査: 同一 (clientID, seq) が異なるログ index で
	// 2 度状態を変化させたら、セッションによる重複排除の失敗。
	if fresh {
		k := effectKey{client: cmd.ClientID, seq: cmd.Seq}
		if prev, ok := s.effectAt[k]; ok {
			if prev != e.Index {
				s.fail(fmt.Sprintf("exactly-once 違反: client %d seq %d が index %d と %d で二重適用", cmd.ClientID, cmd.Seq, prev, e.Index))
			}
		} else {
			s.effectAt[k] = e.Index
		}
	}

	// このサーバーで待っているクライアントに応答 (ClientID 0 は M2 ワークロードの予約値)
	if seq, ok := sv.pending[cmd.ClientID]; ok && seq == cmd.Seq {
		delete(sv.pending, cmd.ClientID)
		if cmd.ClientID >= 1 && int(cmd.ClientID) <= len(s.clients) {
			c := s.clients[cmd.ClientID-1]
			s.replyToClient(sv.id, c, seq, res, true, 0)
		}
	}
}

func (s *Simulator) appRestore(sv *Server, snap *raft.Snapshot) {
	sv.app = kv.RestoreStore(snap.Data)
	sv.pending = map[uint64]uint64{}
}

func (s *Simulator) appCrash(sv *Server) {
	// 揮発状態の全喪失
	sv.app = kv.NewStore()
	sv.pending = map[uint64]uint64{}
}

func (s *Simulator) appRestart(sv *Server) {
	// 耐久スナップショットがあればそこから復元。以降のエントリは
	// Raft が commit を回復するにつれ再 apply される。
	r := sv.store.Load()
	if r.Snapshot != nil {
		sv.app = kv.RestoreStore(r.Snapshot.Data)
	} else {
		sv.app = kv.NewStore()
	}
	sv.pending = map[uint64]uint64{}
}
