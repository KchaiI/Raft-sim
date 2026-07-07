// Package storage はシミュレートされた耐久ストレージ。
// 1 回の Apply = 1 fsync 単位 (原子的)。クラッシュ時には fsync 済みの
// 状態のみが生存する (SPEC §3.3, DECISIONS.md D-003)。
package storage

import (
	"fmt"

	"raftsim/raft"
)

// Durable は 1 ノード分の耐久状態。
type Durable struct {
	hs      raft.HardState
	snap    *raft.Snapshot
	entries []raft.Entry // スナップショット以降のログ
}

func New() *Durable { return &Durable{} }

// Apply は Persist 指示を原子的に耐久化する (= fsync 完了)。
// 適用順: HardState → Snapshot(+ReplaceLog) → Entries。
func (d *Durable) Apply(p *raft.Persist) {
	if p == nil {
		return
	}
	if p.HardState != nil {
		d.hs = *p.HardState
	}
	if p.Snapshot != nil {
		s := *p.Snapshot
		s.Config = p.Snapshot.Config.Clone()
		d.snap = &s
		if p.ReplaceLog {
			d.entries = nil
		} else {
			// スナップショット地点以前を破棄
			for len(d.entries) > 0 && d.entries[0].Index <= s.Index {
				d.entries = d.entries[1:]
			}
		}
	}
	if len(p.Entries) > 0 {
		first := p.Entries[0].Index
		// first 以降の既存エントリを truncate して追記
		keep := 0
		for keep < len(d.entries) && d.entries[keep].Index < first {
			keep++
		}
		d.entries = append(d.entries[:keep:keep], p.Entries...)
		// 連続性の検査 (呼び出し側のバグを早期に暴く)
		prev := uint64(0)
		if keep > 0 {
			prev = d.entries[keep-1].Index
		} else if d.snap != nil {
			prev = d.snap.Index
		}
		if first != prev+1 {
			panic(fmt.Sprintf("storage: 不連続な追記 first=%d prev=%d", first, prev))
		}
	}
}

// Load は再起動用に耐久状態の深いコピーを返す。
func (d *Durable) Load() raft.Restore {
	r := raft.Restore{HardState: d.hs}
	if d.snap != nil {
		s := *d.snap
		s.Config = d.snap.Config.Clone()
		r.Snapshot = &s
	}
	r.Entries = make([]raft.Entry, len(d.entries))
	copy(r.Entries, d.entries)
	return r
}

// HardState は検査用アクセサ。
func (d *Durable) HardState() raft.HardState { return d.hs }

// LogSize は耐久ログのエントリ数 (スナップショット以降)。
func (d *Durable) LogSize() int { return len(d.entries) }
