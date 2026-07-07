package sim

import "raftsim/raft"

// KV アプリケーション層のフック。M4 で実装する。
// M1-M3 では Raft コアの検証のみを行うため何もしない。

func (s *Simulator) appApply(sv *Server, e raft.Entry)         {}
func (s *Simulator) appRestore(sv *Server, snap *raft.Snapshot) {}
func (s *Simulator) appCrash(sv *Server)                        {}
func (s *Simulator) appRestart(sv *Server)                      {}
