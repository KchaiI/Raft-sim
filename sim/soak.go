package sim

// SoakOptions はソークテスト 1 シード分の構成 (SPEC §4.4)。
// シードの純関数: 3/5/7 ノード混合 × 全障害有効 (メッセージ喪失/重複/遅延、
// 分断、クラッシュ、fsync 境界クラッシュ、クロックスキュー、構成変更チャーン、
// リーダーシップ移譲、スナップショット)。
// この構成は CLI (run/soak/replay) とテストで共有され、同一シードは常に
// 同一の宇宙を再現する。
func SoakOptions(seed int64) Options {
	nodes := []int{3, 5, 7}[seed%3]
	spares := 0
	churn := 0.0
	if nodes >= 5 {
		spares = 2
		churn = 0.15
	}
	return Options{
		Seed:              seed,
		Nodes:             nodes,
		SpareNodes:        spares,
		Clients:           4,
		Horizon:           5 * Second,
		SnapshotThreshold: 40,
		Net: NetParams{
			DropProb:  0.10,
			DupProb:   0.05,
			DelayMin:  200 * Microsecond,
			DelayMean: 5 * Millisecond,
			DelayMax:  400 * Millisecond, // 数 election timeout 分 (SPEC §3.3)
		},
		Faults: FaultOpts{
			PPartition:    0.15,
			PCrash:        0.10,
			PCrashPersist: 0.01,
			PConfChange:   churn,
			PTransfer:     0.05,
		},
	}
}

// RunSoakSeed は 1 シード実行し、違反があれば error を返す。
// 戻り値: 完了クライアント操作数, 処理イベント数。
func RunSoakSeed(seed int64) (completeOps, events int, err error) {
	s := New(SoakOptions(seed))
	if err := s.Run(); err != nil {
		return 0, s.Events(), err
	}
	if err := s.CheckLinearizable(); err != nil {
		return 0, s.Events(), err
	}
	for _, op := range s.History() {
		if op.Complete {
			completeOps++
		}
	}
	return completeOps, s.Events(), nil
}
