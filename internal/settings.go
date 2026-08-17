package internal

import (
	"fmt"
	"strconv"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return []contracts.SettingDef{{
		Key: "stale_after_sec", Label: "Worker stale after (s)", Type: contracts.SettingTypeInt,
		Value: fmt.Sprintf("%d", m.staleAfterSec), Default: "60",
		Description: "Heartbeat age before worker marked stale; POOL_STALE_AFTER_SEC", Group: "Pool",
	}}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	if key != "stale_after_sec" {
		return fmt.Errorf("unknown setting %q", key)
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("stale_after_sec must be positive int")
	}
	m.staleAfterSec = n
	if m.store != nil {
		m.store.SetStaleAfterSec(n)
	}
	return nil
}
