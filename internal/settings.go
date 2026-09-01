package internal

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	dispatchVal := "false"
	if m.dispatch {
		dispatchVal = "true"
	}
	return []contracts.SettingDef{
		{
			Key: "stale_after_sec", Label: "Worker stale after (s)", Type: contracts.SettingTypeInt,
			Value: fmt.Sprintf("%d", m.staleAfterSec), Default: "60",
			Description: "Heartbeat age before worker marked stale; POOL_STALE_AFTER_SEC", Group: "Pool",
		},
		{
			Key: "dispatch", Label: "Forward jobs to workers", Type: contracts.SettingTypeBool,
			Value: dispatchVal, Default: "true",
			Description: "Run dispatcher to forward assigned jobs to worker TranscodeService; POOL_DISPATCH", Group: "Pool",
		},
		{
			Key: "dispatch_timeout_sec", Label: "Dispatch timeout (s)", Type: contracts.SettingTypeInt,
			Value: fmt.Sprintf("%d", m.dispatchTimeoutSec), Default: fmt.Sprintf("%d", defaultDispatchTimeoutSec),
			Description: "Max wait for worker job completion; POOL_DISPATCH_TIMEOUT_SEC", Group: "Pool",
		},
	}
}

func (m *Module) UpdateSetting(key, value string) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	switch key {
	case "stale_after_sec":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("stale_after_sec must be positive int")
		}
		m.staleAfterSec = n
		if m.store != nil {
			m.store.SetStaleAfterSec(n)
		}
		return nil
	case "dispatch":
		on := value == "1" || value == "true"
		m.dispatch = on
		if on && m.disp == nil && m.store != nil {
			m.startDispatcher(context.Background())
		}
		if !on && m.dispCancel != nil {
			m.dispCancel()
			m.dispCancel = nil
			m.disp = nil
		}
		return nil
	case "dispatch_timeout_sec":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("dispatch_timeout_sec must be positive int")
		}
		m.dispatchTimeoutSec = n
		if m.disp != nil {
			m.disp.SetTimeout(n)
		}
		return nil
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
}
