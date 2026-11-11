// Package wa membungkus seluruh interaksi dengan whatsmeow dan mengisolasi detail protokol.
package wa

import (
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

// ConfigureDeviceProps mengatur metadata perangkat dan mematikan full history sync sebelum pairing.
func ConfigureDeviceProps() {
	store.SetOSInfo("waapi", [3]uint32{1, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	// Minta server tidak mengirim full history sync agar tidak memicu spike RAM puluhan MB
	store.DeviceProps.RequireFullSync = proto.Bool(false)
	store.DeviceProps.HistorySyncConfig = &waCompanionReg.DeviceProps_HistorySyncConfig{
		FullSyncDaysLimit:   proto.Uint32(0),
		FullSyncSizeMbLimit: proto.Uint32(1),
		StorageQuotaMb:      proto.Uint32(1),
		RecentSyncDaysLimit: proto.Uint32(0),
		InlineInitialPayloadInE2EeMsg: proto.Bool(false),
	}
}

