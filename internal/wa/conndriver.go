package wa

import (
	"context"

	"go.mau.fi/whatsmeow"
)

// connDriver mengabstraksi operasi koneksi whatsmeow agar Supervisor dapat diuji secara deterministik.
type connDriver interface {
	ConnectContext(ctx context.Context) error
	ResetConnection()
	Disconnect()
	IsConnected() bool
	IsLoggedIn() bool
}

// whatsmeowConnDriver membungkus *whatsmeow.Client sebagai connDriver nyata untuk produksi.
type whatsmeowConnDriver struct {
	cli *whatsmeow.Client
}

func (d *whatsmeowConnDriver) ConnectContext(ctx context.Context) error {
	return d.cli.ConnectContext(ctx)
}

func (d *whatsmeowConnDriver) ResetConnection() {
	d.cli.ResetConnection()
}

func (d *whatsmeowConnDriver) Disconnect() {
	d.cli.Disconnect()
}

func (d *whatsmeowConnDriver) IsConnected() bool {
	return d.cli != nil && d.cli.IsConnected()
}

func (d *whatsmeowConnDriver) IsLoggedIn() bool {
	return d.cli != nil && d.cli.IsLoggedIn()
}
