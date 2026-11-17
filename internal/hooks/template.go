package hooks

import (
	"strings"
	"time"
)

// RenderTemplate mengganti placeholder yang dikenali memakai strings.NewReplacer (tanpa text/template).
func RenderTemplate(tpl string, v EventView, loc *time.Location, now time.Time) string {
	if tpl == "" || !strings.Contains(tpl, "{{") {
		return tpl
	}
	if now.IsZero() {
		now = time.Now()
	}
	if loc != nil {
		now = now.In(loc)
	}
	r := strings.NewReplacer(
		"{{push_name}}", v.PushName,
		"{{sender_pn}}", v.SenderPN,
		"{{chat}}", v.Chat,
		"{{text}}", v.Text,
		"{{date}}", now.Format("2006-01-02"),
		"{{time}}", now.Format("15:04"),
	)
	return r.Replace(tpl)
}
