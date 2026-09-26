package observability

// The Overview page: the plain-language face of RelayForge. It answers a
// first-time user's questions — what is this, what do I do, what happened —
// using data the broker already computes (topics, group membership,
// consumer lag). Technical endpoints stay available in the footer.

import (
	"html/template"
	"net/http"
	"time"
)

// OverviewData is the snapshot the Overview page renders. Plain fields,
// no broker types: the broker fills it in.
type OverviewData struct {
	Healthy     bool // broker is up and serving
	Replication string
	Streams     []StreamRow
	Groups      []GroupRow
	GeneratedAt time.Time
}

// StreamRow is one topic in plain terms.
type StreamRow struct {
	Topic      string
	Partitions int
	Events     uint64
}

// GroupRow is one consumer group in plain terms.
type GroupRow struct {
	Group   string
	Members int
	Started bool   // has the group committed any progress at all
	Lag     uint64 // events behind, summed over its partitions
	Behind  bool   // Started && Lag > 0 (drives the highlight)
}

var overviewTmpl = template.Must(template.New("overview").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>RelayForge — Overview</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 body{font-family:-apple-system,'Segoe UI',Roboto,sans-serif;margin:0;background:#f6f7f9;color:#1a202c}
 header{background:#1a365d;color:#fff;padding:26px 32px}
 h1{margin:0 0 6px;font-size:22px} .tag{opacity:.85;font-size:14px;max-width:760px}
 main{max-width:860px;margin:22px auto;padding:0 16px}
 .card{background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:16px 20px;margin-bottom:16px}
 .card h2{font-size:16px;margin:0 0 10px}
 .ok{color:#276749;font-weight:600} .bad{color:#9b2c2c;font-weight:600}
 table{width:100%;border-collapse:collapse} td,th{padding:7px 10px;border-bottom:1px solid #edf0f3;text-align:left;font-size:14px}
 th{color:#4a5568;font-weight:600}
 .muted{color:#718096} .verdict{font-weight:600} .behind{color:#975a16}
 code{background:#edf2f7;padding:2px 6px;border-radius:4px;font-size:13px}
 footer{max-width:860px;margin:6px auto 30px;padding:0 16px;font-size:13px;color:#718096}
 footer a{color:#4a5568}
</style></head>
<body>
<header><h1>RelayForge</h1>
<div class="tag">Your app's dependable drop-off point for updates: send events here — RelayForge keeps them safe — your receivers collect them at their own pace.</div></header>
<main>
<div class="card"><b>Status:</b>
{{if .Healthy}}<span class="ok">Running — all streams recovered and serving.</span>
{{else}}<span class="bad">Starting up…</span>{{end}}
{{if .Replication}}<div class="muted">{{.Replication}}</div>{{end}}</div>

<div class="card"><h2>What's stored</h2>
{{if .Streams}}<table><tr><th>Stream (topic)</th><th>Partitions</th><th>Events stored</th></tr>
{{range .Streams}}<tr><td>{{.Topic}}</td><td>{{.Partitions}}</td><td>{{.Events}}</td></tr>{{end}}</table>
{{else}}<p class="muted">Nothing yet. Send your first event:<br><code>relayctl produce -m "hello"</code></p>{{end}}</div>

<div class="card"><h2>Receivers (consumer groups)</h2>
{{if .Groups}}<table><tr><th>Group</th><th>Members</th><th>Where they are</th></tr>
{{range .Groups}}<tr><td>{{.Group}}</td><td>{{.Members}}</td>
<td class="verdict {{if .Behind}}behind{{end}}">{{if not .Started}}no progress recorded yet{{else if eq .Lag 0}}&#10003; caught up{{else}}{{.Lag}} events behind{{end}}</td></tr>{{end}}</table>
{{else}}<p class="muted">No receivers yet. Start one in another terminal:<br><code>relayctl consume -group workers -topic default</code></p>{{end}}</div>
</main>
<footer>Technical details: <a href="/metrics">metrics</a> · <a href="/debug/pprof/">profiling</a> · <a href="/healthz">health check</a> · full docs in the repository (<code>docs/</code>) · generated {{.GeneratedAt.Format "15:04:05"}}</footer>
</body></html>`))

// OverviewHandler renders the Overview page from a snapshot function.
func OverviewHandler(source func() OverviewData) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data := source()
		if data.GeneratedAt.IsZero() {
			data.GeneratedAt = time.Now()
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = overviewTmpl.Execute(w, data)
	})
}
