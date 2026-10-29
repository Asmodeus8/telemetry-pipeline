package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type Notifier interface {
	Notify(ctx context.Context, e Event) error
}

type Webhook struct {
	URL    string
	Client *http.Client
}

func (w Webhook) Notify(ctx context.Context, e Event) error {
	b, _ := json.Marshal(e)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return send(w.client(), req)
}

func (w Webhook) client() *http.Client {
	if w.Client != nil {
		return w.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

type Telegram struct {
	Token  string
	ChatID string
	Base   string
	Client *http.Client
}

func Message(e Event) string {
	icon := "🔴"
	if e.State == Resolved {
		icon = "✅"
	}
	if e.Metric == "" {
		return fmt.Sprintf("%s %s: %s is %s", icon, e.Rule, e.Device, e.State)
	}
	return fmt.Sprintf("%s %s: %s %s = %g (%s)", icon, e.Rule, e.Device, e.Metric, e.Value, e.State)
}

func (t Telegram) Notify(ctx context.Context, e Event) error {
	base := t.Base
	if base == "" {
		base = "https://api.telegram.org"
	}
	form := url.Values{"chat_id": {t.ChatID}, "text": {Message(e)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/bot"+t.Token+"/sendMessage", bytes.NewBufferString(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := t.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	return send(c, req)
}

func send(c *http.Client, req *http.Request) error {
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned %s", req.URL.Host, resp.Status)
	}
	return nil
}
