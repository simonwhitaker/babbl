package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a running babbl daemon.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(addr string) *Client {
	base := addr
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return &Client{
		baseURL: strings.TrimSuffix(base, "/"),
		// No client-side timeout: long-poll requests are bounded by context.
		http: &http.Client{},
	}
}

func (c *Client) Publish(ctx context.Context, topic, payload, from string) (Message, error) {
	u := fmt.Sprintf("%s/topics/%s", c.baseURL, url.PathEscape(topic))
	if from != "" {
		u += "?from=" + url.QueryEscape(from)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(payload))
	if err != nil {
		return Message{}, err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.http.Do(req)
	if err != nil {
		return Message{}, connError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return Message{}, httpError(resp)
	}
	var m Message
	return m, json.NewDecoder(resp.Body).Decode(&m)
}

// Next long-polls for one message. It re-issues the request as the server's own
// wait cap expires, so it keeps blocking until ctx is done. Returns ok=false
// when ctx expires with nothing delivered.
func (c *Client) Next(ctx context.Context, topic, subscriber string, fromLatest bool) (Message, bool, error) {
	q := url.Values{}
	q.Set("as", subscriber)
	q.Set("wait", maxWait.String())
	if fromLatest {
		q.Set("from", "latest")
	}
	u := fmt.Sprintf("%s/topics/%s/next?%s", c.baseURL, url.PathEscape(topic), q.Encode())

	for {
		if err := ctx.Err(); err != nil {
			return Message{}, false, nil
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return Message{}, false, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return Message{}, false, nil
			}
			return Message{}, false, connError(err)
		}
		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			continue // server-side wait elapsed; ask again.
		}
		if resp.StatusCode != http.StatusOK {
			err := httpError(resp)
			resp.Body.Close()
			return Message{}, false, err
		}
		var m Message
		err = json.NewDecoder(resp.Body).Decode(&m)
		resp.Body.Close()
		return m, err == nil, err
	}
}

func (c *Client) Peek(ctx context.Context, topic string, from, limit int) ([]Message, error) {
	q := url.Values{}
	q.Set("from", strconv.Itoa(from))
	q.Set("limit", strconv.Itoa(limit))
	u := fmt.Sprintf("%s/topics/%s?%s", c.baseURL, url.PathEscape(topic), q.Encode())

	var msgs []Message
	return msgs, c.getJSON(ctx, u, &msgs)
}

func (c *Client) Topics(ctx context.Context) ([]TopicInfo, error) {
	var infos []TopicInfo
	return infos, c.getJSON(ctx, c.baseURL+"/topics", &infos)
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return connError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return httpError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func connError(err error) error {
	return fmt.Errorf("%w (is the daemon running? try: babbl serve)", err)
}

func httpError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return fmt.Errorf("server error: %s", e.Error)
	}
	return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}
