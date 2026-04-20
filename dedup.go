package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nuln/agent-core"
)

func init() {
	agent.RegisterPluginConfigSpec(agent.PluginConfigSpec{
		PluginName:  "dedup",
		PluginType:  "pipe",
		Description: "Deduplicates messages by ID and sequence matching",
		Fields: []agent.ConfigField{
			{Key: "store_ttl_days", Description: "Number of days to keep message IDs for deduplication", Default: "7", Type: agent.ConfigFieldInt},
		},
	})

	agent.RegisterPipe("dedup", 200, func(pctx agent.PipeContext) agent.Pipe {
		var store agent.KVStore
		if pctx.Storage != nil {
			var err error
			store, err = pctx.Storage.GetStore("dedup")
			if err != nil {
				slog.Warn("dedup: failed to get store, falling back to memory-only", "error", err)
			}
		}

		ttlDays := 7
		if v := os.Getenv("DEDUP_STORE_TTL_DAYS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				ttlDays = n
			}
		}

		return NewMessageDedup(store, time.Duration(ttlDays)*24*time.Hour)
	})
}

const (
	// memTTL is the duration to keep entries in the L1 memory cache.
	memTTL = 60 * time.Second
)

// MessageDedup tracks recently seen message IDs to prevent duplicate processing.
// It uses a dual-layer approach: L1 in-memory cache (hot, 60s) backed by a
// persistent KVStore (cold) to survive process restarts.
type MessageDedup struct {
	mu       sync.Mutex
	seen     map[string]time.Time // L1 memory cache
	store    agent.KVStore        // persistent store (may be nil)
	storeTTL time.Duration
}

// NewMessageDedup creates a dedup pipe. If store is nil, operates in memory-only mode.
func NewMessageDedup(store agent.KVStore, storeTTL time.Duration) *MessageDedup {
	d := &MessageDedup{
		seen:     make(map[string]time.Time),
		store:    store,
		storeTTL: storeTTL,
	}
	go d.cleanupLoop()
	return d
}

func (d *MessageDedup) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		d.cleanupMemory()
		d.cleanupStore()
	}
}

// cleanupMemory removes expired entries from the L1 memory cache.
func (d *MessageDedup) cleanupMemory() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	for k, t := range d.seen {
		if now.Sub(t) > memTTL {
			delete(d.seen, k)
		}
	}
}

// cleanupStore removes expired entries from the persistent store.
func (d *MessageDedup) cleanupStore() {
	if d.store == nil {
		return
	}
	all, err := d.store.List()
	if err != nil {
		return
	}
	now := time.Now()
	for k, v := range all {
		ts := decodeTimestamp(v)
		if now.Sub(ts) > d.storeTTL {
			_ = d.store.Delete([]byte(k))
		}
	}
}

func (d *MessageDedup) Handle(_ context.Context, _ agent.Dialog, msg *agent.Message) bool {
	// Optimization: If we have a persistent store, we rely on MessageID deduplication
	// instead of rejecting everything before process start. This allows processing
	// messages that were sent while the bot was offline.
	if d.store == nil && !msg.CreateTime.IsZero() && isBeforeProcessStart(msg.CreateTime) {
		return true // Memory-only mode: reject old messages to be safe
	}

	if msg.MessageID != "" {
		return d.IsDuplicate(msg.MessageID)
	}

	// Fallback to sequence matching if ID is missing
	return d.IsSequenceDuplicate(msg)
}

// IsSequenceDuplicate checks if a message is a duplicate based on content hash sequence.
func (d *MessageDedup) IsSequenceDuplicate(msg *agent.Message) bool {
	if msg.SessionKey == "" || (msg.Content == "" && len(msg.Images) == 0) {
		return false
	}

	fingerprint := hashMessage(msg)
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if this fingerprint exists in the session's history
	historyKey := "seq:" + msg.SessionKey
	if d.store != nil {
		if data, err := d.store.Get([]byte(historyKey)); err == nil && data != nil {
			var history []string
			_ = json.Unmarshal(data, &history)
			for _, h := range history {
				if h == fingerprint {
					return true
				}
			}
			// Add to history (keep last 10)
			history = append(history, fingerprint)
			if len(history) > 10 {
				history = history[1:]
			}
			newData, _ := json.Marshal(history)
			_ = d.store.Put([]byte(historyKey), newData)
		} else {
			// First message in session
			history := []string{fingerprint}
			newData, _ := json.Marshal(history)
			_ = d.store.Put([]byte(historyKey), newData)
		}
	}

	return false
}

func hashMessage(msg *agent.Message) string {
	h := sha256.New()
	h.Write([]byte(msg.Content))
	h.Write([]byte(fmt.Sprintf("%d", msg.CreateTime.Unix())))
	return hex.EncodeToString(h.Sum(nil))
}

// IsDuplicate returns true if msgID was already seen.
// Checks L1 memory cache first, then falls back to the persistent store.
func (d *MessageDedup) IsDuplicate(msgID string) bool {
	if msgID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	// L1: check memory
	if _, ok := d.seen[msgID]; ok {
		return true
	}

	// L2: check persistent store
	if d.store != nil {
		if data, err := d.store.Get([]byte(msgID)); err == nil && data != nil {
			ts := decodeTimestamp(data)
			if time.Since(ts) <= d.storeTTL {
				// Backfill L1 cache
				d.seen[msgID] = ts
				return true
			}
			// Expired in store, clean it up
			_ = d.store.Delete([]byte(msgID))
		}
	}

	// Not seen: mark as seen in both layers
	now := time.Now()
	d.seen[msgID] = now
	if d.store != nil {
		if err := d.store.Put([]byte(msgID), encodeTimestamp(now)); err != nil {
			slog.Warn("dedup: failed to persist message ID", "msgID", msgID, "error", err)
		}
	}
	return false
}

// isBeforeProcessStart returns true if msgTime predates the current threshold.
func isBeforeProcessStart(msgTime time.Time) bool {
	return msgTime.Before(time.Now().Add(-10 * time.Minute))
}

func encodeTimestamp(t time.Time) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(t.UnixMilli()))
	return buf
}

func decodeTimestamp(data []byte) time.Time {
	if len(data) < 8 {
		return time.Time{}
	}
	ms := int64(binary.BigEndian.Uint64(data))
	return time.UnixMilli(ms)
}
