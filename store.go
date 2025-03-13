package atlas

import (
	"context"
	"fmt"
	"github.com/themedef/go-atlas/internal/commands"
	"github.com/themedef/go-atlas/internal/logger"
	"github.com/themedef/go-atlas/internal/pubsub"
	"github.com/themedef/go-atlas/internal/transaction"
	"log"
	"sync"
	"time"
)

type Config struct {
	CleanupInterval time.Duration
	EnableLogging   bool
	LogFile         string
}

type Entry struct {
	Value      interface{}
	Expiration time.Time
}

type DB struct {
	mu          sync.RWMutex
	data        map[string]Entry
	logger      *logger.Logger
	pubsub      *pubsub.PubSub
	config      Config
	transaction *transaction.Transaction
	commands    *commands.CommandAPI
}

func NewStore(config Config) *DB {
	dbLogger, err := logger.NewLogger(logger.Config{
		LogFile: config.LogFile,
		Enabled: config.EnableLogging,
	})
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	db := &DB{
		data:   make(map[string]Entry),
		logger: dbLogger,
		pubsub: pubsub.NewPubSub(),
		config: config,
	}

	db.transaction = transaction.NewTransaction(db)
	db.commands = commands.NewCommandAPI(db)
	go db.cleanupExpiredKeys(config.CleanupInterval)
	return db
}

func ttlSecondsToTime(ttl int) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Second * time.Duration(ttl))
}

func isExpired(e Entry) bool {
	if e.Expiration.IsZero() {
		return false
	}
	return time.Now().After(e.Expiration)
}

func (db *DB) setInternal(ctx context.Context, key string, value interface{}, ttl int, ifExists, ifNotExists bool) (bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("setInternal timeout key=%s", key)
		return false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, exists := db.data[key]

	if ifExists && !exists {
		db.logger.Warn("setInternal (XX) fail key=%s", key)
		return false, nil
	}
	if ifNotExists && exists {
		db.logger.Warn("setInternal (NX) fail key=%s", key)
		return false, nil
	}

	newEntry := Entry{
		Value:      value,
		Expiration: ttlSecondsToTime(ttl),
	}
	db.data[key] = newEntry

	var mode string
	switch {
	case ifExists:
		mode = "XX"
	case ifNotExists:
		mode = "NX"
	default:
		mode = "STD"
	}

	db.logger.Info("setInternal (%s) key=%s value=%v ttl=%d", mode, key, value, ttl)
	db.pubsub.Publish(key, fmt.Sprintf("SET %s: %v", mode, value))
	return true, nil
}

func (db *DB) Set(ctx context.Context, key string, value interface{}, ttl int) error {
	_, err := db.setInternal(ctx, key, value, ttl, false, false)
	return err
}

func (db *DB) SetNX(ctx context.Context, key string, value interface{}, ttl int) (bool, error) {
	return db.setInternal(ctx, key, value, ttl, false, true)
}

func (db *DB) SetXX(ctx context.Context, key string, value interface{}, ttl int) (bool, error) {
	return db.setInternal(ctx, key, value, ttl, true, false)
}

func (db *DB) Get(ctx context.Context, key string) (interface{}, bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("GET timeout key=%s", key)
		return nil, false, ctx.Err()
	default:
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		delete(db.data, key)
		db.logger.Info("GET key=%s (not found or expired)", key)
		return nil, false, nil
	}
	db.logger.Info("GET key=%s value=%v", key, entry.Value)
	db.pubsub.Publish(key, fmt.Sprintf("GET: %v", entry.Value))
	return entry.Value, true, nil
}

func (db *DB) Delete(ctx context.Context, key string) (bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("DELETE timeout key=%s", key)
		return false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if entry, exists := db.data[key]; exists {
		delete(db.data, key)
		db.logger.Info("DELETE key=%s oldValue=%v", key, entry.Value)
		db.pubsub.Publish(key, "DELETE")
		return true, nil
	}
	db.logger.Warn("DELETE key=%s (not found)", key)
	return false, nil
}

func (db *DB) SetCAS(ctx context.Context, key string, oldValue, newValue interface{}, ttl int) (bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("SET CAS timeout key=%s", key)
		return false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		db.logger.Warn("SET CAS key=%s (not found/expired)", key)
		return false, fmt.Errorf("key not found or expired")
	}

	if entry.Value != oldValue {
		db.logger.Warn("SET CAS failed key=%s (oldValue mismatch)", key)
		return false, fmt.Errorf("oldValue mismatch")
	}

	entry.Value = newValue
	entry.Expiration = ttlSecondsToTime(ttl)
	db.data[key] = entry

	db.logger.Info("SET CAS key=%s oldValue=%v newValue=%v ttl=%d", key, oldValue, newValue, ttl)
	db.pubsub.Publish(key, fmt.Sprintf("SET CAS: %v", newValue))
	return true, nil
}

func (db *DB) Incr(ctx context.Context, key string) (int64, bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("INCR timeout key=%s", key)
		return 0, false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		db.data[key] = Entry{Value: int64(1)}
		db.logger.Info("INCR key=%s value=1 (new)", key)
		db.pubsub.Publish(key, "INCR: 1")
		return 1, true, nil
	}

	val, ok := entry.Value.(int64)
	if !ok {
		return 0, false, fmt.Errorf("value is not an int64 (key=%s)", key)
	}

	val++
	entry.Value = val
	db.data[key] = entry

	db.logger.Info("INCR key=%s -> %d", key, val)
	db.pubsub.Publish(key, fmt.Sprintf("INCR: %d", val))
	return val, true, nil
}

func (db *DB) Decr(ctx context.Context, key string) (int64, bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("DECR timeout key=%s", key)
		return 0, false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		db.data[key] = Entry{Value: int64(-1)}
		db.logger.Info("DECR key=%s value=-1 (new)", key)
		db.pubsub.Publish(key, "DECR: -1")
		return -1, true, nil
	}

	val, ok := entry.Value.(int64)
	if !ok {
		return 0, false, fmt.Errorf("value is not an int64 (key=%s)", key)
	}

	val--
	entry.Value = val
	db.data[key] = entry

	db.logger.Info("DECR key=%s -> %d", key, val)
	db.pubsub.Publish(key, fmt.Sprintf("DECR: %d", val))
	return val, true, nil
}

func (db *DB) LPush(ctx context.Context, key string, value interface{}) error {
	select {
	case <-ctx.Done():
		db.logger.Error("LPUSH timeout key=%s", key)
		return ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if exists && !isExpired(entry) {
		list, ok := entry.Value.([]interface{})
		if !ok {
			db.logger.Error("LPUSH failed: key=%s is not a list", key)
			return fmt.Errorf("LPUSH failed: key=%s is not a list", key)
		}
		entry.Value = append([]interface{}{value}, list...)
		db.data[key] = entry
	} else {
		db.data[key] = Entry{Value: []interface{}{value}}
	}

	db.logger.Info("LPUSH key=%s value=%v", key, value)
	db.pubsub.Publish(key, fmt.Sprintf("LPUSH: %v", value))
	return nil
}

func (db *DB) RPush(ctx context.Context, key string, value interface{}) error {
	select {
	case <-ctx.Done():
		db.logger.Error("RPUSH timeout key=%s", key)
		return ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if exists && !isExpired(entry) {
		list, ok := entry.Value.([]interface{})
		if !ok {
			db.logger.Error("RPUSH failed: key=%s is not a list", key)
			return fmt.Errorf("RPUSH failed: key=%s is not a list", key)
		}
		entry.Value = append(list, value)
		db.data[key] = entry
	} else {
		db.data[key] = Entry{Value: []interface{}{value}}
	}

	db.logger.Info("RPUSH key=%s value=%v", key, value)
	db.pubsub.Publish(key, fmt.Sprintf("RPUSH: %v", value))
	return nil
}

func (db *DB) LPop(ctx context.Context, key string) (interface{}, bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("LPOP timeout key=%s", key)
		return nil, false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		return nil, false, nil
	}

	list, ok := entry.Value.([]interface{})
	if !ok || len(list) == 0 {
		return nil, false, nil
	}

	val := list[0]
	list = list[1:]
	if len(list) == 0 {
		delete(db.data, key)
	} else {
		entry.Value = list
		db.data[key] = entry
	}
	db.logger.Info("LPOP key=%s value=%v", key, val)
	db.pubsub.Publish(key, fmt.Sprintf("LPOP: %v", val))
	return val, true, nil
}

func (db *DB) RPop(ctx context.Context, key string) (interface{}, bool, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("RPOP timeout key=%s", key)
		return nil, false, ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		return nil, false, nil
	}

	list, ok := entry.Value.([]interface{})
	if !ok || len(list) == 0 {
		return nil, false, nil
	}

	val := list[len(list)-1]
	list = list[:len(list)-1]
	if len(list) == 0 {
		delete(db.data, key)
	} else {
		entry.Value = list
		db.data[key] = entry
	}
	db.logger.Info("RPOP key=%s value=%v", key, val)
	db.pubsub.Publish(key, fmt.Sprintf("RPOP: %v", val))
	return val, true, nil
}

func (db *DB) FindByValue(ctx context.Context, value interface{}) ([]string, error) {
	select {
	case <-ctx.Done():
		db.logger.Error("FIND timeout for value=%v", value)
		return nil, ctx.Err()
	default:
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	var keys []string
	for key, entry := range db.data {
		if !isExpired(entry) && entry.Value == value {
			keys = append(keys, key)
		}
	}
	db.logger.Info("FIND value=%v keys=%v", value, keys)
	return keys, nil
}

func (db *DB) UpdateTTL(ctx context.Context, key string, ttl int) error {
	select {
	case <-ctx.Done():
		db.logger.Error("UpdateTTL timeout key=%s", key)
		return ctx.Err()
	default:
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.data[key]
	if !exists || isExpired(entry) {
		db.logger.Warn("UpdateTTL failed key=%s (not found or expired)", key)
		return fmt.Errorf("key not found or expired")
	}

	if ttl <= 0 {
		entry.Expiration = time.Time{}
	} else {
		entry.Expiration = time.Now().Add(time.Second * time.Duration(ttl))
	}

	db.data[key] = entry
	db.logger.Info("UpdateTTL key=%s new TTL=%d", key, ttl)
	db.pubsub.Publish(key, fmt.Sprintf("UPDATE TTL: %d", ttl))
	return nil
}

func (db *DB) FlushAll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		db.logger.Error("FLUSH timeout")
		return ctx.Err()
	default:
	}

	go func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		db.logger.Info("Starting gradual FLUSH ALL")

		batchSize := 100
		keys := make([]string, 0, len(db.data))
		for key := range db.data {
			keys = append(keys, key)
		}

		for _, key := range keys {
			db.pubsub.Publish(key, "FLUSH ALL")
		}

		for i := 0; i < len(keys); i += batchSize {
			end := i + batchSize
			if end > len(keys) {
				end = len(keys)
			}
			for _, key := range keys[i:end] {
				delete(db.data, key)
				db.pubsub.UnsubscribeAllForKey(key)
			}

			time.Sleep(10 * time.Millisecond)
		}

		db.logger.Info("Gradual FLUSH ALL completed")
	}()

	return nil
}

func (db *DB) cleanupExpiredKeys(interval time.Duration) {
	for {
		time.Sleep(interval)

		expiredKeys := make([]string, 0)
		db.mu.RLock()
		for key, entry := range db.data {
			if isExpired(entry) {
				expiredKeys = append(expiredKeys, key)
			}
		}
		db.mu.RUnlock()

		if len(expiredKeys) > 0 {
			db.mu.Lock()
			for _, key := range expiredKeys {
				if entry, exists := db.data[key]; exists && isExpired(entry) {
					delete(db.data, key)
					db.logger.Info("EXPIRED key=%s", key)
				}
			}
			db.mu.Unlock()
		}
	}
}

func (db *DB) Subscribe(key string) chan string {
	db.logger.Info("SUBSCRIBE key=%s", key)
	return db.pubsub.Subscribe(key)
}

func (db *DB) Unsubscribe(key string, ch chan string) {
	db.logger.Info("UNSUBSCRIBE key=%s", key)
	db.pubsub.Unsubscribe(key, ch)
}

func (db *DB) Publish(key, message string) {
	db.logger.Info("PUBLISH key=%s message=%s", key, message)
	db.pubsub.Publish(key, message)
}

func (db *DB) ClosePubSub() {
	db.logger.Info("CLOSING PUBSUB")
	db.pubsub.Close()
}

func (db *DB) Logger() *logger.Logger {
	return db.logger
}

func (db *DB) Transaction() *transaction.Transaction {
	return db.transaction
}

func (db *DB) Commands() *commands.CommandAPI {
	return db.commands
}
