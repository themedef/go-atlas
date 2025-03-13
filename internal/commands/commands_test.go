package commands

import (
	"context"
	"github.com/themedef/go-atlas"
	"testing"
	"time"
)

func helperCreateAPI() (*CommandAPI, context.Context) {
	db := main.NewStore(main.Config{
		CleanupInterval: 1 * time.Second,
		EnableLogging:   false,
		LogFile:         "",
	})
	ctx := context.Background()
	return NewCommandAPI(db), ctx
}

func TestCommandAPI_Set(t *testing.T) {
	api, ctx := helperCreateAPI()

	parts := []string{"SET", "myKey", "myValue"}
	got, err := api.ExecuteCommand(ctx, parts)
	if err != nil {
		t.Fatalf("ExecuteCommand error: %v", err)
	}
	if got != "OK" {
		t.Errorf("Got=%q, want=%q", got, "OK")
	}
}

func TestCommandAPI_Get(t *testing.T) {
	api, ctx := helperCreateAPI()

	_, err := api.ExecuteCommand(ctx, []string{"SET", "key1", "val1"})
	if err != nil {
		t.Fatalf("SET error: %v", err)
	}

	got, err := api.ExecuteCommand(ctx, []string{"GET", "key1"})
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	if got != "\"val1\"" {
		t.Errorf("Got=%q, want=%q", got, "\"val1\"")
	}

	got2, err2 := api.ExecuteCommand(ctx, []string{"GET", "noKey"})
	if err2 != nil {
		t.Fatalf("GET error: %v", err2)
	}
	if got2 != "(nil)" {
		t.Errorf("Got=%q, want=%q", got2, "(nil)")
	}
}

func TestCommandAPI_Del(t *testing.T) {
	api, ctx := helperCreateAPI()

	_, err := api.ExecuteCommand(ctx, []string{"SET", "willDelete", "something"})
	if err != nil {
		t.Fatalf("SET error: %v", err)
	}

	got, err := api.ExecuteCommand(ctx, []string{"DEL", "willDelete"})
	if err != nil {
		t.Fatalf("DEL error: %v", err)
	}
	if got != "true" {
		t.Errorf("Got=%q, want=%q", got, "true")
	}

	got2, err2 := api.ExecuteCommand(ctx, []string{"DEL", "willDelete"})
	if err2 != nil {
		t.Fatalf("DEL error: %v", err2)
	}
	if got2 != "false" {
		t.Errorf("Got=%q, want=%q", got2, "false")
	}
}

func TestCommandAPI_IncrDecr(t *testing.T) {
	api, ctx := helperCreateAPI()

	got, err := api.ExecuteCommand(ctx, []string{"INCR", "numKey"})
	if err != nil {
		t.Fatalf("INCR error: %v", err)
	}
	if got != "1" {
		t.Errorf("Got=%q, want=%q", got, "1")
	}

	got2, err2 := api.ExecuteCommand(ctx, []string{"INCR", "numKey"})
	if err2 != nil {
		t.Fatalf("INCR error: %v", err2)
	}
	if got2 != "2" {
		t.Errorf("Got=%q, want=%q", got2, "2")
	}

	got3, err3 := api.ExecuteCommand(ctx, []string{"DECR", "numKey"})
	if err3 != nil {
		t.Fatalf("DECR error: %v", err3)
	}
	if got3 != "1" {
		t.Errorf("Got=%q, want=%q", got3, "1")
	}
}

func TestCommandAPI_ListOps(t *testing.T) {
	api, ctx := helperCreateAPI()

	got, err := api.ExecuteCommand(ctx, []string{"LPUSH", "listKey", "val1"})
	if err != nil || got != "OK" {
		t.Fatalf("LPUSH got=%q err=%v, want=OK", got, err)
	}

	_, err2 := api.ExecuteCommand(ctx, []string{"LPUSH", "listKey", "val2"})
	if err2 != nil {
		t.Fatalf("LPUSH2 error: %v", err2)
	}

	got3, err3 := api.ExecuteCommand(ctx, []string{"RPOP", "listKey"})
	if err3 != nil {
		t.Fatalf("RPOP error: %v", err3)
	}
	if got3 != "val1" {
		t.Errorf("Got=%q, want=%q", got3, "val1")
	}

	got4, err4 := api.ExecuteCommand(ctx, []string{"LPOP", "listKey"})
	if err4 != nil {
		t.Fatalf("LPOP error: %v", err4)
	}
	if got4 != "val2" {
		t.Errorf("Got=%q, want=%q", got4, "val2")
	}

	got5, err5 := api.ExecuteCommand(ctx, []string{"RPOP", "listKey"})
	if err5 != nil {
		t.Fatalf("RPOP error: %v", err5)
	}
	if got5 != "(nil)" {
		t.Errorf("Got=%q, want=%q", got5, "(nil)")
	}
}

func TestCommandAPI_ExpireFind(t *testing.T) {
	api, ctx := helperCreateAPI()

	_, err := api.ExecuteCommand(ctx, []string{"EXPIRE", "someKey"})
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	gotF, errF := api.ExecuteCommand(ctx, []string{"FIND", "aaa"})
	if errF != nil {
		t.Fatalf("FIND error: %v", errF)
	}
	if gotF != "Keys: []" {
		t.Errorf("Got=%q, want=%q", gotF, "Keys: []")
	}

	_, _ = api.ExecuteCommand(ctx, []string{"SET", "k1", "aaa"})
	gotF2, errF2 := api.ExecuteCommand(ctx, []string{"FIND", "aaa"})
	if errF2 != nil {
		t.Fatalf("FIND2 error: %v", errF2)
	}
	if gotF2 != "Keys: [k1]" {
		t.Errorf("Got=%q, want=%q", gotF2, "Keys: [k1]")
	}

	gotE, errE := api.ExecuteCommand(ctx, []string{"EXPIRE", "k1", "30"})
	if errE != nil {
		t.Fatalf("EXPIRE error: %v", errE)
	}
	if gotE != "OK" {
		t.Errorf("Got=%q, want=%q", gotE, "OK")
	}
}

func TestCommandAPI_UnknownQuit(t *testing.T) {
	api, ctx := helperCreateAPI()

	_, err := api.ExecuteCommand(ctx, []string{"FOO"})
	if err == nil {
		t.Error("Expected error for unknown command, got nil")
	}

	got, err2 := api.ExecuteCommand(ctx, []string{"QUIT"})
	if err2 != nil {
		t.Fatalf("QUIT error: %v", err2)
	}
	if got != "Bye!" {
		t.Errorf("Got=%q, want=%q", got, "Bye!")
	}
}
