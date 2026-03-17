package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kperreau/postgres-cdc/internal/model"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	// Defaults alone lack a postgres user; set one so validation passes.
	cfg.Postgres.User = "test"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid with user set: %v", err)
	}
}

func TestValidateRejectsEmptyUser(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing postgres user and dsn")
	}
}

func TestValidateRejectsInvalidTopicMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Postgres.User = "test"
	cfg.Topic.Mode = "invalid"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid topic mode")
	}
}

func TestValidateRejectsSingleModeWithoutTopicName(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Postgres.User = "test"
	cfg.Topic.Mode = model.TopicSingle
	cfg.Topic.SingleTopicName = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for single mode without topic name")
	}
}

func TestValidateAcceptsSingleModeWithTopicName(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Postgres.User = "test"
	cfg.Topic.Mode = model.TopicSingle
	cfg.Topic.SingleTopicName = "cdc-all"
	cfg.Topic.Prefix = "" // not required in single mode
	if err := cfg.Validate(); err != nil {
		t.Fatalf("single mode with topic name should be valid: %v", err)
	}
}

func TestLoadFromYAML(t *testing.T) {
	content := `
postgres:
  user: testuser
  password: secret
  dbname: testdb
replication:
  slot_name: my_slot
  publication_name: my_pub
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Postgres.User != "testuser" {
		t.Errorf("want user=testuser, got %s", cfg.Postgres.User)
	}
	if cfg.Replication.SlotName != "my_slot" {
		t.Errorf("want slot_name=my_slot, got %s", cfg.Replication.SlotName)
	}
}

func TestPostgresConnString(t *testing.T) {
	p := &PostgresConfig{DSN: "postgres://u:p@h/d"}
	if got := p.ConnString(); got != "postgres://u:p@h/d" {
		t.Errorf("DSN mode: got %s", got)
	}

	p2 := &PostgresConfig{Host: "db.local", Port: 5433, User: "app", Password: "pw", DBName: "mydb"}
	got := p2.ConnString()
	want := "host=db.local port=5433 user=app password=pw dbname=mydb"
	if got != want {
		t.Errorf("component mode:\n got: %s\nwant: %s", got, want)
	}
}
