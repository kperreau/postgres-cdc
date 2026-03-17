package topic

import (
	"testing"

	"github.com/kperreau/postgres-cdc/internal/model"
)

func TestResolvePerTable(t *testing.T) {
	r, err := NewResolver(model.TopicPerTable, "cdc", "")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		db, schema, table string
		want              string
	}{
		{"app", "public", "users", "cdc.app.public.users"},
		{"app", "public", "orders", "cdc.app.public.orders"},
		{"analytics", "events", "page_views", "cdc.analytics.events.page_views"},
	}
	for _, tt := range tests {
		got := r.Resolve(tt.db, tt.schema, tt.table)
		if got != tt.want {
			t.Errorf("Resolve(%q,%q,%q) = %q, want %q", tt.db, tt.schema, tt.table, got, tt.want)
		}
	}
}

func TestResolveSingleTopic(t *testing.T) {
	r, err := NewResolver(model.TopicSingle, "", "cdc-all-events")
	if err != nil {
		t.Fatal(err)
	}
	got := r.Resolve("app", "public", "users")
	if got != "cdc-all-events" {
		t.Errorf("got %q, want cdc-all-events", got)
	}
}

func TestNewResolverValidation(t *testing.T) {
	_, err := NewResolver(model.TopicPerTable, "", "")
	if err == nil {
		t.Fatal("expected error for per_table with empty prefix")
	}

	_, err = NewResolver(model.TopicSingle, "", "")
	if err == nil {
		t.Fatal("expected error for single with empty topic name")
	}

	_, err = NewResolver("bogus", "", "")
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

func BenchmarkResolvePerTable(b *testing.B) {
	r, _ := NewResolver(model.TopicPerTable, "cdc", "")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve("app", "public", "users")
	}
}
