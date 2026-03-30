package producer

import (
	"errors"
	"strings"
	"testing"

	"github.com/cenkalti/backoff/v5"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestChunkRecords(t *testing.T) {
	t.Parallel()

	records := []*kgo.Record{
		{Topic: "cdc.app.public.a"},
		{Topic: "cdc.app.public.b"},
		{Topic: "cdc.app.public.c"},
		{Topic: "cdc.app.public.d"},
		{Topic: "cdc.app.public.e"},
	}

	chunks := chunkRecords(records, 2)
	if len(chunks) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(chunks))
	}
	if len(chunks[0]) != 2 || len(chunks[1]) != 2 || len(chunks[2]) != 1 {
		t.Fatalf("chunk sizes = [%d %d %d], want [2 2 1]", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}
	if chunks[0][0] != records[0] || chunks[2][0] != records[4] {
		t.Fatal("chunking should preserve record order")
	}
}

func TestSummarizeBatch(t *testing.T) {
	t.Parallel()

	summary := summarizeBatch([]*kgo.Record{
		{Topic: "cdc.app.public.small", Key: []byte("a"), Value: []byte("bc")},
		{Topic: "cdc.app.public.large", Key: []byte("abcd"), Value: []byte("efghij")},
	})

	if summary.Count != 2 {
		t.Fatalf("Count = %d, want 2", summary.Count)
	}
	if summary.TotalPayloadBytes != 13 {
		t.Fatalf("TotalPayloadBytes = %d, want 13", summary.TotalPayloadBytes)
	}
	if summary.MaxPayloadBytes != 10 {
		t.Fatalf("MaxPayloadBytes = %d, want 10", summary.MaxPayloadBytes)
	}
	if summary.MaxPayloadTopic != "cdc.app.public.large" {
		t.Fatalf("MaxPayloadTopic = %q, want cdc.app.public.large", summary.MaxPayloadTopic)
	}
}

func TestPermanentKafkaProduceErrIncludesBatchDetails(t *testing.T) {
	t.Parallel()

	cfg := Config{
		BatchMaxBytes:   16 * 1024 * 1024,
		BatchMaxRecords: 1000,
	}
	records := []*kgo.Record{
		{Topic: "cdc.app.public.small", Key: []byte("1"), Value: []byte("12")},
		{Topic: "cdc.app.public.tooling_ai_requests", Key: []byte("1234"), Value: []byte("123456")},
	}

	err := permanentKafkaProduceErr(kerr.MessageTooLarge, records, cfg)
	if err == nil {
		t.Fatal("expected permanent error")
	}
	var permanent *backoff.PermanentError
	if !errors.As(err, &permanent) {
		t.Fatal("expected permanent error wrapper")
	}

	msg := err.Error()
	if !strings.Contains(msg, `largest record key+value payload=10 bytes on topic "cdc.app.public.tooling_ai_requests"`) {
		t.Fatalf("message missing largest record details: %s", msg)
	}
	if !strings.Contains(msg, "configured tuning.producer_batch_max_bytes=16777216") {
		t.Fatalf("message missing configured batch max bytes: %s", msg)
	}
	if !strings.Contains(msg, "batch payload=13 bytes across 2 records") {
		t.Fatalf("message missing batch summary: %s", msg)
	}
}

func TestPermanentKafkaProduceErrIncludesRecordLimitDetails(t *testing.T) {
	t.Parallel()

	cfg := Config{
		BatchMaxBytes:   1024,
		BatchMaxRecords: 2,
	}
	records := []*kgo.Record{
		{Topic: "cdc.app.public.a", Key: []byte("123"), Value: []byte("4567")},
		{Topic: "cdc.app.public.b", Key: []byte("12"), Value: []byte("34")},
		{Topic: "cdc.app.public.c", Key: []byte("1"), Value: []byte("2")},
	}

	err := permanentKafkaProduceErr(kerr.RecordListTooLarge, records, cfg)
	if err == nil {
		t.Fatal("expected permanent error")
	}

	msg := err.Error()
	if !strings.Contains(msg, "configured tuning.producer_batch_max_bytes=1024 and tuning.producer_batch_max_records=2") {
		t.Fatalf("message missing config details: %s", msg)
	}
	if !strings.Contains(msg, "batch payload=13 bytes across 3 records") {
		t.Fatalf("message missing batch summary: %s", msg)
	}
}

func TestPermanentKafkaProduceErrIgnoresNonKafkaErrors(t *testing.T) {
	t.Parallel()

	err := permanentKafkaProduceErr(errors.New("boom"), nil, Config{})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
