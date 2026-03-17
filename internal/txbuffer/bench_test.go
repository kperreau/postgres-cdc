package txbuffer

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

func BenchmarkBufferSmallTx(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("bench_txbuf", reg)
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 64}, func(_ *model.TxBatch) error {
		return nil
	}, zerolog.Nop(), &m.PG, &m.CDC)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id", IsKey: true}}}
	_ = buf.OnRelation(rel)

	row := []model.ColumnValue{{Name: "id", Value: "1"}}
	now := time.Now()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.OnBegin(uint32(i), pglogrepl.LSN(i*0x100), now)
		_ = buf.OnInsert(1, row, pglogrepl.LSN(i*0x100+1))
		_ = buf.OnCommit(pglogrepl.LSN(i*0x100+0x10), now)
	}
}

func BenchmarkBufferLargeTx(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("bench_txbuf_lg", reg)
	buf := New(Config{MaxTxBytes: 1 << 30, InitialEventCap: 256}, func(_ *model.TxBatch) error {
		return nil
	}, zerolog.Nop(), &m.PG, &m.CDC)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "events", Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "data"}}}
	_ = buf.OnRelation(rel)

	row := []model.ColumnValue{
		{Name: "id", Value: "1"},
		{Name: "data", Value: "some payload data for benchmarking purposes"},
	}
	now := time.Now()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.OnBegin(uint32(i), pglogrepl.LSN(i*0x10000), now)
		for j := 0; j < 100; j++ {
			_ = buf.OnInsert(1, row, pglogrepl.LSN(i*0x10000+j+1))
		}
		_ = buf.OnCommit(pglogrepl.LSN(i*0x10000+0x1000), now)
	}
}
