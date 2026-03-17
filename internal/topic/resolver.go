// Package topic resolves Redpanda topic names from table metadata.
package topic

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kperreau/postgres-cdc/internal/model"
)

// Resolver maps (database, schema, table) tuples to Redpanda topic names.
type Resolver struct {
	mode            model.TopicMode
	prefix          string
	singleTopicName string
}

// NewResolver creates a Resolver from the given configuration.
func NewResolver(mode model.TopicMode, prefix, singleTopicName string) (*Resolver, error) {
	switch mode {
	case model.TopicPerTable:
		if prefix == "" {
			return nil, errors.New("topic: prefix is required for per_table mode")
		}
	case model.TopicSingle:
		if singleTopicName == "" {
			return nil, errors.New("topic: single_topic_name is required for single mode")
		}
	default:
		return nil, fmt.Errorf("topic: unknown mode %q", mode)
	}
	return &Resolver{
		mode:            mode,
		prefix:          prefix,
		singleTopicName: singleTopicName,
	}, nil
}

// Resolve returns the topic name for the given table coordinates.
// For per_table mode: {prefix}.{database}.{schema}.{table}
// For single mode: the configured single topic name.
//
// Resolve is allocation-free for single mode. For per_table mode it builds a
// dot-separated string. It is safe for concurrent use (read-only).
func (r *Resolver) Resolve(database, schema, table string) string {
	if r.mode == model.TopicSingle {
		return r.singleTopicName
	}
	// per_table: cdc.{database}.{schema}.{table}
	var b strings.Builder
	b.Grow(len(r.prefix) + 1 + len(database) + 1 + len(schema) + 1 + len(table))
	b.WriteString(r.prefix)
	b.WriteByte('.')
	b.WriteString(database)
	b.WriteByte('.')
	b.WriteString(schema)
	b.WriteByte('.')
	b.WriteString(table)
	return b.String()
}
