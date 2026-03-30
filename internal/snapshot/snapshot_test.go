package snapshot

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestBuildSnapshotRelationMarksPrimaryKeys(t *testing.T) {
	t.Parallel()

	rel := buildSnapshotRelation("public", "billing_credit_balances", []pgconn.FieldDescription{
		{Name: "organization_id", DataTypeOID: 2950},
		{Name: "subscription_offer_id", DataTypeOID: 2950},
		{Name: "balance", DataTypeOID: 20},
	}, []string{"organization_id", "subscription_offer_id"})

	if rel.Namespace != "public" || rel.Name != "billing_credit_balances" {
		t.Fatalf("unexpected relation identity: %+v", rel)
	}
	if len(rel.KeyCols) != 2 || rel.KeyCols[0] != 0 || rel.KeyCols[1] != 1 {
		t.Fatalf("unexpected key cols: %+v", rel.KeyCols)
	}
	if !rel.Columns[0].IsKey || !rel.Columns[1].IsKey || rel.Columns[2].IsKey {
		t.Fatalf("unexpected key flags: %+v", rel.Columns)
	}
	if rel.Columns[0].TypeOID != 2950 || rel.Columns[2].TypeOID != 20 {
		t.Fatalf("unexpected type OIDs: %+v", rel.Columns)
	}
}
