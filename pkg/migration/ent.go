package migration

import (
	"context"

	"entgo.io/ent/dialect/sql/schema"
)

type EntMigrator interface {
	NamedDiff(ctx context.Context, url, name string, opts ...schema.MigrateOption) error
}
