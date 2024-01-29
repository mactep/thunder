package migration

import (
	"context"
	"fmt"
	"log"
	"os"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"ariga.io/atlas/sql/sqltool"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql/schema"
	_ "github.com/lib/pq"
)

// GenerateMigration generates a migration file using ent and atlas under the hood.
// It uses embedded postgres to create a local postgres server to generate the migration.
// This can be used as a cmd tool.
// cmd.go:
//
//	func main() {
//	  migration.GenerateMigration(entGeneratedMigrate)
//	}
//
// $ go run cmd.go <migration-name>
func GenerateMigration(ent EntMigrator) {
	// Declare variables
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatal("migration name is required")
	}

	migrationName := os.Args[1]

	// Create embedded postgres server
	dbName := "postgres-migration"
	dbUser := "service-migrator"
	dbPassword := "password"
	var dbPort uint32 = 5432

	dbURL := fmt.Sprintf("postgresql://%s:%s@localhost:%d/%s?sslmode=disable", dbUser, dbPassword, dbPort, dbName)
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Password(dbPassword).
		Username(dbUser).
		Database(dbName).
		Version(embeddedpostgres.V15).
		Port(dbPort),
	)
	err := postgres.Start()
	if err != nil {
		log.Fatalf("failed to start postgres: %v", err)
	}
	defer postgres.Stop()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Panic occurred:", r)
			postgres.Stop()
		}
	}()

	// Create a local migration directory able to understand golang-migrate migration files for replay.
	dir, err := sqltool.NewGolangMigrateDir("migrations")
	if err != nil {
		log.Fatalf("failed creating atlas migration directory: %v", err)
	}
	// Write migration diff.
	opts := []schema.MigrateOption{
		schema.WithFormatter(sqltool.GolangMigrateFormatter), // provide formatter
		schema.WithMigrationMode(schema.ModeReplay),          // provide migration mode
		schema.WithDialect(dialect.Postgres),                 // Ent dialect to use
		schema.WithDropColumn(true),                          // drop column if it doesn't exist
		schema.WithDropIndex(true),                           // drop index if it doesn't exist
		schema.WithDir(dir),                                  // provide migration directory
	}

	// Generate migrations using Atlas support.
	err = ent.NamedDiff(ctx, dbURL, migrationName, opts...)
	if err != nil {
		log.Fatalf("failed generating migration file: %v", err)
	}
}
