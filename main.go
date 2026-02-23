// Reproduction for https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/282
//
// Multiplexed session RW transactions silently lose writes.
//
// Usage:
//   go run main.go -insert=<rw|stmt> -delete=<stmt-mutation|apply|stmt-dml>
//
// Prerequisites:
//   SPANNER_EMULATOR_HOST=localhost:9010
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

const db = "projects/test-project/instances/test-instance/databases/test-database"

var (
	insertMode = flag.String("insert", "rw", "INSERT mode: rw (ReadWriteTransaction) or stmt (StmtBasedTransaction)")
	deleteMode = flag.String("delete", "stmt-mutation", "DELETE mode: stmt-mutation, apply, or stmt-dml")
	skipSetup  = flag.Bool("skip-setup", false, "skip instance/database creation")
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		log.Fatal("SPANNER_EMULATOR_HOST is not set")
	}

	ctx := context.Background()

	if !*skipSetup {
		if err := setup(ctx); err != nil {
			log.Fatalf("Setup: %v", err)
		}
	}
	if err := reproduce(ctx); err != nil {
		log.Fatalf("FAIL: %v", err)
	}
	log.Println("PASS")
}

func setup(ctx context.Context) error {
	ic, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer ic.Close()

	iop, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/test-project",
		InstanceId: "test-instance",
		Instance: &instancepb.Instance{
			Config:      "projects/test-project/instanceConfigs/emulator-config",
			DisplayName: "test-instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		return err
	}
	if _, err := iop.Wait(ctx); err != nil {
		return err
	}

	dc, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer dc.Close()

	dop, err := dc.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/test-project/instances/test-instance",
		CreateStatement: "CREATE DATABASE `test-database`",
		ExtraStatements: []string{
			"CREATE TABLE T (PK INT64 NOT NULL, Val INT64) PRIMARY KEY(PK)",
		},
	})
	if err != nil {
		return err
	}
	_, err = dop.Wait(ctx)
	return err
}

func reproduce(ctx context.Context) error {
	client, err := spanner.NewClientWithConfig(ctx, db,
		spanner.ClientConfig{
			DisableNativeMetrics: true,
			SessionPoolConfig: spanner.SessionPoolConfig{
				MinOpened: 1,
				MaxOpened: 10,
			},
		},
		option.WithGRPCConnectionPool(1),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	// Step 1: INSERT
	switch *insertMode {
	case "rw":
		log.Println("INSERT: ReadWriteTransaction (DML)")
		_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, spanner.Statement{SQL: "INSERT INTO T (PK, Val) VALUES (1, 1)"})
			return err
		})
	case "stmt":
		log.Println("INSERT: StmtBasedTransaction (DML)")
		err = execStmtDML(ctx, client, "INSERT INTO T (PK, Val) VALUES (1, 1)")
	default:
		return fmt.Errorf("unknown insert mode: %s", *insertMode)
	}
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Step 2: DELETE
	switch *deleteMode {
	case "stmt-mutation":
		log.Println("DELETE: StmtBasedTransaction (BufferWrite Mutation)")
		err = execStmtMutation(ctx, client)
	case "apply":
		log.Println("DELETE: client.Apply (Mutation)")
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Delete("T", spanner.Key{1}),
		})
	case "stmt-dml":
		log.Println("DELETE: StmtBasedTransaction (DML)")
		err = execStmtDML(ctx, client, "DELETE FROM T WHERE PK = 1")
	default:
		return fmt.Errorf("unknown delete mode: %s", *deleteMode)
	}
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	// Step 3: Verify deletion.
	row, err := client.Single().ReadRow(ctx, "T", spanner.Key{1}, []string{"PK"})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("read: %w", err)
	}

	var pk int64
	if err := row.Column(0, &pk); err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	return fmt.Errorf("BUG: row PK=%d still exists after DELETE succeeded without error", pk)
}

func execStmtDML(ctx context.Context, client *spanner.Client, sql string) error {
	txn, err := spanner.NewReadWriteStmtBasedTransaction(ctx, client)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	iter := txn.Query(ctx, spanner.Statement{SQL: sql})
	if err := iter.Do(func(_ *spanner.Row) error { return nil }); err != nil {
		txn.Rollback(ctx)
		return fmt.Errorf("query: %w", err)
	}
	_, err = txn.CommitWithReturnResp(ctx)
	return err
}

func execStmtMutation(ctx context.Context, client *spanner.Client) error {
	txn, err := spanner.NewReadWriteStmtBasedTransaction(ctx, client)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.Delete("T", spanner.Key{1}),
	}); err != nil {
		txn.Rollback(ctx)
		return fmt.Errorf("buffer write: %w", err)
	}
	_, err = txn.CommitWithReturnResp(ctx)
	return err
}
