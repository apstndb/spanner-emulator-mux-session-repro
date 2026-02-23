// Reproduction for https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/282
//
// Multiplexed session RW transactions silently lose writes.
//
// Usage:
//   go run main.go -delete=<stmt-mutation|rw-mutation|apply|stmt-dml> -begin=<default|inlined|explicit>
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
	deleteMode = flag.String("delete", "stmt-mutation", "DELETE mode: stmt-mutation, rw-mutation, apply, or stmt-dml")
	beginMode  = flag.String("begin", "default", "BeginTransaction mode: default, inlined, or explicit")
	skipSetup  = flag.Bool("skip-setup", false, "skip instance/database creation")
)

func parseBeginOption() (spanner.BeginTransactionOption, error) {
	switch *beginMode {
	case "default":
		return spanner.DefaultBeginTransaction, nil
	case "inlined":
		return spanner.InlinedBeginTransaction, nil
	case "explicit":
		return spanner.ExplicitBeginTransaction, nil
	default:
		return 0, fmt.Errorf("unknown begin mode: %s", *beginMode)
	}
}

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
	beginOpt, err := parseBeginOption()
	if err != nil {
		return err
	}
	txnOpts := spanner.TransactionOptions{
		BeginTransactionOption: beginOpt,
	}

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

	// Step 1: INSERT via DML (fixed, not relevant to the bug).
	log.Println("INSERT: ReadWriteTransaction (DML)")
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: "INSERT INTO T (PK, Val) VALUES (1, 1)"})
		return err
	})
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Step 2: DELETE
	switch *deleteMode {
	case "stmt-mutation":
		log.Printf("DELETE: StmtBasedTransaction (BufferWrite, begin=%s)", *beginMode)
		err = execStmtMutation(ctx, client, txnOpts)
	case "rw-mutation":
		log.Printf("DELETE: ReadWriteTransaction (BufferWrite, begin=%s)", *beginMode)
		_, err = client.ReadWriteTransactionWithOptions(ctx,
			func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				return txn.BufferWrite([]*spanner.Mutation{
					spanner.Delete("T", spanner.Key{1}),
				})
			}, txnOpts)
	case "apply":
		log.Println("DELETE: client.Apply (begin option N/A)")
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Delete("T", spanner.Key{1}),
		})
	case "stmt-dml":
		log.Printf("DELETE: StmtBasedTransaction (DML, begin=%s)", *beginMode)
		err = execStmtDML(ctx, client, txnOpts, "DELETE FROM T WHERE PK = 1")
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

func execStmtDML(ctx context.Context, client *spanner.Client, opts spanner.TransactionOptions, sql string) error {
	txn, err := spanner.NewReadWriteStmtBasedTransactionWithOptions(ctx, client, opts)
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

func execStmtMutation(ctx context.Context, client *spanner.Client, opts spanner.TransactionOptions) error {
	txn, err := spanner.NewReadWriteStmtBasedTransactionWithOptions(ctx, client, opts)
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
