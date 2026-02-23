@sakthivelmanii

The issue I reported in https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/282#issuecomment-3659846637 is still reproducible on emulator **v1.5.50** (which claims to have fixed this issue). The root cause appears to be different from the cross-database `TransactionID` collision that was fixed in v1.5.50.

**Reproduction repository:** https://github.com/apstndb/spanner-emulator-mux-session-repro

### Environment

- Emulator: **v1.5.50** (also reproduced on latest)
- Go client: **cloud.google.com/go/spanner v1.87.0** (multiplexed sessions for RW enabled by default, controllable via `GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW`)

### Reproduction conditions

The bug reproduces when **all three** of the following conditions are met:

1. **Multiplexed sessions for RW** are enabled
2. The transaction uses **explicit `BeginTransaction` RPC** (not inlined with the first statement)
3. The commit contains **mutations** (DML-only commits are not affected)

Note: I have not tested whether mutations are also lost when DML and mutations coexist in the same explicit-begin transaction.

This explains why my original report (using [spanner-mycli](https://github.com/apstndb/spanner-mycli)) was affected: `spanner-mycli` uses `NewReadWriteStmtBasedTransaction`, which defaults to explicit `BeginTransaction`. When a user runs a mutation-only command (e.g., `MUTATE ... DELETE`), this path is taken.

### Test matrix

Full results: [run_all_output.txt](https://github.com/apstndb/spanner-emulator-mux-session-repro/blob/main/run_all_output.txt)

BUG cases only (all with `MULTIPLEXED_SESSIONS_FOR_RW` enabled):

| DELETE method | `BeginTransactionOption` | Result |
|---|---|---|
| `StmtBasedTransaction` + `BufferWrite` | default (= explicit) | **BUG** |
| `StmtBasedTransaction` + `BufferWrite` | explicit | **BUG** |
| `StmtBasedTransaction` + `BufferWrite` | inlined | PASS |
| `ReadWriteTransaction` + `BufferWrite` | default (= inlined) | PASS |
| `ReadWriteTransaction` + `BufferWrite` | explicit | **BUG** |
| `ReadWriteTransaction` + `BufferWrite` | inlined | PASS |
| `client.Apply` | (inlined internally) | PASS |
| DML (any begin mode) | any | PASS |

### Key observations

- The **INSERT method does not matter**; only the DELETE (mutation commit) triggers the bug.
- `client.Apply` works correctly because it uses inlined `BeginTransaction` internally.
- DML-only transactions work regardless of begin mode.
- Setting `GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW=false` works around the issue entirely.

This suggests the emulator's multiplexed session transaction manager does not correctly handle mutations committed in a transaction that was started via an explicit `BeginTransaction` RPC.
