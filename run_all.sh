#!/usr/bin/env bash
set -euo pipefail

EMULATOR_IMAGE="${EMULATOR_IMAGE:-gcr.io/cloud-spanner-emulator/emulator:1.5.50}"
EMULATOR_HOST="localhost:9010"
CONTAINER_NAME="spanner-emu-test"

# Build first.
echo "Building..."
go build -o repro . || exit 1

# Collect results.
results=()

run_test() {
  local rw_env="$1" insert="$2" delete="$3"
  local label="RW=${rw_env:-unset} insert=${insert} delete=${delete}"

  # Restart emulator.
  docker rm -f "$CONTAINER_NAME" &>/dev/null || true
  docker run -d --rm -p 9010:9010 -p 9020:9020 --name "$CONTAINER_NAME" "$EMULATOR_IMAGE" &>/dev/null
  sleep 2

  # Build env.
  local -a env=(SPANNER_EMULATOR_HOST="$EMULATOR_HOST")
  if [[ -n "$rw_env" ]]; then
    env+=(GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW="$rw_env")
  fi

  # Run.
  if env "${env[@]}" ./repro -insert="$insert" -delete="$delete" 2>&1 | tail -1 | grep -q "^PASS"; then
    results+=("PASS  $label")
  else
    results+=("BUG   $label")
  fi
}

echo "Running tests with ${EMULATOR_IMAGE}..."
echo ""

# RW env variants × insert variants × delete variants
for rw_env in "true" "false" ""; do
  for insert in "rw" "stmt"; do
    for delete in "stmt-mutation" "apply" "stmt-dml"; do
      run_test "$rw_env" "$insert" "$delete"
    done
  done
done

# Cleanup.
docker rm -f "$CONTAINER_NAME" &>/dev/null || true

# Print results.
echo ""
echo "============================== Results =============================="
printf "%-6s %-10s %-10s %-15s\n" "Result" "RW env" "INSERT" "DELETE"
echo "---------------------------------------------------------------------"
for r in "${results[@]}"; do
  result="${r%% *}"
  rest="${r#* }"
  # Parse "RW=xxx insert=yyy delete=zzz"
  rw_val=$(echo "$rest" | sed 's/.*RW=\([^ ]*\).*/\1/')
  ins_val=$(echo "$rest" | sed 's/.*insert=\([^ ]*\).*/\1/')
  del_val=$(echo "$rest" | sed 's/.*delete=\([^ ]*\).*/\1/')
  printf "%-6s %-10s %-10s %-15s\n" "$result" "$rw_val" "$ins_val" "$del_val"
done
