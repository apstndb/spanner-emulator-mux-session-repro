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
  local rw_env="$1" delete="$2" begin="$3"
  local label="RW=${rw_env:-unset} delete=${delete} begin=${begin}"

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
  local args=(-delete="$delete" -begin="$begin")
  if env "${env[@]}" ./repro "${args[@]}" 2>&1 | tail -1 | grep -q "^PASS"; then
    results+=("PASS  $label")
  else
    results+=("BUG   $label")
  fi
}

echo "Running tests with ${EMULATOR_IMAGE}..."
echo ""

for rw_env in "true" "false" ""; do
  for delete in "stmt-mutation" "rw-mutation" "apply" "stmt-dml"; do
    for begin in "default" "inlined" "explicit"; do
      # client.Apply ignores begin option, only run once with default.
      if [[ "$delete" == "apply" && "$begin" != "default" ]]; then
        continue
      fi
      run_test "$rw_env" "$delete" "$begin"
    done
  done
done

# Cleanup.
docker rm -f "$CONTAINER_NAME" &>/dev/null || true

# Print results.
echo ""
echo "================================= Results ================================="
printf "%-6s %-10s %-15s %-10s\n" "Result" "RW env" "DELETE" "begin"
echo "---------------------------------------------------------------------------"
for r in "${results[@]}"; do
  result="${r%% *}"
  rest="${r#* }"
  rw_val=$(echo "$rest" | sed 's/.*RW=\([^ ]*\).*/\1/')
  del_val=$(echo "$rest" | sed 's/.*delete=\([^ ]*\).*/\1/')
  begin_val=$(echo "$rest" | sed 's/.*begin=\([^ ]*\).*/\1/')
  printf "%-6s %-10s %-15s %-10s\n" "$result" "$rw_val" "$del_val" "$begin_val"
done
