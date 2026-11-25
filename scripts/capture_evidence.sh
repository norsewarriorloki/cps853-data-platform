#!/usr/bin/env bash
# Run pipeline stages in order and retain their console output for the report.
set -euo pipefail

usage() {
    cat <<'HELP'
Usage: bash scripts/capture_evidence.sh [--clean] [--with-tests]

  --clean       Run make clean first: DELETE generated data, database contents,
                Kafka messages, and checkpoints. Raw input and evidence remain.
  --with-tests  Also capture batch validation, streaming quarantine, rolling-window,
                and consumer sink tests after the pipeline completes.
  --help        Show this help.

Output is displayed live and saved in documentation/evidence/run-<unique-id>/.
Stops at the first failed stage. Existing evidence is never overwritten.
Without --clean, existing pipeline state is retained; replay adds Kafka messages.
HELP
}

clean_first=false
with_tests=false
for argument in "$@"; do
    case "$argument" in
        --clean) clean_first=true ;;
        --with-tests) with_tests=true ;;
        --help|-h) usage; exit 0 ;;
        *) printf 'Unknown option: %s\n' "$argument" >&2; usage >&2; exit 2 ;;
    esac
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."
for tool in make tee mktemp; do
    command -v "$tool" >/dev/null || { printf 'Required command missing: %s\n' "$tool" >&2; exit 1; }
done

mkdir -p documentation/evidence
evidence_dir=$(mktemp -d "$PWD/documentation/evidence/run-XXXXXX")
printf '[evidence] Saving this run to %s\n' "$evidence_dir"

capture() {
    local target=$1
    local filename=$2
    local output_file="$evidence_dir/$filename"
    local status
    printf '\n[evidence] Running make %s -> %s\n' "$target" "$filename"
    if make "$target" 2>&1 | tee "$output_file"; then
        return 0
    else
        status=$?
        printf '[evidence] FAILED: make %s (exit %s). Stopping; partial output: %s\n' \
            "$target" "$status" "$output_file" >&2
        exit "$status"
    fi
}

if "$clean_first"; then
    capture clean 00-clean.txt
fi

capture silver 01-silver.txt
capture dimensions 02-dimensions.txt
capture gold 03-gold.txt
capture produce 04-producer.txt
capture consume 05-consumer.txt
capture answers 06-answers.txt

if "$with_tests"; then
    capture test-validation 07-batch-tests.txt
    capture test-invalid 08-stream-tests.txt
    capture test-rolling 09-rolling-tests.txt
    capture test-consumer-sinks 10-sink-tests.txt
fi

printf '\n[evidence] All requested stages succeeded. Output: %s\n' "$evidence_dir"
printf '[evidence] Include the numbered .txt files in LaTeX using \\lstinputlisting.\n'
