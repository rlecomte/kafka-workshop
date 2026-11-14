#!/usr/bin/env bash
# Lance un cluster local de brokers coordonnés par etcd.
#
#   scripts/cluster.sh start [N]   lance N brokers (3 par défaut), ports 9092, 9093...
#   scripts/cluster.sh stop        arrête tous les brokers
#   scripts/cluster.sh kill ID     arrête brutalement le broker ID (kill -9)
#   scripts/cluster.sh restart ID  relance le broker ID
#   scripts/cluster.sh logs ID     suit les logs du broker ID
#
# Les données sont dans cluster/data-<id>, les logs dans cluster/broker-<id>.log.
# Prérequis : docker compose up -d etcd
set -euo pipefail
cd "$(dirname "$0")/.."

DIR=cluster
BIN=target/debug/broker
mkdir -p "$DIR"

start_one() {
    local id=$1
    BROKER_ETCD_URL=${BROKER_ETCD_URL:-http://localhost:2379} \
    BROKER_ETCD_PREFIX=${BROKER_ETCD_PREFIX:-/workshop} \
    BROKER_NODE_ID=$id \
    BROKER_PORT=$((9092 + id)) \
    BROKER_DATA_DIR="$DIR/data-$id" \
    RUST_LOG=${RUST_LOG:-info} \
        nohup "$BIN" > "$DIR/broker-$id.log" 2>&1 &
    echo $! > "$DIR/broker-$id.pid"
    echo "broker $id démarré sur le port $((9092 + id)) (pid $!)"
}

stop_one() {
    local id=$1 signal=${2:-TERM}
    if [[ -f "$DIR/broker-$id.pid" ]]; then
        kill "-$signal" "$(cat "$DIR/broker-$id.pid")" 2>/dev/null || true
        rm -f "$DIR/broker-$id.pid"
        echo "broker $id arrêté ($signal)"
    fi
}

case "${1:-}" in
    start)
        cargo build --quiet
        for ((id = 0; id < ${2:-3}; id++)); do start_one "$id"; done
        ;;
    stop)
        for pid in "$DIR"/broker-*.pid; do
            [[ -e "$pid" ]] || continue
            id=${pid#"$DIR"/broker-}; stop_one "${id%.pid}"
        done
        ;;
    kill) stop_one "$2" KILL ;;
    restart) start_one "$2" ;;
    logs) tail -f "$DIR/broker-$2.log" ;;
    *) sed -n '2,10p' "$0"; exit 1 ;;
esac
