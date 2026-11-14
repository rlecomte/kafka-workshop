#!/usr/bin/env bash
# Construit la chaîne de branches chapitre/chNN et solution/chNN.
set -euo pipefail
#
# Les sources sont les branches solution/chNN (et chapitre/ch00, écrit à la
# main) : modifiez-les, puis relancez ce script pour régénérer toute la chaîne
# (les squelettes chapitre/chNN sont produits par strip.py). Les énoncés, le
# README, docker-compose.yml et scripts/ viennent de solution/ch08.
#
# Usage : outils/build-chain.sh   (depuis n'importe quel worktree du dépôt)
TOOLS=$(cd "$(dirname "$0")" && pwd)
REPO=$(git rev-parse --show-toplevel)
B=$(mktemp -d)/build
CO="Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"

cd "$REPO"
# Les sources, figées avant de réécrire les branches.
declare -A SRC
SRC[ch00]=$(git rev-parse chapitre/ch00)
for n in 00 01 02 03 04 05 06 07 08; do SRC[$n]=$(git rev-parse solution/ch$n); done
DOCS=${SRC[08]}

git worktree add -q --detach "$B" HEAD
trap 'cd "$REPO"; git worktree remove --force "$B"; git branch -D build-chain >/dev/null 2>&1 || true' EXIT
cd "$B"
git checkout -q --orphan build-chain
git rm -rfq . >/dev/null

# step <branche> <ref source> <message> [chapitre spec...]
step() {
    local branch=$1 source=$2 message=$3
    shift 3
    git rm -rfq . >/dev/null 2>&1 || true
    git checkout -q "$source" -- .
    git checkout -q "$DOCS" -- README.md chapters docker-compose.yml scripts .gitignore
    if [[ $# -gt 0 ]]; then
        python3 "$TOOLS/strip.py" "$B" "$@"
    fi
    git add -A
    git commit -q -m "$message" -m "$CO"
    git branch -f "$branch" HEAD
    echo "$branch -> $(git rev-parse --short HEAD)"
}

step chapitre/ch00 ${SRC[ch00]} "Chapitre 0 : point de départ"
step solution/ch00 ${SRC[00]} "Chapitre 0 : solution"

step chapitre/ch01 ${SRC[01]} "Chapitre 1 : point de départ" 1 \
    src/topics.rs:create,get,get_by_id,list,validate_name \
    src/handlers/metadata.rs:handle_metadata,lookup_topic,describe_topic \
    src/handlers/create_topics.rs:handle_create_topics,create_topic \
    src/handlers/describe_configs.rs:handle_describe_configs,describe_resource \
    src/apis.rs:@apis src/lib.rs:@lints
step solution/ch01 ${SRC[01]} "Chapitre 1 : solution"

step chapitre/ch02 ${SRC[02]} "Chapitre 2 : point de départ" 2 \
    src/record_batch.rs:parse,compute_crc,set_base_offset,parse_batches \
    src/log/segment.rs:create,append \
    src/log/mod.rs:open,append \
    src/replica.rs:create_replicas \
    src/lib.rs:create_topic \
    src/handlers/produce.rs:handle_produce,produce_topic,produce_partition \
    src/handlers/init_producer_id.rs:handle_init_producer_id \
    src/apis.rs:@apis src/lib.rs:@lints
step solution/ch02 ${SRC[02]} "Chapitre 2 : solution"

step chapitre/ch03 ${SRC[03]} "Chapitre 3 : point de départ" 3 \
    src/log/segment.rs:read_header,read \
    src/log/mod.rs:read,offset_for_timestamp \
    src/replica.rs:append,read,high_watermark,offset_for_timestamp \
    src/handlers/fetch.rs:handle_fetch,read_fetch,fetch_partition \
    src/handlers/list_offsets.rs:handle_list_offsets,list_offset \
    src/apis.rs:@apis src/lib.rs:@lints
step solution/ch03 ${SRC[03]} "Chapitre 3 : solution"

step chapitre/ch04 ${SRC[04]} "Chapitre 4 : point de départ" 4 \
    src/log/index.rs:append,lookup,truncate_to_position \
    src/log/segment.rs:create,open,recover,read_valid_batch,track,append,read,delete \
    src/log/mod.rs:open,append_batch,roll,apply_retention \
    src/replica.rs:open_replica,load_existing,apply_retention \
    src/lib.rs:new \
    src/lib.rs:@lints
step solution/ch04 ${SRC[04]} "Chapitre 4 : solution"

step chapitre/ch05 ${SRC[05]} "Chapitre 5 : point de départ" 5 \
    src/groups/group.rs:join,prepare_rebalance,maybe_complete_join,complete_join,select_protocol,sync,heartbeat,leave,remove_member,check_commit,tick \
    src/groups/mod.rs:tick,apply_commits \
    src/groups/offsets.rs:partition_for \
    src/handlers/find_coordinator.rs:handle_find_coordinator,find_coordinator \
    src/handlers/groups.rs:handle_join_group,handle_sync_group,handle_heartbeat,handle_leave_group \
    src/handlers/offsets.rs:handle_offset_commit,store_commits,handle_offset_fetch,fetch_offsets \
    src/lib.rs:ensure_offsets_topic,load_offsets \
    src/replica.rs:read_all \
    src/apis.rs:@apis src/lib.rs:@lints
step solution/ch05 ${SRC[05]} "Chapitre 5 : solution"

step chapitre/ch06 ${SRC[06]} "Chapitre 6 : point de départ" 6 \
    src/cluster/mod.rs:apply,assign_replicas \
    src/cluster/node.rs:register,keep_alive_loop,join_cluster,watch_loop,controller_loop,is_controller,on_cluster_change,sync_offsets_partitions,create_topic_in_cluster \
    src/lib.rs:live_brokers,controller_id,leader_replica,coordinator_for,is_coordinator,create_topic,ensure_offsets_topic \
    src/handlers/metadata.rs:handle_metadata,lookup_topic,describe_topic \
    src/handlers/create_topics.rs:handle_create_topics \
    src/handlers/find_coordinator.rs:find_coordinator \
    src/lib.rs:@lints
step solution/ch06 ${SRC[06]} "Chapitre 6 : solution"

step chapitre/ch07 ${SRC[07]} "Chapitre 7 : point de départ" 7 \
    src/log/mod.rs:append_replicated,append_with_epoch,truncate_to,reset_to \
    src/log/segment.rs:truncate_to \
    src/replica.rs:append,append_as_follower,read,read_for_follower,update_partition_state,record_follower_fetch,update_high_watermark,isr_to_shrink,truncate_to,reset_to \
    src/replication/mod.rs:sync_fetchers,fetch_loop,fetch_from,shrink_isrs \
    src/cluster/node.rs:alter_isr \
    src/lib.rs:wait_for_high_watermark \
    src/handlers/fetch.rs:fetch_partition \
    src/handlers/produce.rs:handle_produce,produce_partition \
    src/handlers/offsets.rs:store_commits \
    src/lib.rs:@lints
step solution/ch07 ${SRC[07]} "Chapitre 7 : solution"

step chapitre/ch08 ${SRC[08]} "Chapitre 8 : point de départ" 8 \
    src/cluster/node.rs:elect,reconcile_partitions \
    src/replica.rs:update_partition_state,checkpoint_high_watermark,read_all \
    src/lib.rs:@lints
step solution/ch08 ${SRC[08]} "Chapitre 8 : solution"
