# bash run.sh

export RUST_LOG=warn,mysticeti_core::consensus=trace,mysticeti_core::net_sync=DEBUG,mysticeti_core::core=DEBUG

tmux kill-server || true

tmux new -d -s "v0" "cargo run --bin mysticeti -- dry-run --committee-size 4 --authority 0 > v0.log.ansi" 
tmux new -d -s "v1" "cargo run --bin mysticeti -- dry-run --committee-size 4 --authority 1 > v1.log.ansi" 
tmux new -d -s "v2" "cargo run --bin mysticeti -- dry-run --committee-size 4 --authority 2 > v2.log.ansi" 
tmux new -d -s "v3" "cargo run --bin mysticeti -- dry-run --committee-size 4 --authority 3 > v3.log.ansi"

sleep 5
tmux kill-server
