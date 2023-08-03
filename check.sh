#!/bin/bash

RED='\033[1;31m'
GREEN='\033[1;32m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Running cargo test...${NC}"
cargo test || { echo -e "${RED}cargo test failed${NC}"; exit 1; }
echo -e "${BLUE}Running simulator tests...${NC}"
cargo test -p mysticeti-core --features simulator test_network_sync_sim || { echo -e "${RED}Simulator test failed${NC}"; exit 1; }
cargo test -p mysticeti-core --features simulator epoch || { echo -e "${RED}Simulator Epoch test failed${NC}"; exit 1; }
echo -e "${BLUE}Running cargo fmt --check...${NC}"
cargo fmt --check || { echo -e "${RED}Formatting failed${NC}"; exit 1; }
echo -e "${BLUE}Running clippy...${NC}"
cargo clippy -- -D warnings -A clippy::len-without-is-empty -A clippy::result-unit-err || { echo -e "${RED}Clippy failed${NC}"; exit 1; }
echo -e "${GREEN}All checks passed!${NC}"
