// TODO: other sections of readme

# Cross-compilation

Setup:
 * rustup target add x86_64-unknown-linux-gnu
 * brew install SergioBenitez/osxct/x86_64-unknown-linux-gnu

Build:
 * TARGET_CC=x86_64-unknown-linux-gnu cargo build --release --target x86_64-unknown-linux-gnu -p mysticeti-main
