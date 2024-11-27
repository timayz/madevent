dev:
	cargo watch -x 'run -- --log debug serve'

lint:
	cargo clippy --fix --all-features -- -D warnings

test:
	cargo test

fmt:
	cargo fmt -- --emit files

deny:
	cargo deny check

machete:
	cargo machete

advisory.clean:
	rm -rf ~/.cargo/advisory-db

pants:
	cargo pants

audit:
	cargo audit

