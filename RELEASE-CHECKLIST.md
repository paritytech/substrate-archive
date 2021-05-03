# Releasing `substrate-archive`

Here's how to make a new release of `substrate-archive`.

## Checklist

1. Make a new branch, name it whatever you want, but `vx.y.z-prep` is a good template
1. Update `Cargo.toml` in `substrate-archive` and `substrate-archive-backend` with the new version number.
1. Update all references to `substrate` crates and `polkadot` to their respective latest releases.
1. Update the `CHANGELOG` as specified [here](https://keepachangelog.com/en/1.0.0/).
1. Run all tests: `TEST_DATABASE_URL="postgres://localhost:5432/archive cargo test --all`.
1. Push the PR against the `release` branch.
1. Once reviewed, merge it to `release`.
1. Tag the `release` branch` with `git tag vx.y.z` and push the tags with `git push --tags`
1. Build a binary for debian, compatible with the current glibc version (`v2.31`)
    1. `docker run --rm -it debian:jessie`
    1. install required dependencies `apt-get update && apt-get -y install git curl gcc clang`
    1. install rust from rustup.rs: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    1. reload the shell: `source $HOME/.cargo/env`
    1. build archive: `git clone https://github.com/paritytech/substrate-archive.git --branch release --single-branch && cd substrate-archive/bin/polkadot-archive && SKIP_WASM_BUILD=1 cargo build --release`
    1. keeping the container running, in a another terminal find the id of the docker container with `docker ps -a`, copy the binary to host with `docker cp $YOUR_CONTAINER_ID:/substrate-archive/bin/polkadot-archive/target/release/polkadot-archive .`
1. Signal the new release to devop for deployment
