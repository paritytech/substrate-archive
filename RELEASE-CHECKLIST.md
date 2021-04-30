# Releasing `substrate-archive`

Here's how to make a new release of `substrate-archive` to crates.io.

## Checklist

1. Make a new branch, name it whatever you want, but `vx.y.z-prep` is a good template
1. Update `Cargo.toml` in `substrate-archive` and `substrate-archive-backend` with the new version number.
1. Update all references to `substrate` crates and `polkadot` to their respective latest releases.
1. Update the `CHANGELOG` as specified [here](https://keepachangelog.com/en/1.0.0/).
1. Run all tests: `cargo test --all`.
1. Push the PR.
1. Once reviewed, merge it to master.
1. Tag master with `git tag vx.y.z` and push the tags with `git push --tags`
1. Build a binary for debian, compatible with the current glibc version (`v1.2.3`)
    1. TODO: step 1
    1. TODO: step 2
    1. TODO: step n
1. Signal the new release to devop for deployment
