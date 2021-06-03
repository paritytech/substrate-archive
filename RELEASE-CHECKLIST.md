# Releasing `substrate-archive`

Here's how to make a new release of `substrate-archive`.

## Checklist

1. Make a new branch, name it whatever you want, but `vx.y.z-prep` is a good template
2. Update `Cargo.toml` in `substrate-archive` and `substrate-archive-backend` with the new version number.
   2a. Update version in `Cargo.toml` for `polkadot-archive` with new version number. 
3. Update all references to `substrate` crates and `polkadot` to their respective latest releases.
4. Update the `CHANGELOG` for `substrate-archive` and `polkadot-archive` as specified [here](https://keepachangelog.com/en/1.0.0/).
5. Run all tests: `TEST_DATABASE_URL="postgres://localhost:5432/archive cargo test --all`.
6. Push the PR against the `release` branch.
7. Once reviewed, merge it to `release`. Upon merging, Github Actions will create a draft release and upload
binaries.
8. Tag the `release` branch with `git tag vx.y.z` and push the tags with `git push --tags`.
9. Review the draft release in the github UI.
10. Get a review of the draft release from the team.
11. Publish the release from github UI.
12. Signal to devops that there is a new release available.
