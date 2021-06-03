# Releasing `substrate-archive`

Here's how to make a new release of `substrate-archive`.

## Checklist

1. Make a new branch, name it whatever you want, but `vx.y.z-prep` is a good template
2. Update `Cargo.toml` in `substrate-archive` and `substrate-archive-backend` with the new version number.
   2a. Update version in `Cargo.toml` for `polkadot-archive` with new version number.
3. Update the `CHANGELOG` for `substrate-archive` and `polkadot-archive` as specified [here](https://keepachangelog.com/en/1.0.0/).
4. Make a PR against `substrate-archive` master with the version bumps
5. Once merged, or in a new branch that includes the changes from `vx.y.z-prep`:
	- Update all references to `substrate` and `polkadot` crates to their respective latest releases.
	  - `diener --update polkadot --tag ${latest_release_tag}`
	  - `diener --update substrate --branch polkadot-${latest_release_tag}`
	- Run all tests: `TEST_DATABASE_URL="postgres://localhost:5432/archive cargo test --all`.
6. Push the PR against the `release` branch.
   6a. Any merge conflicts should be resolved with `git checkout --ours -- .`
7. Once reviewed, merge it to `release`. Upon merging, Github Actions will create a draft release and upload
binaries.
8. Tag the `release` branch with `git tag vx.y.z` and push the tags with `git push --tags`.
9. Review the draft release in the github UI.
10. Get a review of the draft release from the team.
11. Publish the release from github UI.
12. Signal to devops that there is a new release available.
