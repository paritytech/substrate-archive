# Releasing `substrate-archive`

Here's how to make a new release of `substrate-archive`.

## Checklist

1. Make a new branch, name it according to the template `release-vx.y.z` for consistency.
2. Update `Cargo.toml` in `substrate-archive` and `substrate-archive-backend` with the new version number.
	* 2a. Update version in `Cargo.toml` for `polkadot-archive` with new version number.
3. Update the `CHANGELOG` for `substrate-archive` and `polkadot-archive` as specified [here](https://keepachangelog.com/en/1.0.0/).
4. Make a PR against master with these changes
3. Once the PR to master is merged, we have to prepare the release branch.
	* 3a. Update all references to `substrate` and `polkadot` crates to their respective latest releases.
		- `diener update --polkadot --tag `${latest_tag}`
		- `diener update --substrate --branch `polkadot-${latest_tag}`
	* 3b. Run all tests: `TEST_DATABASE_URL="postgres://localhost:5432/archive cargo test --all`.
		- These tests will also be run under CI on branch push.
4. Tag the release branch with `git tag vx.y.z` and push the tags with `git push --tags`.
   - This will trigger the automated release CI which will build the binaries. It can take a bit for this to finish,
   (~1 hour), but once it's done it will create the draft release from the changelog, and upload the artifacts.
5. Review the draft release in the github UI.
6. Get a review of the draft release from the team.
7. Publish the release from github UI.
8. Signal to devops that there is a new release available.
