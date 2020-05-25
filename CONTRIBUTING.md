# Contributing to substrate-archive 

substrate-archive welcomes contribution from everyone in the form of suggestions, bug
reports, pull requests, and feedback. This document gives some guidance if you
are thinking of helping us.

Please reach out here in a GitHub issue if you have any questions.

## Submitting bug reports and feature requests

When reporting a bug or asking for help, please include enough details so that
the people helping you can reproduce the behavior you are seeing. For some tips
on how to approach this, read about how to produce a [Minimal, Complete, and
Verifiable example].

[Minimal, Complete, and Verifiable example]: https://stackoverflow.com/help/mcve

If there are any logs that occur as a result of your issue, be sure to include them in the bug report as well. Logs for substrate-archive
can be found in `~/.local/share/substrate_archive/archive.logs` on linux, `$HOME/Library/Application Support/archive.logs` on MacOS and `C:\Users\Alice\AppData\Local\archive.logs` on Windows. Try to include only relevant portions if you can.

## Versioning

As many crates in the rust ecosystem, substrate-archive follows [semantic versioning]. This means bumping PATCH version on bug fixes that don't break backwards compatibility, MINOR version on new features and MAJOR version otherwise (MAJOR.MINOR.PATCH). Versions < 1.0 are considered to have the format 0.MAJOR.MINOR, which means bumping MINOR version for all non-breaking changes.

Bumping versions should be done in a separate from regular code changes PR.

[semantic versioning]: https://semver.org/

## Releasing a new version

This part of the guidelines is for substrate-archive maintainers.

When making a new release make sure to follow these steps:
* Submit a PR with a version bump and list all major and breaking changes in the crate's changelog

## Conduct

We follow [Substrate Code of Conduct].

[Substrate Code of Conduct]: https://github.com/paritytech/substrate/blob/master/CODE_OF_CONDUCT.adoc

## Attribution

This guideline is adapted from [Serde's CONTRIBUTING guide].

[Serde's CONTRIBUTING guide]: https://github.com/serde-rs/serde/blob/master/CONTRIBUTING.md
