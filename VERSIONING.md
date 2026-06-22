# Versioning — Generations

Voltra versions are grouped into **generations**. A generation is the most
significant part of the version: it groups a line of releases and resets the
numeric version back to `1.0.0`.

## Format

Release tags:

```
g<generation>.<major>.<minor>.<patch>
```

Examples: `g1.1.0.0`, `g1.1.2.0`, `g2.1.0.0`.

- **generation** — the release line. Bumped only for a major new era of the
  engine. Encoded as the most-significant component everywhere.
- **major.minor.patch** — ordinary semver *within* a generation, starting at
  `1.0.0`.

`Cargo.toml` holds the 3-part `major.minor.patch` (`version = "1.0.0"`); the
generation lives in the `voltra::GENERATION` constant (and
`voltra::GENERATION_CODENAME`). The git tag combines them.

## Current

| Generation | Codename | Numeric line | First tag |
|---|---|---|---|
| **1** | **Genesis** | `1.x.x` | `g1.1.0.0` |

Shown by `voltra --version` → `voltra v1.0.0 · Gen 1 (Genesis)`.

> Legacy `v2.0.x` tags predate this scheme and are treated as **generation 0**.
> The updater ranks generation first, so Gen 1 supersedes them despite the lower
> numeric version.

## How updates resolve

`voltra update` compares `(generation, major, minor, patch)` tuples. Generation
dominates, so the `1.0.0` reset at the start of a generation still updates users
on the previous line. Legacy `v`/bare tags parse as generation 0.

## Releasing

1. Bump `version` in `Cargo.toml` (within the generation), and `GENERATION` /
   `GENERATION_CODENAME` in `src/lib.rs` only when starting a new generation.
2. Tag `g<gen>.<major>.<minor>.<patch>` and push it. The release workflow builds
   all platforms + Docker and publishes binaries to `voltra-releases`.

Scaffolded projects pin the engine to `g<gen>.<version>` automatically (derived
from `GENERATION` + `CARGO_PKG_VERSION`), so the tag they reference always
exists.
