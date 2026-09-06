# Public release readiness

Assessment updated 2026-09-06. Intended positioning: experimental open-source
infrastructure software with a reproducible correctness demo.

## Prepared

- MIT licensing under elei.io, package metadata, and vendored DuckLake notice.
- README with setup, support matrix, operating boundaries, and design links.
- Contributor guide, security reporting guidance, and architecture rationale.
- CI for unit tests, lint, formatting, package builds, secret scanning, and the
  disposable PostgreSQL/filesystem demo. Actions and scanner use immutable pins.
- Demo verifies exact preservation of 4,000 rows and reduction of 40 files to 1.
- Compose respects the image's non-root identity. The demo initializes ownership
  of its dedicated named volume explicitly.
- Mutating CLI commands require explicit lake scope or `MAINTAIN_ALL_LAKES=true`.
- Orphan cleanup is disabled by default, including one-shot commands. Existing
  installations must set `ORPHAN_CLEANUP_ENABLED=true` to retain that behavior.
- Native errors redact configured secrets and URL userinfo before truncation;
  credential fields are excluded from configuration representations.
- Stress harness requires `SOAK_METADATA_SCHEMA` instead of a personal catalog
  identifier. Historical reports are clearly distinguished from current checks.

## Verification

- 183 unit tests passed locally; lint and formatting passed.
- Wheel and source distribution built successfully.
- pip-audit found no known vulnerabilities in the locked runtime Python
  dependencies for this platform. This excludes native libraries and images.
- Workflow syntax checked with actionlint; demo Compose configuration validated.
- Gitleaks 8.30.1 found no findings in all 18 original commits or the working tree.
  This is a tool result, not proof that every possible sensitive value is absent.
- A clean native image build passed on Linux/ARM64. The documented demo command
  then rebuilt the final application layer from the public checkout and passed:
  40 active files became 1, with exact preservation of 4,000 rows. The container's
  non-root UID, MIT metadata, copyright notice, and default-disabled orphan
  cleanup were also verified. See [recorded result](../examples/demo/result.json).

## Before publication

- Review the prepared public checkout, which replaces the personal author and
  committer email with the existing GitHub no-reply identity. Keep the original
  repository private as a backup. Rewritten commits have different IDs.
- Run the committed workflows on GitHub; local validation cannot verify hosted
  runner behavior or repository settings.
- Enable private vulnerability reporting and select the final public repository
  destination. Publishing or force-pushing is a separate deliberate action.
- Complete a dependency/license inventory before publishing binary images.
- Validate whether an available signed DuckLake release for DuckDB 1.5.5 includes
  the external Hive-path fix before removing the patch or unsigned exception.

## Follow-up roadmap

1. Extend integration tests to concurrent updates/deletes, schema evolution,
   storage faults, crash recovery, and retention/deletion policy boundaries.
2. Publish reproducible soak artifacts with tested commit, versions, hardware,
   and commands; old `/tmp` paths are historical references, not available evidence.
3. Validate least-privilege database and storage permission recipes.
4. Add a compact metrics dashboard and a recorded walkthrough for the portfolio.

No production-readiness or performance claim follows from the small demo.
