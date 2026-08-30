# Python Orthanc Tools

Python Orthanc Tools is a collection of Python library classes and command-line
tools for moving, synchronizing, forwarding, checking, and cleaning DICOM data
in Orthanc. It also contains HL7 worklist and report helpers, a RabbitMQ-backed
replicator, and a PostgreSQL backup tool.

This repository is a fork of `orthanc-team/python-orthanc-tools`. Keep changes
small and easy to review upstream. The tools often run unattended against
durable medical data, so a successful happy-path test is not enough evidence
for destructive or restart-sensitive behavior.

## What must stay true

### 1. A confirmed copy comes before source deletion

Several migrators and forwarders can delete source resources. Never mark a
transfer complete or delete its source until the destination has confirmed the
required data. Partial transfers, timeouts, and failed jobs must remain visible
and retryable.

### 2. Resume state never gets ahead of durable work

Orthanc change sequence ids, last-update values, status files, and RabbitMQ
messages let long-running tools recover after interruption. Advance persisted
state only after the corresponding side effect succeeds. Write state so a
crash cannot turn a valid checkpoint into a truncated or misleading one.

### 3. DICOM and Orthanc identities are not interchangeable

Keep Orthanc resource ids, DICOM Study/Series/SOP Instance UIDs, accession
numbers, and labels distinct. Use the identifier required by the receiving API
and preserve the hierarchy between study, series, and instance. Test routing or
deduplication changes with more than one study and destination when applicable.

### 4. Long-running tools remain controllable

Many modules combine worker threads, schedules, retries, callbacks, and network
clients. Preserve foreground behavior, explicit stop paths, bounded retries or
backoff, and useful failure logs. Do not swallow a worker failure and leave the
process looking healthy.

### 5. External systems are trust boundaries

Orthanc, DICOM peers, RabbitMQ, SFTP, PostgreSQL, HL7 senders, the filesystem,
and environment variables can fail or provide malformed input. Validate at
those boundaries and keep credentials out of logs. Do not weaken validation or
error handling merely to simplify internal code.

## The easiest ways to lose data

1. **Deleting after an attempted send.** A request being issued is not proof
   that the destination stored the complete resource.
2. **Advancing a cursor on failure.** A persisted sequence or timestamp that
   skips failed work can make the loss permanent after restart.
3. **Testing against the wrong Orthanc.** Integration tests call destructive
   helpers such as `delete_all_content()` and remove Docker volumes. Use only
   the dedicated Compose stacks under `tests/`.
4. **Treating shutdown as an edge case.** Thread, callback, and connection
   cleanup affects correctness because these tools are expected to run for long
   periods and recover from service interruptions.

Never use real patient data as a fixture. Do not run a development command
against a clinical, production, or otherwise valuable Orthanc instance.

## How it works

Most files in `orthanc_tools/` define one reusable class plus a `python -m`
entry point. CLI arguments may also come from environment variables for Docker
use. `orthanc_tools/__init__.py` exposes the package's public imports.

Tools use `orthanc-api-client` for REST operations and pydicom for DICOM data.
The cloner, migrators, comparator, syncher, forwarder, monitor, and replicator
coordinate copies or changes between systems. Helpers under
`orthanc_tools/helpers/` own scheduling and timing behavior. The bundled
`hl7Lib` package parses HL7 messages, runs an HL7 server, and builds DICOM
worklists or report series.

## Where code lives

- `orthanc_tools/*.py` contains the main tools and their CLI entry points.
- `orthanc_tools/helpers/` contains shared scheduling, timer, timeout, and file
  cleanup helpers.
- `orthanc_tools/hl7Lib/` contains HL7 parsing, validation, networking, message
  handlers, and DICOM builders.
- `orthanc_tools/hl7Lib/tests/` contains the mostly self-contained HL7 tests.
- `tests/` contains unit tests, DICOM fixtures, and Docker-backed integration
  tests.
- `tests/docker-setup*` contains disposable Orthanc, RabbitMQ, and auth-service
  stacks used by integration tests.
- `demo-setups/` contains example deployments, not production configuration.
- `setup.py`, `requirements.txt`, `pyproject.toml`, and `tox.ini` contain package
  and test configuration. Check them together before changing support claims.

The package currently declares Python `>=3.11, <4` in `setup.py`. Keep its
classifiers, the tox matrix, and the CI compatibility matrix aligned when
changing supported versions.

## Verification

Start with the smallest test that proves the changed behavior. The test suite
uses `unittest`, even when pytest is used as its runner.

Useful focused commands:

```bash
python -m unittest orthanc_tools.hl7Lib.tests.test_hl7_message_parser
python -m unittest tests.test_old_files_deleter
python -m unittest tests.test_3_orthancs.Test3Orthancs.<test_method>
```

The following command and suites start Docker Compose services, delete their
test data and volumes, and use fixed localhost ports:

- `python -m unittest discover -s orthanc_tools/hl7Lib/tests`
- `tests.test_3_orthancs`
- `tests.test_orthanc_replicator`
- `tests.test_label_modifier`

`tests.test_3_orthancs` skips Compose when `ORTHANC_TEST_EXTERNAL=1` and uses
`ORTHANC_TEST_HOST`, but its tests still call `delete_all_content()`. Unset both
variables for normal runs. Use external mode only when the user explicitly
requests it and the three target Orthanc instances are confirmed disposable.

Before running one, confirm its dedicated ports and containers will not collide
with another checkout. Afterward, inspect `docker compose` state and `git diff`.
`tests.test_label_modifier` rewrites
`tests/docker-setup-auth/permissions.json` during its run.

Use `tox` only when the task concerns the full compatibility matrix. Do not run
every Docker suite merely to verify a small pure-Python change.

## Releases and pull requests

The local `origin` and `fork` remote names are a checkout convention, not a
guarantee. Inspect remotes before fetching, rebasing, or pushing.

`.github/workflows/ci.yml` runs on branch pushes and pull requests.
`.github/workflows/release.yml` runs only for tags, requires CI first, and can
publish the Python package and Docker image. A request to open a pull request
does not authorize creating a tag or publishing a package or image.

Do not change the package version, build distributions, publish artifacts, or
create a tag unless the user requests a release. Do not hand-edit `dist/`,
`*.egg-info/`, caches, or virtual environments.

## Taste

- Trace the complete transfer, retry, checkpoint, and cleanup path before
  editing it.
- Prefer the existing client APIs, helpers, standard library, and nearby code
  over new wrappers or dependencies.
- Preserve public imports, CLI flags, and environment variable behavior unless
  the task explicitly allows a breaking change.
- Keep network and filesystem guards at their boundaries. Keep the core data
  flow direct enough to audit.
- Tests should prove failure and recovery for destructive, concurrent, or
  persisted-state changes.
- Comments explain operational constraints, data-safety invariants, and
  protocol quirks. Remove narration that merely repeats the code.
