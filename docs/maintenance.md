# Maintenance, testing, and recovery

Fork images are built from this repository. Installing `orthanc-tools` from PyPI installs the
upstream distribution and does not identify this fork's forwarding changes. Use a release image
tag and its `org.opencontainers.image.revision` label to identify the deployed source.

## Reproducible builds

`requirements.txt` contains reviewed direct dependencies; `requirements.lock` and
`requirements-build.lock` pin the complete runtime and build environments with hashes.
The Dockerfile pins the Python image digest. Update these pins deliberately, run the regression
and integration checks below, and audit the resulting runtime before releasing an image.

### Dependency review: 2026-11-07

The November refresh updates certifi to 2026.10.23 for the critical
[compromised emSign CA advisory](https://github.com/advisories/GHSA-6q48-6c7w-9gw2).
Python 3.14.10 also blocks the compromised intermediate keys and fixes
`tarfile.tar_filter` traversal (CVE-2026-115557 and CVE-2026-114583). See the
[Python release notes](https://www.python.org/downloads/release/python-31410/).
Setuptools 84.2.1 fixes the Windows legacy wheel-installation path escape
([GHSA-xmj3-9gf7-h52w](https://github.com/advisories/GHSA-xmj3-9gf7-h52w)).
Pip 26.3 replaces the base image's vulnerable vendored urllib3 and msgpack copies;
updating the application's own packages alone does not replace pip's copies.

Compatible maintenance updates include Paramiko 5.1.0 (RSA SHA-2 signing and
agent-key fixes, plus hybrid post-quantum key exchange), schedule 1.2.2 (timezone
scheduling fixes), hl7 0.4.5 (parser/MLLP fixes), blessed 1.52.0, wcwidth 0.9.4,
and pycparser 3.12. StrEnum 0.5.0 is deferred because its pre-1.0 minor update
has no identified security need. The other runtime/build pins were current.

Both lockfiles pass pip-audit with no known advisories at this review date.
The finished image was also scanned with Trivy 0.76.2 and its 2026-11-06 database.
This is **not a clean bill of health for the whole image**: Debian 13 findings,
including high and critical severities, remain without fixed versions in that
database. Reducing the full Debian build image to a tested runtime image is a
separate hardening opportunity. Trivy also reports setuptools 70.3.0 from pip's
vendored metadata; pip contains the `pkg_resources` subset, not the vulnerable
`wheel.py` or `package_index.py` implementations. Installed setuptools is 84.2.1.

Recent CPython advisories also need follow-up when a patched 3.14 image ships:
[CVE-2026-116566](https://www.cve.org/CVERecord?id=CVE-2026-116566) (embedded NULs
in `socket.getaddrinfo`) and
[CVE-2026-103995](https://www.cve.org/CVERecord?id=CVE-2026-103995) (lenient HTTP
framing). The 3.14.10-only FIPS regression
[CVE-2026-119170](https://www.cve.org/CVERecord?id=CVE-2026-119170) affects OpenSSL
FIPS mode; that mode was verified disabled in this release image. Reassess this
if deploying with different OpenSSL configuration.

```sh
docker build --build-arg VCS_REF="$(git rev-parse HEAD)" -t python-orthanc-tools:maintenance .
```

## Disposable production-like tests

The maintenance overlay uses pinned Orthanc 26.9.1 and PostgreSQL 17.11, PostgreSQL indexing,
filesystem DICOM storage, source overwrite disabled, and short HTTP keep-alive. It publishes
no ports and uses project-scoped storage. All credentials and DICOM inputs are synthetic.

Use a unique test project. Legacy Docker integration tests also require
`ORTHANC_TEST_ALLOW_DELETE=1`; without that flag, ordinary unit discovery skips them.
CI runs mixed peering/DICOM routing with six forwarding workers, then exercises destination
failure, job timeout/restart, real routing metadata, heartbeat responsiveness, and worklist safety.

```sh
docker compose -p orthanc-maintenance-test \
  -f tests/docker-setup/docker-compose.yml -f tests/docker-setup/maintenance.yml up -d

docker run --rm --network orthanc-maintenance-test_default \
  --mount type=bind,source="$PWD",target=/work,readonly \
  -e ORTHANC_SOURCE_URL=http://orthanc-a:8042 \
  -e ORTHANC_ALWAYS_DESTINATION_URL=http://orthanc-b:8042 \
  -e ORTHANC_FILTERED_DESTINATION_URL=http://orthanc-c:8042 \
  -e ORTHANC_TEST_ALLOW_DELETE=1 \
  python-orthanc-tools:maintenance \
  python -m unittest discover -s /work/tests -p test_forwarder_integration.py

docker compose -p orthanc-maintenance-test \
  -f tests/docker-setup/docker-compose.yml -f tests/docker-setup/maintenance.yml down --volumes
```

## Forwarding receipts and uncertain outcomes

Metadata `4600` contains the no-match routing marker. Metadata `4601` contains a JSON receipt
on the actual study, series, or instance selected by the trigger. Its `configuration` identifies
the destination configuration; `snapshot` identifies the sorted instance IDs; `sent` lists fully
confirmed destinations. `jobs` keys are JSON-encoded `[destination_retry_key, unit]` pairs, where
the unit is `all`, `series:<Orthanc ID>`, or `instance:<Orthanc ID>`.

Each job value has this meaning:

| Value | Meaning |
| --- | --- |
| An Orthanc job ID string | Resume polling this accepted job. |
| `true` | This unit was confirmed successful; it survives pruning of Orthanc job history. |
| `null` | Submission was intended, but its outcome is uncertain. Automatic resubmission is blocked. |

If submission loses its response, metadata cannot be persisted, or a pending job disappears
from Orthanc history, retain the source and investigate. Pause only the affected tools service
while reconciling the recorded resource, destination, snapshot, job history, and destination
inventory. Replace an uncertain marker with a job ID only when it is proven to belong to that
same send; mark success only after delivery is independently verified. Clear an intent to allow
retry only after establishing that no send was accepted. If the outcome cannot be established,
leave the source and receipt intact for manual recovery. Keep credentials and clinical data out
of diagnostic exports.

Health checks measure source/API and process responsiveness. A responsive job can remain
pending while its destination is unavailable; forwarding errors, job age, and retained backlog
need operational monitoring. Docker restart policies restart exited processes, not merely
unhealthy containers. HTTP waits are bounded; a source-listing failure exits the CLI without
stranding workers. An unwritable heartbeat logs an error and leaves health to expire.

## Safe backup and worklist replacement

Worklist replacement validates and creates the new item before removing matching old IDs.
Existing items survive validation or creation failures. A failed cleanup can leave duplicates;
it must be investigated rather than treated as failed creation and blindly replayed.

`PostgresDumper` verifies the SFTP host using system `known_hosts` or
`SFTP_KNOWN_HOSTS` / `--sftp_known_hosts`. Its server must support OpenSSH atomic
`posix-rename`. Dumps upload to exclusive temporary files; publication occurs only after
both `pg_dump` and gzip succeed. A failure preserves the previous daily backup and exits
unsuccessfully. Mount a trusted known-hosts file and ensure `pg_dump` is installed when using
this tool. Restore-test backups against disposable PostgreSQL before relying on them.

## Production rollout and rollback

Verify live commands, image IDs, routing, source job history, health settings, and backlog counts
before rollout. Retain the previous image and configuration in a protected host-side rollback
directory. Recreate only the tools service with the tested image; preserve Orthanc/PostgreSQL
services, storage, routes, and credentials. Wait for healthy startup, inspect application failures
without exporting patient data, and verify backlog/forwarding behavior before updating the next
service. Roll back the image/configuration if readiness or routing verification fails; preserve
receipts and source data during recovery.
