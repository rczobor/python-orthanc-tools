# Working on python-orthanc-tools

## Setup and verification

Use an isolated virtual environment with a Python version in the CI matrix
(currently 3.11 and 3.14). Install the reviewed runtime and build dependencies:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock -r requirements-build.lock
```

On Windows, activate `.venv` with its native `Scripts` activation command.
Do not install project dependencies into the host's system Python or copy a
live Orthanc environment into a worktree.

During iteration, target the changed unit/regression module with
`python -m unittest tests.<module>`. Before calling the final revision verified,
use the CI contract: both unit suites, distribution build, and the synthetic
container forwarding/recovery workflow. CI can provide the full final validation
for ordinary changes; forwarding/deletion, recovery, authentication and image
changes need their affected isolated integration checks locally too. Never use a
production Orthanc as a test target.

```sh
python -m unittest discover -s tests
python -m unittest discover -s orthanc_tools/hl7Lib/tests
python -m build --no-isolation
```

Tests that need Docker must use the explicit disposable fixtures in `tests/`.
An unavailable Docker daemon is a capability failure, not a failed application
assertion. Keep deletion opt-ins scoped to those synthetic fixtures.

## Maintenance batches

Snapshot the initial PR set for a batch request; report later arrivals separately.
When taking over a bot branch, preserve concurrent changes, add separate fix
commits, and avoid bot rebases that can discard them. Use the shared `babysit-pr`
skill, including its independent-subagent fallback for unavailable advisory bots.
Required checks, reviews and explicit merge authorization still apply.
