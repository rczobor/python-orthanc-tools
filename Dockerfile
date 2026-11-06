FROM python:3.14@sha256:d18055009f327b22993de14458aa5a6bac032cdf459ea2a2e9bbdc3d3d49e4fb

ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/rczobor/python-orthanc-tools" \
      org.opencontainers.image.version="0.23.1" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /src
COPY requirements.lock requirements-build.lock ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock -r requirements-build.lock

COPY orthanc_tools/ ./orthanc_tools/
COPY LICENSE.txt MANIFEST.in pyproject.toml README.md setup.cfg setup.py requirements.txt ./
RUN python -m build --no-isolation --wheel . \
    && python -m pip install --no-cache-dir --no-deps dist/orthanc_tools*.whl \
    && python -m pip check

WORKDIR /
