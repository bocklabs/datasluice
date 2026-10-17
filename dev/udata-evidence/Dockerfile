FROM ghcr.io/astral-sh/uv@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv

FROM python@sha256:b6bd71b0dd3811ddbcbc523ec2965fd1e1bcfdf7a20ab24679273d3bee726129

ARG UDATA_COMMIT=0546582058d84706812a1c37387576efc4e5ad1f

COPY --from=uv /uv /uvx /bin/

COPY build-inputs.sha256 build-requirements.in build-requirements.txt debian.sources requirements.in requirements.txt requirements-auth.txt /opt/udata-build/
COPY upstream/pyproject.toml /opt/udata-build/upstream/pyproject.toml

RUN echo '0846b917ed93cdc4011f75ba3406da7ed3f10fa6a3a6d0d4da2d4f22edc79b1b  /opt/udata-build/build-inputs.sha256' | sha256sum --check - \
    && cd /opt/udata-build \
    && sha256sum --check build-inputs.sha256 \
    && cp debian.sources /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential=12.9 git=1:2.39.5-0+deb12u3 libffi-dev=3.4.4-1 \
        libjpeg62-turbo-dev=1:2.1.5-2 libxml2-dev=2.9.14+dfsg-1.3~deb12u6 libxslt1-dev=1.1.35-1+deb12u4 \
    && rm -rf /var/lib/apt/lists/* \
    && git clone https://github.com/opendatateam/udata.git /opt/udata \
    && git -C /opt/udata checkout --detach "$UDATA_COMMIT" \
    && test "$(git -C /opt/udata rev-parse HEAD)" = "$UDATA_COMMIT" \
    && cmp /opt/udata/pyproject.toml /opt/udata-build/upstream/pyproject.toml \
    && uv pip install --system --no-cache --require-hashes --require-build-hashes \
        --build-constraints /opt/udata-build/build-requirements.txt --requirements /opt/udata-build/requirements.txt \
    && uv pip install --system --no-cache --no-deps --require-build-hashes \
        --build-constraints /opt/udata-build/build-requirements.txt /opt/udata \
    && uv pip uninstall --system 'Flask-Security' \
    && uv pip install --system --no-cache --require-hashes --no-deps --force-reinstall \
        --only-binary :all: --requirements /opt/udata-build/requirements-auth.txt \
    && python -c 'from flask_security.utils import get_within_delta'

WORKDIR /opt/udata

RUN printf '%s\n' \
    'import os' \
    'SECRET_KEY = os.environ["UDATA_SECRET_KEY"]' \
    'API_TOKEN_SECRET = os.environ["UDATA_API_TOKEN_SECRET"]' \
    'MONGODB_HOST = "mongodb://mongo:27017/udata"' \
    'ELASTICSEARCH_URL = "http://search:9200"' \
    'ELASTICSEARCH_INDEX_BASENAME = "udata-evidence"' \
    'CELERY_BROKER_URL = "redis://redis:6379"' \
    'CELERY_RESULT_BACKEND = "redis://redis:6379"' \
    'CACHE_REDIS_URL = "redis://redis:6379/1"' \
    'MAIL_SERVER = "mailpit"' \
    'MAIL_PORT = 1025' \
    'SEND_MAIL = False' \
    'SESSION_COOKIE_SECURE = False' \
    'URLS_ALLOW_LOCAL = True' \
    'FS_URL = "http://127.0.0.1:5640"' \
    > /opt/udata/udata.cfg

ENV GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0=safe.directory \
    GIT_CONFIG_VALUE_0=/opt/udata

USER 10001:10001

EXPOSE 7000

CMD ["udata", "serve", "--host=0.0.0.0", "--port=7000", "--no-reload"]
