FROM ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1 AS uv

FROM python:3.13-slim-bookworm@sha256:b6bd71b0dd3811ddbcbc523ec2965fd1e1bcfdf7a20ab24679273d3bee726129

ARG UDATA_COMMIT=0546582058d84706812a1c37387576efc4e5ad1f

COPY --from=uv /uv /uvx /bin/

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential git libffi-dev libjpeg62-turbo-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/* \
    && git clone https://github.com/opendatateam/udata.git /opt/udata \
    && git -C /opt/udata checkout --detach "$UDATA_COMMIT" \
    && test "$(git -C /opt/udata rev-parse HEAD)" = "$UDATA_COMMIT" \
    && uv pip install --system --no-cache 'flask-caching==2.3.1' /opt/udata

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
    > /opt/udata/udata.cfg

EXPOSE 7000

CMD ["udata", "serve", "--host=0.0.0.0", "--port=7000", "--no-reload"]
