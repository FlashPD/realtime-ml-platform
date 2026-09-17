FROM python:3.12.12-slim-bookworm

RUN apt-get update \
    && apt-get install --yes --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tripml \
    && useradd --uid 10001 --gid tripml --create-home tripml

WORKDIR /opt/tripml
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir '.[serving]'

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XDG_CACHE_HOME=/tmp/cache \
    MPLCONFIGDIR=/tmp/matplotlib
USER 10001:10001
EXPOSE 8000
CMD ["tripml", "serve", "--host", "0.0.0.0", "--port", "8000"]
