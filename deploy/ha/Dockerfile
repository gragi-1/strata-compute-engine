FROM python:3.12-slim-trixie@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016 AS postgres-build
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential bison flex pkg-config libssl-dev libreadline-dev zlib1g-dev \
    liblz4-dev libzstd-dev ca-certificates bzip2 && rm -rf /var/lib/apt/lists/*
ADD https://ftp.postgresql.org/pub/source/v17.11/postgresql-17.11.tar.bz2 /tmp/postgres.tar.bz2
RUN echo 'dd27f2b3c59e73ed14aa3324901242bf69a032a6347805f274e6260322d42979  /tmp/postgres.tar.bz2' \
    | sha256sum --check --strict \
    && mkdir /postgres-source && tar -xjf /tmp/postgres.tar.bz2 --strip-components=1 -C /postgres-source
WORKDIR /postgres-source
# Strata stores relational state and JSON; XML/XSLT and server-side scripting are unnecessary.
RUN ./configure --prefix=/usr/local/pgsql --with-openssl --with-lz4 --with-zstd --without-icu \
    && make -j2 && make install-strip \
    && useradd --system --uid 999 postgres && chown -R postgres /postgres-source
USER postgres
RUN make check

FROM python:3.12-slim-trixie@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016
RUN apt-get update && apt-get upgrade --no-install-recommends -y \
    && apt-get install --no-install-recommends -y libreadline8t64 zlib1g liblz4-1 libzstd1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 999 --home-dir /var/lib/postgresql postgres \
    && mkdir -p /var/lib/postgresql/data /var/run/postgresql \
    && chown -R postgres:postgres /var/lib/postgresql /var/run/postgresql
COPY --from=postgres-build /usr/local/pgsql /usr/local/pgsql
COPY --from=postgres-build /postgres-source/COPYRIGHT /usr/local/share/strata/postgresql-COPYRIGHT
COPY deploy/ha/requirements.txt /opt/strata-ha/requirements.txt
RUN python3 -m venv /opt/strata-ha/venv \
    && /opt/strata-ha/venv/bin/pip install --no-cache-dir --require-hashes \
       -r /opt/strata-ha/requirements.txt
COPY deploy/ha/patroni.yml deploy/ha/start.py deploy/ha/bootstrap.py /opt/strata-ha/
COPY deploy/ha/vendor-components.json /usr/local/share/strata/vendor-components.json
ENV PATH="/opt/strata-ha/venv/bin:/usr/local/pgsql/bin:$PATH" PYTHONUNBUFFERED=1
USER postgres
VOLUME /var/lib/postgresql/data
ENTRYPOINT ["python", "/opt/strata-ha/start.py"]
