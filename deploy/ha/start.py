"""Read mounted secrets without privileged execution or logging their contents."""

import json
import os
import re
from pathlib import Path

import yaml


def main():
    name = os.environ["STRATA_PG_NODE"]
    if not re.fullmatch(r"pg-[abc]", name):
        raise ValueError("STRATA_PG_NODE must be pg-a, pg-b or pg-c")
    root = Path("/tmp/strata-ha")
    root.mkdir(mode=0o700, exist_ok=True)
    secrets = json.loads(Path("/run/strata/secrets.json").read_text())
    required = {"superuser", "replication", "application", "rest"}
    if set(secrets) != required or any(
        not isinstance(value, str) or len(value) < 32 for value in secrets.values()
    ):
        raise ValueError("HA secrets require four private passwords of at least 32 characters")
    for file in ("server.pem", "server.key", "ca.pem", "client.pem", "client.key"):
        target = root / file
        target.write_bytes((Path("/run/strata/tls") / file).read_bytes())
        target.chmod(0o600)
    config = yaml.safe_load(Path("/opt/strata-ha/patroni.yml").read_text())
    config["name"] = name
    config["postgresql"]["connect_address"] = name + ":5432"
    config["postgresql"]["authentication"]["superuser"]["password"] = secrets["superuser"]
    replication = config["postgresql"]["authentication"]["replication"]
    replication["password"] = secrets["replication"]
    replication["sslrootcert"] = str(root / "ca.pem")
    config["restapi"]["connect_address"] = name + ":8008"
    config["restapi"]["authentication"] = {"username": "ha-admin", "password": secrets["rest"]}
    # Patroni also verifies TLS when consulting another member before promotion.
    config["ctl"] = {
        "cacert": str(root / "ca.pem"),
        "certfile": str(root / "client.pem"),
        "keyfile": str(root / "client.key"),
    }
    settings = root / "patroni.yml"
    settings.write_text(yaml.safe_dump(config))
    settings.chmod(0o600)
    os.execvp("patroni", ["patroni", str(settings)])


if __name__ == "__main__":
    main()
