"""Schema operations for an installed wheel, independent of the working directory."""

from importlib.resources import files
from pathlib import Path

from alembic import command
from alembic.config import Config


def configuration() -> Config:
    installed = Path(str(files("control_plane").joinpath("_migrations")))
    # Editable installs use the repository's authored migration directory.
    source = Path(__file__).resolve().parent.parent / "migrations"
    location = installed if (installed / "env.py").is_file() else source
    if not (location / "env.py").is_file():
        raise RuntimeError("Strata installation does not contain schema migrations")
    config = Config()
    config.set_main_option("script_location", str(location).replace("%", "%%"))
    return config


def upgrade(check: bool = False) -> None:
    config = configuration()
    if check:
        command.check(config)
    else:
        command.upgrade(config, "head")
