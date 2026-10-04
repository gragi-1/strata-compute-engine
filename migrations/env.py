from alembic import context
from sqlalchemy import create_engine, text

from control_plane import models  # noqa: F401
from control_plane.config import Settings
from control_plane.database import Base

url = context.get_x_argument(as_dictionary=True).get("database_url") or Settings().database_url
if context.is_offline_mode():
    context.configure(url=url, target_metadata=Base.metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    with create_engine(url).begin() as connection:
        if connection.dialect.name == "postgresql":
            # Serialize upgrades from independent installed administrators on this schema.
            connection.execute(
                text(
                    "SELECT pg_advisory_xact_lock(hashtext(current_database()), "
                    "hashtext('strata-migrations-' || current_schema()))"
                )
            )
        context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
