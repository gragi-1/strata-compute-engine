from alembic import context
from sqlalchemy import create_engine

from control_plane import models  # noqa: F401
from control_plane.config import Settings
from control_plane.database import Base

url = context.get_x_argument(as_dictionary=True).get("database_url") or Settings().database_url
if context.is_offline_mode():
    context.configure(url=url, target_metadata=Base.metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    with create_engine(url).connect() as connection:
        context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
