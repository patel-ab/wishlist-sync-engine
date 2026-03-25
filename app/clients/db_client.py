import psycopg2
import os
from psycopg2.extensions import connection as PgConnection

from app.config.db_connection_config import DBConfig

sslmode = os.getenv("DB_SSLMODE")

class PostgresClient:
    def __init__(self, config: DBConfig) -> None:
        self._host = config.db_host
        self._port = config.db_port
        self._db_name = config.db_name
        self._user = config.db_user
        self._password = config.db_password

    def create_connection(self) -> PgConnection:
        return psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._db_name,
            user=self._user,
            password=self._password,
            sslmode=sslmode
        )