"""Mixins for distinct database providers.

This is the place to include methods
for specific providers.
"""


class SqliteMixin:
    """Bind SQLite database provider."""

    db_provider = 'sqlite'


class PostgresqlMixin:
    """Bind PostgreSQL database provider."""

    db_provider = 'postgres'


class OracleMixin:
    """Bind oracle database provider."""

    db_provider = 'oracle'


class MysqlMixin:
    """Bind MySQL database provider."""

    db_provider = 'mysql'
