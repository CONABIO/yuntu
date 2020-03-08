"""Mixins for distinct database providers.

This is the place to include methods for specific providers.
"""


class SqliteMixin:
    """Bind SQLite database provider."""

    provider = 'sqlite'


class PostgresqlMixin:
    """Bind PostgreSQL database provider."""

    provider = 'postgres'


class OracleMixin:
    """Bind oracle database provider."""

    provider = 'oracle'


class MysqlMixin:
    """Bind MySQL database provider."""

    provider = 'mysql'
