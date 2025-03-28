from typing import Dict, Any, List, Optional, Tuple
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from core.interfaces.database_interface import DatabaseConnectionInterface
from core.exceptions.custom_exceptions import DatabaseConnectionError


class SchemaRetriever:
    """
    Retrieves database schema information including tables, columns, data types,
    primary keys, foreign keys, and other metadata.
    """

    def __init__(self, connection: DatabaseConnectionInterface):
        """
        Initialize with a database connection.

        Args:
            connection (DatabaseConnectionInterface): An established database connection
        """
        self._connection = connection
        self._engine = None

    def _get_engine(self) -> Engine:
        """
        Get the SQLAlchemy engine from the connection.

        Returns:
            Engine: SQLAlchemy engine

        Raises:
            DatabaseConnectionError: If engine cannot be retrieved
        """
        if hasattr(self._connection, '_engine') and self._connection._engine is not None:
            return self._connection._engine
        else:
            raise DatabaseConnectionError("No active database engine")

    def get_all_tables(self) -> List[str]:
        """
        Get a list of all table names in the database.

        Returns:
            List[str]: List of table names
        """
        engine = self._get_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed schema information for a specific table.

        Args:
            table_name (str): The name of the table

        Returns:
            Dict[str, Any]: Table schema including columns, types, constraints
        """
        engine = self._get_engine()
        inspector = inspect(engine)

        # Get column information
        columns = inspector.get_columns(table_name)

        # Get primary key information
        pk_columns = inspector.get_pk_constraint(table_name)

        # Get foreign key information
        foreign_keys = inspector.get_foreign_keys(table_name)

        # Get index information
        indexes = inspector.get_indexes(table_name)

        # Get unique constraints
        unique_constraints = inspector.get_unique_constraints(table_name)

        return {
            'table_name': table_name,
            'columns': columns,
            'primary_key': pk_columns,
            'foreign_keys': foreign_keys,
            'indexes': indexes,
            'unique_constraints': unique_constraints
        }

    def get_column_metadata(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get metadata for all columns in the specified table.

        Args:
            table_name (str): The name of the table

        Returns:
            List[Dict[str, Any]]: List of column metadata dictionaries
        """
        engine = self._get_engine()
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)

        # Format the column information for easier consumption
        return [
            {
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col.get('nullable', True),
                'default': col.get('default', None),
                'comment': col.get('comment', None)
            }
            for col in columns
        ]

    def get_table_relationships(self, table_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all relationships for a specific table.

        Args:
            table_name (str): The name of the table

        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary with incoming and outgoing relationships
        """
        engine = self._get_engine()
        inspector = inspect(engine)

        # Outgoing foreign keys (this table references other tables)
        outgoing = inspector.get_foreign_keys(table_name)

        # Incoming foreign keys (other tables reference this table)
        incoming = []
        for other_table in self.get_all_tables():
            if other_table == table_name:
                continue

            for fk in inspector.get_foreign_keys(other_table):
                if fk.get('referred_table') == table_name:
                    fk['table_name'] = other_table
                    incoming.append(fk)

        return {
            'outgoing': outgoing,
            'incoming': incoming
        }

    def get_database_schema(self) -> Dict[str, Any]:
        """
        Get complete schema information for the entire database.

        Returns:
            Dict[str, Any]: Complete database schema information
        """
        tables = self.get_all_tables()
        schema = {
            'tables': {}
        }

        for table in tables:
            schema['tables'][table] = self.get_table_schema(table)

        return schema

    def get_schema_summary(self) -> Dict[str, Any]:
        """
        Get a summarized version of the database schema.

        Returns:
            Dict[str, Any]: Summary of database schema
        """
        tables = self.get_all_tables()

        summary = {
            'table_count': len(tables),
            'tables': {}
        }

        for table in tables:
            columns = self.get_column_metadata(table)
            summary['tables'][table] = {
                'column_count': len(columns),
                'columns': [col['name'] for col in columns]
            }

        return summary