from typing import Dict, Any
import threading
import time
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging

from data_analytics_platform.core.exceptions.custom_exceptions import DatabaseConnectionError
from data_analytics_platform.database.config import DatabaseConfig

logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Manages a pool of database connections.
    Provides connection reuse, monitoring, and automatic recovery.
    """

    def __init__(
            self,
            config: DatabaseConfig,
            pool_size: int = 5,
            max_overflow: int = 10,
            pool_timeout: int = 30,
            pool_recycle: int = 1800,
            health_check_interval: int = 300
    ):
        """
        Initialize a connection pool.

        Args:
            config (DatabaseConfig): Database configuration
            pool_size (int): The size of the pool to be maintained
            max_overflow (int): The maximum overflow size of the pool
            pool_timeout (int): Seconds to wait before giving up on getting a connection
            pool_recycle (int): Seconds after which a connection is automatically recycled
            health_check_interval (int): Seconds between health checks
        """
        self.config = config
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.health_check_interval = health_check_interval

        # Connection tracking
        self._engines: Dict[str, Engine] = {}
        self._session_factories: Dict[str, sessionmaker] = {}
        self._last_used: Dict[str, float] = {}
        self._connection_ids = set()
        self._lock = threading.RLock()

        # Monitoring thread
        self._monitor_thread = None
        self._monitor_active = False

    def start_monitoring(self):
        """Start the connection monitoring thread."""
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._monitor_active = True
            self._monitor_thread = threading.Thread(
                target=self._monitor_connections,
                daemon=True
            )
            self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop the connection monitoring thread."""
        self._monitor_active = False
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=1.0)
            self._monitor_thread = None

    def _monitor_connections(self):
        """Monitor connections and perform health checks."""
        while self._monitor_active:
            try:
                self._perform_health_check()

                # Clean up expired connections
                current_time = time.time()
                with self._lock:
                    for conn_id, last_used in list(self._last_used.items()):
                        # If not used for a while, close and remove
                        if current_time - last_used > self.pool_recycle:
                            self._remove_connection(conn_id)

                time.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"Error in connection monitoring: {str(e)}")
                time.sleep(10)  # Wait a bit before retrying

    def _perform_health_check(self):
        """Perform health check on all connections."""
        with self._lock:
            for conn_id, engine in list(self._engines.items()):
                try:
                    # Execute a simple query to check connection
                    with engine.connect() as conn:
                        conn.execute(sa.text("SELECT 1"))
                except SQLAlchemyError as e:
                    logger.warning(f"Connection {conn_id} failed health check: {str(e)}")
                    self._remove_connection(conn_id)

    def _remove_connection(self, conn_id: str):
        """
        Remove a connection from the pool.

        Args:
            conn_id (str): Connection ID to remove
        """
        with self._lock:
            if conn_id in self._engines:
                try:
                    self._engines[conn_id].dispose()
                except:
                    pass

                del self._engines[conn_id]

            if conn_id in self._session_factories:
                del self._session_factories[conn_id]

            if conn_id in self._last_used:
                del self._last_used[conn_id]

            if conn_id in self._connection_ids:
                self._connection_ids.remove(conn_id)

    def get_engine(self, connection_string: str, **kwargs) -> Engine:
        """
        Get a database engine from the pool or create a new one.

        Args:
            connection_string (str): SQLAlchemy connection string
            **kwargs: Additional engine creation parameters

        Returns:
            Engine: SQLAlchemy engine

        Raises:
            DatabaseConnectionError: If engine creation fails
        """
        # Create a unique identifier for this connection
        conn_id = f"{connection_string}:{hash(str(kwargs))}"

        with self._lock:
            # Check if we already have an engine for this connection
            if conn_id in self._engines:
                self._last_used[conn_id] = time.time()
                return self._engines[conn_id]

            # Check if we're at max capacity
            if len(self._engines) >= self.pool_size + self.max_overflow:
                raise DatabaseConnectionError("Connection pool is full")

            # Create new engine
            try:
                # Add pooling parameters
                engine_kwargs = {
                    "pool_size": self.pool_size,
                    "max_overflow": self.max_overflow,
                    "pool_timeout": self.pool_timeout,
                    "pool_recycle": self.pool_recycle,
                    **kwargs
                }

                engine = sa.create_engine(connection_string, **engine_kwargs)

                # Test the connection
                with engine.connect() as connection:
                    connection.execute(sa.text("SELECT 1"))

                # Store the engine
                self._engines[conn_id] = engine
                self._connection_ids.add(conn_id)
                self._last_used[conn_id] = time.time()

                return engine
            except SQLAlchemyError as e:
                raise DatabaseConnectionError(
                    f"Failed to create database engine: {str(e)}"
                ) from e

    def get_session_factory(self, connection_string: str, **kwargs) -> sessionmaker:
        """
        Get a session factory for the given connection string.

        Args:
            connection_string (str): SQLAlchemy connection string
            **kwargs: Additional engine creation parameters

        Returns:
            sessionmaker: SQLAlchemy session factory
        """
        # Create a unique identifier for this connection
        conn_id = f"{connection_string}:{hash(str(kwargs))}"

        with self._lock:
            # Check if we already have a session factory
            if conn_id in self._session_factories:
                self._last_used[conn_id] = time.time()
                return self._session_factories[conn_id]

            # Get the engine
            engine = self.get_engine(connection_string, **kwargs)

            # Create session factory
            session_factory = sessionmaker(bind=engine)

            # Store the session factory
            self._session_factories[conn_id] = session_factory
            self._last_used[conn_id] = time.time()

            return session_factory

    def dispose_all(self):
        """Dispose all engines and clean up the pool."""
        with self._lock:
            for engine in self._engines.values():
                try:
                    engine.dispose()
                except:
                    pass

            self._engines.clear()
            self._session_factories.clear()
            self._last_used.clear()
            self._connection_ids.clear()

        self.stop_monitoring()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.

        Returns:
            Dict[str, Any]: Pool statistics
        """
        with self._lock:
            return {
                "active_connections": len(self._engines),
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
                "connection_ids": list(self._connection_ids),
                "last_used": {k: time.ctime(v) for k, v in self._last_used.items()}
            }