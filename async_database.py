import asyncio
import logging
from typing import Dict, Any, List, Optional, Union, AsyncGenerator
from contextlib import asynccontextmanager
import json
from datetime import datetime
import aiosqlite
import asyncpg

from monitoring import metrics
from health_checks import health_checker

logger = logging.getLogger(__name__)

class AsyncDatabasePool:
    """
    Async database connection pool with support for SQLite and PostgreSQL.
    
    Provides connection pooling, prepared statements, and query building
    for optimal database performance.
    """
    
    def __init__(self, 
                 db_type: str = 'sqlite',
                 database: str = 'twitter.db',
                 host: str = 'localhost',
                 port: int = 5432,
                 user: str = 'twitter',
                 password: str = '',
                 min_connections: int = 2,
                 max_connections: int = 10,
                 statement_cache_size: int = 100):
        """
        Initialize async database pool.
        
        Args:
            db_type: Database type ('sqlite' or 'postgresql')
            database: Database name or path
            host: Database host (PostgreSQL only)
            port: Database port (PostgreSQL only)
            user: Database user (PostgreSQL only)
            password: Database password (PostgreSQL only)
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
            statement_cache_size: Number of prepared statements to cache
        """
        self.db_type = db_type.lower()
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.statement_cache_size = statement_cache_size
        
        # Connection pool
        self._pool: Optional[Union[aiosqlite.Pool, asyncpg.Pool]] = None
        self._initialized = False
        
        # Prepared statement cache
        self._statement_cache: Dict[str, str] = {}
        self._cache_lock = asyncio.Lock()
        
        # Statistics
        self.total_queries = 0
        self.total_connections = 0
        self.active_connections = 0
        
        logger.info(f"Initialized async database pool for {self.db_type}")
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self._initialized:
            return
        
        try:
            if self.db_type == 'sqlite':
                # Create SQLite pool
                self._pool = await aiosqlite.create_pool(
                    self.database,
                    minsize=self.min_connections,
                    maxsize=self.max_connections
                )
                
                # Initialize schema
                await self._initialize_sqlite_schema()
                
            elif self.db_type == 'postgresql':
                # Create PostgreSQL pool
                self._pool = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    min_size=self.min_connections,
                    max_size=self.max_connections,
                    statement_cache_size=self.statement_cache_size
                )
                
                # Initialize schema
                await self._initialize_postgresql_schema()
            
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            self._initialized = True
            logger.info(f"Database pool initialized with {self.min_connections}-{self.max_connections} connections")
            
            # Register health check
            health_checker.register_check('database', self._health_check)
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def _initialize_sqlite_schema(self):
        """Initialize SQLite database schema."""
        async with self.acquire() as conn:
            # Enable foreign keys
            await conn.execute("PRAGMA foreign_keys = ON")
            
            # Create tweets table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tweets (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    created_at TEXT,
                    user TEXT,
                    username TEXT,
                    retweet_count INTEGER DEFAULT 0,
                    favorite_count INTEGER DEFAULT 0,
                    view_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    quote_count INTEGER DEFAULT 0,
                    lang TEXT,
                    query TEXT,
                    search_product TEXT,
                    collected_at TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create sentiment analysis table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sentiment_analysis (
                    tweet_id TEXT PRIMARY KEY,
                    sentiment_label TEXT,
                    sentiment_compound REAL,
                    sentiment_positive REAL,
                    sentiment_negative REAL,
                    sentiment_neutral REAL,
                    sentiment_polarity REAL,
                    sentiment_subjectivity REAL,
                    analyzed_at TEXT,
                    method TEXT,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            ''')
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_lang ON tweets(lang)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sentiment_label ON sentiment_analysis(sentiment_label)')
            
            await conn.commit()
    
    async def _initialize_postgresql_schema(self):
        """Initialize PostgreSQL database schema."""
        async with self.acquire() as conn:
            # Create tweets table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tweets (
                    id VARCHAR(50) PRIMARY KEY,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP,
                    user VARCHAR(100),
                    username VARCHAR(100),
                    retweet_count INTEGER DEFAULT 0,
                    favorite_count INTEGER DEFAULT 0,
                    view_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    quote_count INTEGER DEFAULT 0,
                    lang VARCHAR(10),
                    query TEXT,
                    search_product VARCHAR(20),
                    collected_at TIMESTAMP,
                    metadata JSONB
                )
            ''')
            
            # Create sentiment analysis table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sentiment_analysis (
                    tweet_id VARCHAR(50) PRIMARY KEY,
                    sentiment_label VARCHAR(20),
                    sentiment_compound REAL,
                    sentiment_positive REAL,
                    sentiment_negative REAL,
                    sentiment_neutral REAL,
                    sentiment_polarity REAL,
                    sentiment_subjectivity REAL,
                    analyzed_at TIMESTAMP,
                    method VARCHAR(20),
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            ''')
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_lang ON tweets(lang)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sentiment_label ON sentiment_analysis(sentiment_label)')
            
            logger.info("PostgreSQL schema initialized")
    
    @asynccontextmanager
    async def acquire(self):
        """
        Acquire a connection from the pool.
        
        Yields:
            Database connection object
        """
        if not self._initialized:
            await self.initialize()
        
        if self._pool is None:
            raise RuntimeError("Database pool not initialized")
        
        self.active_connections += 1
        self.total_connections += 1
        
        # Update metrics
        metrics.record_db_connection(active=True)
        
        try:
            if self.db_type == 'sqlite':
                async with self._pool.acquire() as conn:
                    yield conn
            else:  # postgresql
                async with self._pool.acquire() as conn:
                    yield conn
        
        finally:
            self.active_connections -= 1
            metrics.record_db_connection(active=False)
    
    async def execute(self, query: str, params: tuple = None) -> aiosqlite.Cursor:
        """
        Execute a query with optional parameters.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Cursor object
        """
        start_time = time.time()
        
        try:
            async with self.acquire() as conn:
                if params:
                    cursor = await conn.execute(query, params)
                else:
                    cursor = await conn.execute(query)
                
                await conn.commit()
                
                # Record metrics
                self.total_queries += 1
                query_time = time.time() - start_time
                metrics.record_db_query(self._get_query_type(query), query_time)
                
                return cursor
        
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            metrics.record_error('database')
            raise
    
    async def executemany(self, query: str, params_list: List[tuple]) -> aiosqlite.Cursor:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Cursor object
        """
        start_time = time.time()
        
        try:
            async with self.acquire() as conn:
                if self.db_type == 'sqlite':
                    cursor = await conn.executemany(query, params_list)
                else:
                    # PostgreSQL uses execute_many for bulk operations
                    await conn.executemany(query, params_list)
                    cursor = conn.cursor()
                
                await conn.commit()
                
                # Record metrics
                self.total_queries += len(params_list)
                query_time = time.time() - start_time
                avg_time = query_time / len(params_list) if params_list else 0
                
                for _ in params_list:
                    metrics.record_db_query(self._get_query_type(query), avg_time)
                
                return cursor
        
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            metrics.record_error('database')
            raise
    
    async def fetchall(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Fetch all rows from a query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries
        """
        cursor = await self.execute(query, params)
        
        if self.db_type == 'sqlite':
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        else:
            # PostgreSQL returns list of Record objects
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def fetchone(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row from a query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Dictionary or None
        """
        cursor = await self.execute(query, params)
        
        if self.db_type == 'sqlite':
            row = await cursor.fetchone()
            return dict(row) if row else None
        else:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def fetchmany(self, query: str, params: tuple = None, size: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch multiple rows from a query.
        
        Args:
            query: SQL query string
            params: Query parameters
            size: Number of rows to fetch
            
        Returns:
            List of dictionaries
        """
        cursor = await self.execute(query, params)
        
        if self.db_type == 'sqlite':
            rows = await cursor.fetchmany(size)
            return [dict(row) for row in rows]
        else:
            rows = await cursor.fetchmany(size)
            return [dict(row) for row in rows]
    
    def _get_query_type(self, query: str) -> str:
        """Extract query type from SQL query."""
        query_upper = query.strip().upper()
        
        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        else:
            return 'OTHER'
    
    # Prepared statement methods
    async def prepare_statement(self, query: str) -> str:
        """
        Prepare a statement and cache it.
        
        Args:
            query: SQL query string
            
        Returns:
            Statement key
        """
        # Generate statement key from query hash
        import hashlib
        statement_key = hashlib.md5(query.encode()).hexdigest()
        
        async with self._cache_lock:
            if statement_key not in self._statement_cache:
                self._statement_cache[statement_key] = query
                
                # Limit cache size
                if len(self._statement_cache) > self.statement_cache_size:
                    # Remove oldest entry
                    oldest_key = next(iter(self._statement_cache))
                    del self._statement_cache[oldest_key]
        
        return statement_key
    
    async def execute_prepared(self, statement_key: str, params: tuple = None):
        """
        Execute a prepared statement.
        
        Args:
            statement_key: Statement key from prepare_statement
            params: Query parameters
        """
        async with self._cache_lock:
            if statement_key not in self._statement_cache:
                raise ValueError(f"Statement not found: {statement_key}")
            
            query = self._statement_cache[statement_key]
        
        return await self.execute(query, params)
    
    # Tweet-specific methods
    async def save_tweet(self, tweet: Dict[str, Any]) -> bool:
        """
        Save a tweet to the database.
        
        Args:
            tweet: Tweet dictionary
            
        Returns:
            True if successful
        """
        query = '''
            INSERT OR REPLACE INTO tweets (
                id, text, created_at, user, username, retweet_count,
                favorite_count, view_count, reply_count, quote_count,
                lang, query, search_product, collected_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        metadata = tweet.get('metadata', {})
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        
        params = (
            tweet.get('id'),
            tweet.get('text'),
            tweet.get('created_at'),
            tweet.get('user'),
            tweet.get('username'),
            tweet.get('retweet_count', 0),
            tweet.get('favorite_count', 0),
            tweet.get('view_count', 0),
            tweet.get('reply_count', 0),
            tweet.get('quote_count', 0),
            tweet.get('lang'),
            tweet.get('query'),
            tweet.get('search_product'),
            tweet.get('collected_at'),
            metadata
        )
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to save tweet: {e}")
            return False
    
    async def save_tweets_batch(self, tweets: List[Dict[str, Any]]) -> int:
        """
        Save multiple tweets in a batch.
        
        Args:
            tweets: List of tweet dictionaries
            
        Returns:
            Number of tweets saved
        """
        if not tweets:
            return 0
        
        query = '''
            INSERT OR REPLACE INTO tweets (
                id, text, created_at, user, username, retweet_count,
                favorite_count, view_count, reply_count, quote_count,
                lang, query, search_product, collected_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        params_list = []
        for tweet in tweets:
            metadata = tweet.get('metadata', {})
            if isinstance(metadata, dict):
                metadata = json.dumps(metadata)
            
            params = (
                tweet.get('id'),
                tweet.get('text'),
                tweet.get('created_at'),
                tweet.get('user'),
                tweet.get('username'),
                tweet.get('retweet_count', 0),
                tweet.get('favorite_count', 0),
                tweet.get('view_count', 0),
                tweet.get('reply_count', 0),
                tweet.get('quote_count', 0),
                tweet.get('lang'),
                tweet.get('query'),
                tweet.get('search_product'),
                tweet.get('collected_at'),
                metadata
            )
            params_list.append(params)
        
        try:
            await self.executemany(query, params_list)
            return len(tweets)
        except Exception as e:
            logger.error(f"Failed to save tweets batch: {e}")
            return 0
    
    async def get_tweet(self, tweet_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a tweet by ID.
        
        Args:
            tweet_id: Tweet ID
            
        Returns:
            Tweet dictionary or None
        """
        query = "SELECT * FROM tweets WHERE id = ?"
        
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        return await self.fetchone(query, (tweet_id,))
    
    async def get_tweets_by_query(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get tweets by search query.
        
        Args:
            query: Search query string
            limit: Maximum number of tweets to return
            
        Returns:
            List of tweet dictionaries
        """
        query_sql = "SELECT * FROM tweets WHERE query = ? ORDER BY created_at DESC LIMIT ?"
        
        if self.db_type == 'postgresql':
            query_sql = query_sql.replace('?', '%s')
        
        return await self.fetchall(query_sql, (query, limit))
    
    async def get_tweets_by_date_range(
        self, 
        start_date: str, 
        end_date: str, 
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get tweets within a date range.
        
        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            limit: Maximum number of tweets
            
        Returns:
            List of tweet dictionaries
        """
        query = """
            SELECT * FROM tweets 
            WHERE created_at BETWEEN ? AND ? 
            ORDER BY created_at DESC 
            LIMIT ?
        """
        
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        return await self.fetchall(query, (start_date, end_date, limit))
    
    async def get_tweet_count(self, query_filter: Optional[str] = None) -> int:
        """
        Get total count of tweets.
        
        Args:
            query_filter: Optional query filter
            
        Returns:
            Tweet count
        """
        if query_filter:
            query = "SELECT COUNT(*) as count FROM tweets WHERE query = ?"
            params = (query_filter,)
        else:
            query = "SELECT COUNT(*) as count FROM tweets"
            params = None
        
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        result = await self.fetchone(query, params)
        return result['count'] if result else 0
    
    async def close(self):
        """Close the connection pool."""
        if self._pool:
            if self.db_type == 'sqlite':
                await self._pool.close()
            else:
                await self._pool.close()
            
            self._pool = None
            self._initialized = False
            
            logger.info("Database pool closed")
    
    async def _health_check(self):
        """Perform health check on database."""
        start_time = time.time()
        
        try:
            # Simple query to test connectivity
            await self.fetchone("SELECT 1")
            
            response_time = time.time() - start_time
            
            from health_checks import HealthCheckResult, HealthStatus
            
            return HealthCheckResult(
                name='database',
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                response_time=response_time,
                details={
                    'response_time_ms': round(response_time * 1000, 2),
                    'active_connections': self.active_connections,
                    'total_queries': self.total_queries
                }
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            
            from health_checks import HealthCheckResult, HealthStatus
            
            return HealthCheckResult(
                name='database',
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                response_time=response_time,
                details={
                    'error': str(e)
                }
            )


class QueryBuilder:
    """
    Safe query builder for constructing SQL queries programmatically.
    
    Prevents SQL injection and provides a fluent interface for building queries.
    """
    
    def __init__(self, db_type: str = 'sqlite'):
        """
        Initialize query builder.
        
        Args:
            db_type: Database type ('sqlite' or 'postgresql')
        """
        self.db_type = db_type
        self._select_fields: List[str] = []
        self._from_table: str = ""
        self._where_conditions: List[str] = []
        self._where_params: List[Any] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
    
    def select(self, *fields: str) -> 'QueryBuilder':
        """Specify SELECT fields."""
        self._select_fields = list(fields) if fields else ['*']
        return self
    
    def from_table(self, table: str) -> 'QueryBuilder':
        """Specify FROM table."""
        self._from_table = table
        return self
    
    def where(self, condition: str, *params: Any) -> 'QueryBuilder':
        """Add WHERE condition."""
        self._where_conditions.append(condition)
        self._where_params.extend(params)
        return self
    
    def where_in(self, field: str, values: List[Any]) -> 'QueryBuilder':
        """Add WHERE IN condition."""
        placeholders = ', '.join(['?' if self.db_type == 'sqlite' else '%s'] * len(values))
        condition = f"{field} IN ({placeholders})"
        self._where_conditions.append(condition)
        self._where_params.extend(values)
        return self
    
    def order_by(self, field: str, direction: str = 'ASC') -> 'QueryBuilder':
        """Add ORDER BY clause."""
        self._order_by.append(f"{field} {direction}")
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Set LIMIT."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """Set OFFSET."""
        self._offset = offset
        return self
    
    def build(self) -> tuple:
        """
        Build the SQL query and parameters.
        
        Returns:
            Tuple of (query_string, parameters)
        """
        # Build SELECT clause
        query = f"SELECT {', '.join(self._select_fields)}"
        
        # Build FROM clause
        query += f" FROM {self._from_table}"
        
        # Build WHERE clause
        if self._where_conditions:
            query += " WHERE " + " AND ".join(self._where_conditions)
        
        # Build ORDER BY clause
        if self._order_by:
            query += " ORDER BY " + ", ".join(self._order_by)
        
        # Build LIMIT clause
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
        
        # Build OFFSET clause
        if self._offset is not None:
            query += f" OFFSET {self._offset}"
        
        # Replace ? with %s for PostgreSQL
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        return query, self._where_params
    
    def build_count(self) -> tuple:
        """
        Build a COUNT query.
        
        Returns:
            Tuple of (query_string, parameters)
        """
        # Save original select fields
        original_fields = self._select_fields
        
        # Build count query
        self._select_fields = ['COUNT(*) as count']
        query, params = self.build()
        
        # Restore original fields
        self._select_fields = original_fields
        
        return query, params


# Global database pool instance
_db_pool: Optional[AsyncDatabasePool] = None

async def get_database_pool() -> AsyncDatabasePool:
    """
    Get or create global database pool instance.
    
    Returns:
        AsyncDatabasePool instance
    """
    global _db_pool
    
    if _db_pool is None:
        # Load configuration
        try:
            from utils import load_config
            config = load_config()
            
            db_config = config.get('database', {})
            db_type = db_config.get('type', 'sqlite')
            database = db_config.get('database', 'twitter.db')
            
            if db_type == 'postgresql':
                _db_pool = AsyncDatabasePool(
                    db_type='postgresql',
                    database=database,
                    host=db_config.get('host', 'localhost'),
                    port=db_config.get('port', 5432),
                    user=db_config.get('user', 'twitter'),
                    password=db_config.get('password', ''),
                    min_connections=db_config.get('min_connections', 2),
                    max_connections=db_config.get('max_connections', 10)
                )
            else:
                _db_pool = AsyncDatabasePool(
                    db_type='sqlite',
                    database=database,
                    min_connections=db_config.get('min_connections', 2),
                    max_connections=db_config.get('max_connections', 10)
                )
            
        except Exception as e:
            logger.warning(f"Could not load database config: {e}, using defaults")
            _db_pool = AsyncDatabasePool()
    
    if not _db_pool._initialized:
        await _db_pool.initialize()
    
    return _db_pool


# Example usage
async def example_usage():
    """Example of how to use the async database pool."""
    
    # Get database pool
    db = await get_database_pool()
    
    # Example 1: Save a tweet
    async def example_1():
        tweet = {
            'id': '123456',
            'text': 'Hello world!',
            'created_at': datetime.now().isoformat(),
            'username': 'user123',
            'lang': 'en'
        }
        
        success = await db.save_tweet(tweet)
        print(f"Tweet saved: {success}")
    
    # Example 2: Save multiple tweets
    async def example_2():
        tweets = [
            {
                'id': f'tweet_{i}',
                'text': f'Tweet number {i}',
                'created_at': datetime.now().isoformat(),
                'username': f'user_{i % 10}',
                'lang': 'en'
            }
            for i in range(100)
        ]
        
        saved_count = await db.save_tweets_batch(tweets)
        print(f"Saved {saved_count} tweets")
    
    # Example 3: Query tweets
    async def example_3():
        # Using prepared statement
        stmt_key = await db.prepare_statement(
            "SELECT * FROM tweets WHERE username = ? ORDER BY created_at DESC LIMIT ?"
        )
        
        tweets = await db.fetchall_prepared(stmt_key, ('user_1', 10))
        print(f"Found {len(tweets)} tweets from user_1")
        
        for tweet in tweets:
            print(f"  - {tweet['text']}")
    
    # Example 4: Use query builder
    async def example_4():
        # Build a query programmatically
        qb = QueryBuilder(db.db_type)
        query, params = (qb
                        .select('id', 'text', 'username')
                        .from_table('tweets')
                        .where('lang = ?', 'en')
                        .where('retweet_count > ?', 10)
                        .order_by('created_at', 'DESC')
                        .limit(20)
                        .build())
        
        tweets = await db.fetchall(query, params)
        print(f"Found {len(tweets)} English tweets with >10 retweets")
    
    # Example 5: Get tweet count
    async def example_5():
        total_count = await db.get_tweet_count()
        print(f"Total tweets in database: {total_count}")
        
        # Count for specific query
        query_count = await db.get_tweet_count(query_filter='bitcoin')
        print(f"Tweets about bitcoin: {query_count}")
    
    # Run examples
    await example_1()
    await example_2()
    await example_3()
    await example_4()
    await example_5()
    
    # Close pool
    await db.close()


if __name__ == "__main__":
    # Run example usage
    asyncio.run(example_usage())