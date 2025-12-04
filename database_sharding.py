import sqlite3
import logging
import hashlib
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
import threading
import json

logger = logging.getLogger(__name__)

class ShardManager:
    """
    Database shard manager for horizontal partitioning of tweet data.
    
    Implements sharding strategy to distribute tweets across multiple databases
    based on tweet ID, date, or other criteria for improved performance and
    scalability with large datasets.
    """
    
    def __init__(self, base_path: str = "data/shards", shards_per_year: int = 4):
        """
        Initialize shard manager.
        
        Args:
            base_path: Base directory for shard databases
            shards_per_year: Number of shards per year (quarterly=4, monthly=12)
        """
        self.base_path = Path(base_path)
        self.shards_per_year = shards_per_year
        
        # Create base directory
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Shard registry (maps shard keys to database paths)
        self._shard_registry: Dict[str, str] = {}
        self._shard_connections: Dict[str, sqlite3.Connection] = {}
        self._lock = threading.Lock()
        
        # Load or create shard registry
        self._load_shard_registry()
        
        logger.info(f"Initialized shard manager with {shards_per_year} shards per year")
    
    def _load_shard_registry(self):
        """Load shard registry from file or create if not exists."""
        registry_path = self.base_path / "shard_registry.json"
        
        if registry_path.exists():
            try:
                with open(registry_path, 'r') as f:
                    self._shard_registry = json.load(f)
                logger.info(f"Loaded {len(self._shard_registry)} shards from registry")
            except Exception as e:
                logger.error(f"Failed to load shard registry: {e}")
                self._shard_registry = {}
        else:
            self._shard_registry = {}
            self._save_shard_registry()
    
    def _save_shard_registry(self):
        """Save shard registry to file."""
        registry_path = self.base_path / "shard_registry.json"
        
        try:
            with open(registry_path, 'w') as f:
                json.dump(self._shard_registry, f, indent=2)
            logger.debug("Shard registry saved")
        except Exception as e:
            logger.error(f"Failed to save shard registry: {e}")
    
    def get_shard_key(self, tweet: Dict[str, Any]) -> str:
        """
        Determine shard key for a tweet.
        
        Args:
            tweet: Tweet dictionary
            
        Returns:
            Shard key string
        """
        # Use tweet creation date and ID for sharding
        created_at = tweet.get('created_at', '')
        
        if created_at:
            try:
                # Parse date and determine shard
                if isinstance(created_at, str):
                    # Try to parse ISO format
                    date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                else:
                    # Assume it's already a datetime-like object
                    date_obj = created_at
                
                year = date_obj.year
                month = date_obj.month
                
                # Calculate shard number
                if self.shards_per_year == 4:  # Quarterly
                    shard_num = (month - 1) // 3 + 1
                    period = f"Q{shard_num}"
                elif self.shards_per_year == 12:  # Monthly
                    period = f"{month:02d}"
                else:  # Custom partitioning
                    shard_num = (month - 1) * self.shards_per_year // 12 + 1
                    period = f"S{shard_num:02d}"
                
                return f"{year}_{period}"
                
            except Exception as e:
                logger.warning(f"Failed to parse date {created_at}: {e}")
        
        # Fallback: use tweet ID hash
        tweet_id = tweet.get('id', '0')
        hash_val = int(hashlib.md5(str(tweet_id).encode()).hexdigest(), 16)
        shard_num = (hash_val % self.shards_per_year) + 1
        
        return f"fallback_S{shard_num:02d}"
    
    def get_shard_path(self, shard_key: str) -> Path:
        """
        Get database path for a shard key.
        
        Args:
            shard_key: Shard key
            
        Returns:
            Path to shard database
        """
        if shard_key not in self._shard_registry:
            # Create new shard
            shard_path = self.base_path / f"tweets_{shard_key}.db"
            self._shard_registry[shard_key] = str(shard_path)
            self._save_shard_registry()
            
            # Initialize shard database
            self._initialize_shard(shard_key, shard_path)
        
        return Path(self._shard_registry[shard_key])
    
    def _initialize_shard(self, shard_key: str, shard_path: Path):
        """
        Initialize a new shard database with schema.
        
        Args:
            shard_key: Shard key
            shard_path: Path to shard database
        """
        try:
            conn = sqlite3.connect(shard_path)
            cursor = conn.cursor()
            
            # Create tweets table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tweets (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    created_at TEXT,
                    user TEXT,
                    username TEXT,
                    retweet_count INTEGER,
                    favorite_count INTEGER,
                    view_count INTEGER,
                    reply_count INTEGER,
                    quote_count INTEGER,
                    lang TEXT,
                    query TEXT,
                    search_product TEXT,
                    collected_at TEXT,
                    shard_key TEXT
                )
            ''')
            
            # Create sentiment analysis table
            cursor.execute('''
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
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id)
                )
            ''')
            
            # Create metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tweets_lang ON tweets(lang)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sentiment_label ON sentiment_analysis(sentiment_label)')
            
            # Insert shard metadata
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', ('shard_key', shard_key, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Initialized shard database: {shard_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize shard {shard_key}: {e}")
            raise
    
    @contextmanager
    def get_shard_connection(self, shard_key: str):
        """
        Get a connection to a shard database.
        
        Args:
            shard_key: Shard key
            
        Yields:
            SQLite connection object
        """
        shard_path = self.get_shard_path(shard_key)
        
        # Check if we have a cached connection
        conn_key = str(shard_path)
        
        with self._lock:
            if conn_key in self._shard_connections:
                try:
                    # Test connection
                    conn = self._shard_connections[conn_key]
                    conn.execute("SELECT 1")
                    yield conn
                    return
                except sqlite3.Error:
                    # Connection is dead, remove it
                    del self._shard_connections[conn_key]
            
            # Create new connection
            conn = sqlite3.connect(shard_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            
            # Store connection for reuse
            self._shard_connections[conn_key] = conn
            
            try:
                yield conn
            finally:
                # Don't close connection, keep it for reuse
                pass
    
    def save_tweet(self, tweet: Dict[str, Any], sentiment: Optional[Dict[str, Any]] = None):
        """
        Save a tweet to the appropriate shard.
        
        Args:
            tweet: Tweet dictionary
            sentiment: Sentiment analysis results (optional)
        """
        shard_key = self.get_shard_key(tweet)
        
        with self.get_shard_connection(shard_key) as conn:
            cursor = conn.cursor()
            
            try:
                # Insert tweet
                cursor.execute('''
                    INSERT OR REPLACE INTO tweets (
                        id, text, created_at, user, username, retweet_count,
                        favorite_count, view_count, reply_count, quote_count,
                        lang, query, search_product, collected_at, shard_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
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
                    shard_key
                ))
                
                # Insert sentiment analysis if provided
                if sentiment:
                    cursor.execute('''
                        INSERT OR REPLACE INTO sentiment_analysis (
                            tweet_id, sentiment_label, sentiment_compound,
                            sentiment_positive, sentiment_negative, sentiment_neutral,
                            sentiment_polarity, sentiment_subjectivity, analyzed_at, method
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tweet.get('id'),
                        sentiment.get('label'),
                        sentiment.get('compound'),
                        sentiment.get('positive'),
                        sentiment.get('negative'),
                        sentiment.get('neutral'),
                        sentiment.get('polarity'),
                        sentiment.get('subjectivity'),
                        tweet.get('analyzed_at'),
                        sentiment.get('method', self.method)
                    ))
                
                conn.commit()
                
            except Exception as e:
                logger.error(f"Failed to save tweet to shard {shard_key}: {e}")
                conn.rollback()
                raise
    
    def save_tweets_batch(self, tweets: List[Dict[str, Any]], 
                         sentiments: Optional[List[Dict[str, Any]]] = None):
        """
        Save multiple tweets to appropriate shards.
        
        Args:
            tweets: List of tweet dictionaries
            sentiments: List of sentiment analysis results (optional)
        """
        # Group tweets by shard
        shards_dict: Dict[str, List[tuple]] = {}
        sentiments_dict: Dict[str, List[Dict[str, Any]]] = {}
        
        for i, tweet in enumerate(tweets):
            shard_key = self.get_shard_key(tweet)
            
            if shard_key not in shards_dict:
                shards_dict[shard_key] = []
                sentiments_dict[shard_key] = []
            
            shards_dict[shard_key].append(tweet)
            if sentiments and i < len(sentiments):
                sentiments_dict[shard_key].append(sentiments[i])
        
        # Save to each shard
        for shard_key, shard_tweets in shards_dict.items():
            shard_sentiments = sentiments_dict.get(shard_key, [])
            
            with self.get_shard_connection(shard_key) as conn:
                cursor = conn.cursor()
                
                try:
                    # Insert tweets
                    for i, tweet in enumerate(shard_tweets):
                        cursor.execute('''
                            INSERT OR REPLACE INTO tweets (
                                id, text, created_at, user, username, retweet_count,
                                favorite_count, view_count, reply_count, quote_count,
                                lang, query, search_product, collected_at, shard_key
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
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
                            shard_key
                        ))
                        
                        # Insert sentiment if available
                        if shard_sentiments and i < len(shard_sentiments):
                            sentiment = shard_sentiments[i]
                            cursor.execute('''
                                INSERT OR REPLACE INTO sentiment_analysis (
                                    tweet_id, sentiment_label, sentiment_compound,
                                    sentiment_positive, sentiment_negative, sentiment_neutral,
                                    sentiment_polarity, sentiment_subjectivity, analyzed_at, method
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                tweet.get('id'),
                                sentiment.get('label'),
                                sentiment.get('compound'),
                                sentiment.get('positive'),
                                sentiment.get('negative'),
                                sentiment.get('neutral'),
                                sentiment.get('polarity'),
                                sentiment.get('subjectivity'),
                                tweet.get('analyzed_at'),
                                sentiment.get('method', 'unknown')
                            ))
                    
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"Failed to save batch to shard {shard_key}: {e}")
                    conn.rollback()
                    raise
    
    def get_tweet(self, tweet_id: str, shard_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve a tweet from shards.
        
        Args:
            tweet_id: Tweet ID
            shard_hint: Optional shard key hint
            
        Returns:
            Tweet dictionary or None if not found
        """
        # If shard hint is provided, search only that shard
        if shard_hint and shard_hint in self._shard_registry:
            return self._search_shard(shard_hint, tweet_id)
        
        # Otherwise, search all shards
        for shard_key in self._shard_registry.keys():
            tweet = self._search_shard(shard_key, tweet_id)
            if tweet:
                return tweet
        
        return None
    
    def _search_shard(self, shard_key: str, tweet_id: str) -> Optional[Dict[str, Any]]:
        """Search for a tweet in a specific shard."""
        try:
            with self.get_shard_connection(shard_key) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT t.*, s.sentiment_label, s.sentiment_compound,
                           s.sentiment_positive, s.sentiment_negative, s.sentiment_neutral,
                           s.sentiment_polarity, s.sentiment_subjectivity, s.method
                    FROM tweets t
                    LEFT JOIN sentiment_analysis s ON t.id = s.tweet_id
                    WHERE t.id = ?
                ''', (tweet_id,))
                
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
        
        except Exception as e:
            logger.error(f"Error searching shard {shard_key} for tweet {tweet_id}: {e}")
        
        return None
    
    def query_shard(
        self, 
        shard_key: str, 
        query: str, 
        params: tuple = ()
    ) -> List[Dict[str, Any]]:
        """
        Execute a query on a specific shard.
        
        Args:
            shard_key: Shard key
            query: SQL query
            params: Query parameters
            
        Returns:
            List of results
        """
        try:
            with self.get_shard_connection(shard_key) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        
        except Exception as e:
            logger.error(f"Query failed on shard {shard_key}: {e}")
            return []
    
    def query_all_shards(
        self, 
        query: str, 
        params: tuple = (),
        limit_per_shard: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query on all shards and combine results.
        
        Args:
            query: SQL query
            params: Query parameters
            limit_per_shard: Limit results per shard
            
        Returns:
            Combined list of results
        """
        all_results = []
        
        for shard_key in self._shard_registry.keys():
            shard_query = query
            
            if limit_per_shard:
                shard_query += f" LIMIT {limit_per_shard}"
            
            results = self.query_shard(shard_key, shard_query, params)
            all_results.extend(results)
        
        return all_results
    
    def get_shard_stats(self, shard_key: str) -> Dict[str, Any]:
        """
        Get statistics for a specific shard.
        
        Args:
            shard_key: Shard key
            
        Returns:
            Dictionary with shard statistics
        """
        try:
            with self.get_shard_connection(shard_key) as conn:
                cursor = conn.cursor()
                
                # Get tweet count
                cursor.execute("SELECT COUNT(*) as count FROM tweets")
                tweet_count = cursor.fetchone()['count']
                
                # Get date range
                cursor.execute("""
                    SELECT MIN(created_at) as min_date, MAX(created_at) as max_date 
                    FROM tweets
                """)
                date_range = cursor.fetchone()
                
                # Get language distribution
                cursor.execute("""
                    SELECT lang, COUNT(*) as count 
                    FROM tweets 
                    GROUP BY lang 
                    ORDER BY count DESC
                """)
                languages = cursor.fetchall()
                
                # Get sentiment distribution
                cursor.execute("""
                    SELECT sentiment_label, COUNT(*) as count 
                    FROM sentiment_analysis 
                    GROUP BY sentiment_label 
                    ORDER BY count DESC
                """)
                sentiments = cursor.fetchall()
                
                # Get file size
                shard_path = self.get_shard_path(shard_key)
                file_size = shard_path.stat().st_size if shard_path.exists() else 0
                
                return {
                    'shard_key': shard_key,
                    'tweet_count': tweet_count,
                    'file_size_bytes': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'min_date': date_range['min_date'],
                    'max_date': date_range['max_date'],
                    'languages': {row['lang']: row['count'] for row in languages},
                    'sentiments': {row['sentiment_label']: row['sentiment_label'] for row in sentiments}
                }
        
        except Exception as e:
            logger.error(f"Failed to get stats for shard {shard_key}: {e}")
            return {'shard_key': shard_key, 'error': str(e)}
    
    def get_all_shard_stats(self) -> Dict[str, Any]:
        """
        Get statistics for all shards.
        
        Returns:
            Dictionary with statistics for all shards
        """
        stats = {
            'total_shards': len(self._shard_registry),
            'shards': {},
            'total_tweets': 0,
            'total_size_mb': 0
        }
        
        for shard_key in self._shard_registry.keys():
            shard_stats = self.get_shard_stats(shard_key)
            stats['shards'][shard_key] = shard_stats
            
            if 'tweet_count' in shard_stats:
                stats['total_tweets'] += shard_stats['tweet_count']
            
            if 'file_size_mb' in shard_stats:
                stats['total_size_mb'] += shard_stats['file_size_mb']
        
        return stats
    
    def get_shard_for_date(self, date: datetime) -> str:
        """
        Get shard key for a specific date.
        
        Args:
            date: Date object
            
        Returns:
            Shard key
        """
        year = date.year
        month = date.month
        
        if self.shards_per_year == 4:  # Quarterly
            shard_num = (month - 1) // 3 + 1
            period = f"Q{shard_num}"
        elif self.shards_per_year == 12:  # Monthly
            period = f"{month:02d}"
        else:  # Custom
            shard_num = (month - 1) * self.shards_per_year // 12 + 1
            period = f"S{shard_num:02d}"
        
        return f"{year}_{period}"
    
    def get_shards_for_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[str]:
        """
        Get all shard keys for a date range.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of shard keys
        """
        shard_keys = set()
        
        current_date = start_date
        while current_date <= end_date:
            shard_key = self.get_shard_for_date(current_date)
            shard_keys.add(shard_key)
            
            # Move to next period
            if self.shards_per_year == 4:  # Quarterly
                if current_date.month <= 3:
                    current_date = current_date.replace(month=4)
                elif current_date.month <= 6:
                    current_date = current_date.replace(month=7)
                elif current_date.month <= 9:
                    current_date = current_date.replace(month=10)
                else:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:  # Monthly or custom
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
        
        return sorted(list(shard_keys))
    
    def close_all_connections(self):
        """Close all shard connections."""
        with self._lock:
            for conn in self._shard_connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
            
            self._shard_connections.clear()
            logger.info("Closed all shard connections")


# Global shard manager instance
_shard_manager: Optional[ShardManager] = None

def get_shard_manager() -> ShardManager:
    """
    Get or create global shard manager instance.
    
    Returns:
        ShardManager instance
    """
    global _shard_manager
    
    if _shard_manager is None:
        _shard_manager = ShardManager()
    
    return _shard_manager


# Example usage
def example_usage():
    """Example of how to use the shard manager."""
    
    # Get shard manager
    shard_manager = get_shard_manager()
    
    # Example 1: Save tweets to appropriate shards
    def example_1():
        tweets = [
            {
                'id': '123',
                'text': 'Hello world',
                'created_at': '2024-03-15T10:30:00',
                'username': 'user1'
            },
            {
                'id': '124',
                'text': 'Another tweet',
                'created_at': '2024-06-20T15:45:00',
                'username': 'user2'
            },
            {
                'id': '125',
                'text': 'Third tweet',
                'created_at': '2024-11-10T08:15:00',
                'username': 'user3'
            }
        ]
        
        # Save tweets (automatically distributed to appropriate shards)
        shard_manager.save_tweets_batch(tweets)
        
        print("Tweets saved to shards")
    
    # Example 2: Query specific shard
    def example_2():
        # Query Q1 2024 shard
        results = shard_manager.query_shard(
            '2024_Q1',
            "SELECT * FROM tweets WHERE username = ?",
            ('user1',)
        )
        
        print(f"Found {len(results)} tweets in Q1 2024")
        for tweet in results:
            print(f"  - {tweet['text']}")
    
    # Example 3: Query all shards
    def example_3():
        # Get all tweets from a user across all shards
        all_tweets = shard_manager.query_all_shards(
            "SELECT * FROM tweets WHERE username = ?",
            ('user1',)
        )
        
        print(f"Found {len(all_tweets)} tweets across all shards")
    
    # Example 4: Get shard statistics
    def example_4():
        stats = shard_manager.get_all_shard_stats()
        
        print(f"Total shards: {stats['total_shards']}")
        print(f"Total tweets: {stats['total_tweets']}")
        print(f"Total size: {stats['total_size_mb']:.2f} MB")
        
        for shard_key, shard_stats in stats['shards'].items():
            print(f"  Shard {shard_key}: {shard_stats['tweet_count']} tweets")
    
    # Example 5: Date range query
    def example_5():
        from datetime import datetime
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 6, 30)
        
        # Get all shards for date range
        shard_keys = shard_manager.get_shards_for_date_range(start_date, end_date)
        
        print(f"Shards for H1 2024: {shard_keys}")
        
        # Query each shard
        all_tweets = []
        for shard_key in shard_keys:
            tweets = shard_manager.query_shard(
                shard_key,
                "SELECT * FROM tweets ORDER BY created_at"
            )
            all_tweets.extend(tweets)
        
        print(f"Found {len(all_tweets)} tweets in H1 2024")
    
    # Run examples
    example_1()
    example_2()
    example_3()
    example_4()
    example_5()


if __name__ == "__main__":
    example_usage()