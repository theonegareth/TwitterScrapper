import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import threading

logger = logging.getLogger(__name__)

class DatabaseManager:
    """SQLite database manager for tweet storage and retrieval."""
    
    def __init__(self, db_path: str = "twitter_data.db"):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._local = threading.local()
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None  # Enable autocommit mode
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    def _init_database(self):
        """Initialize database schema if it doesn't exist."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create tweets table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tweets (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    original_text TEXT,
                    created_at TEXT,
                    user_name TEXT,
                    username TEXT,
                    retweet_count INTEGER DEFAULT 0,
                    favorite_count INTEGER DEFAULT 0,
                    view_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    quote_count INTEGER DEFAULT 0,
                    lang TEXT,
                    query TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    cleaned_text INTEGER DEFAULT 0
                )
            ''')
            
            # Create sentiment analysis table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sentiment_analysis (
                    tweet_id TEXT PRIMARY KEY,
                    analyzer_method TEXT,
                    sentiment_label TEXT,
                    sentiment_compound REAL,
                    sentiment_positive REAL,
                    sentiment_negative REAL,
                    sentiment_neutral REAL,
                    sentiment_polarity REAL,
                    sentiment_subjectivity REAL,
                    analyzed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            ''')
            
            # Create metadata table for hashtags, mentions, URLs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tweet_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT NOT NULL,
                    metadata_type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            ''')
            
            # Create search history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS search_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT NOT NULL,
                    count INTEGER,
                    product TEXT,
                    fetched_count INTEGER,
                    search_time TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tweets_query ON tweets(query)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_sentiment_label ON sentiment_analysis(sentiment_label)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_metadata_tweet_id ON tweet_metadata(tweet_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_metadata_type ON tweet_metadata(metadata_type)
            ''')
            
            conn.commit()
            logger.info(f"Database initialized successfully at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def insert_tweet(self, tweet_data: Dict[str, Any], query: str) -> bool:
        """
        Insert a tweet into the database.
        
        Args:
            tweet_data: Dictionary containing tweet information
            query: Search query that found this tweet
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Insert tweet data
            cursor.execute('''
                INSERT OR REPLACE INTO tweets (
                    id, text, original_text, created_at, user_name, username,
                    retweet_count, favorite_count, view_count, reply_count,
                    quote_count, lang, query, cleaned_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tweet_data.get('id'),
                tweet_data.get('text'),
                tweet_data.get('original_text'),
                tweet_data.get('created_at'),
                tweet_data.get('user'),
                tweet_data.get('username'),
                tweet_data.get('retweet_count', 0),
                tweet_data.get('favorite_count', 0),
                tweet_data.get('view_count', 0),
                tweet_data.get('reply_count', 0),
                tweet_data.get('quote_count', 0),
                tweet_data.get('lang'),
                query,
                1 if tweet_data.get('original_text') else 0
            ))
            
            # Insert sentiment analysis if available
            if any(key.startswith('sentiment_') for key in tweet_data.keys()):
                cursor.execute('''
                    INSERT OR REPLACE INTO sentiment_analysis (
                        tweet_id, analyzer_method, sentiment_label, sentiment_compound,
                        sentiment_positive, sentiment_negative, sentiment_neutral,
                        sentiment_polarity, sentiment_subjectivity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tweet_data.get('id'),
                    tweet_data.get('sentiment_method', 'vader'),
                    tweet_data.get('sentiment_label'),
                    tweet_data.get('sentiment_compound'),
                    tweet_data.get('sentiment_positive'),
                    tweet_data.get('sentiment_negative'),
                    tweet_data.get('sentiment_neutral'),
                    tweet_data.get('sentiment_polarity'),
                    tweet_data.get('sentiment_subjectivity')
                ))
            
            # Insert metadata (hashtags, mentions, URLs)
            self._insert_metadata(cursor, tweet_data.get('id'), tweet_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert tweet {tweet_data.get('id')}: {e}")
            return False
    
    def _insert_metadata(self, cursor, tweet_id: str, tweet_data: Dict[str, Any]):
        """Insert tweet metadata (hashtags, mentions, URLs)."""
        try:
            # Extract and store hashtags
            import re
            text = tweet_data.get('text', '')
            
            hashtags = re.findall(r'#\w+', text)
            for hashtag in hashtags:
                cursor.execute('''
                    INSERT OR IGNORE INTO tweet_metadata (tweet_id, metadata_type, value)
                    VALUES (?, ?, ?)
                ''', (tweet_id, 'hashtag', hashtag))
            
            mentions = re.findall(r'@\w+', text)
            for mention in mentions:
                cursor.execute('''
                    INSERT OR IGNORE INTO tweet_metadata (tweet_id, metadata_type, value)
                    VALUES (?, ?, ?)
                ''', (tweet_id, 'mention', mention))
            
            urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
            for url in urls:
                cursor.execute('''
                    INSERT OR IGNORE INTO tweet_metadata (tweet_id, metadata_type, value)
                    VALUES (?, ?, ?)
                ''', (tweet_id, 'url', url))
                
        except Exception as e:
            logger.warning(f"Failed to insert metadata for tweet {tweet_id}: {e}")
    
    def insert_tweets_batch(self, tweets: List[Dict[str, Any]], query: str) -> int:
        """
        Insert multiple tweets in a batch transaction.
        
        Args:
            tweets: List of tweet dictionaries
            query: Search query
            
        Returns:
            Number of successfully inserted tweets
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            inserted_count = 0
            for tweet in tweets:
                if self.insert_tweet(tweet, query):
                    inserted_count += 1
            
            logger.info(f"Successfully inserted {inserted_count} out of {len(tweets)} tweets")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            return 0
    
    def get_tweets(self, query: Optional[str] = None, limit: Optional[int] = None, 
                   offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve tweets from database.
        
        Args:
            query: Filter by search query
            limit: Maximum number of tweets to return
            offset: Number of tweets to skip
            
        Returns:
            List of tweet dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            sql = '''
                SELECT t.*, s.* FROM tweets t
                LEFT JOIN sentiment_analysis s ON t.id = s.tweet_id
                WHERE 1=1
            '''
            params = []
            
            if query:
                sql += ' AND t.query = ?'
                params.append(query)
            
            sql += ' ORDER BY t.created_at DESC'
            
            if limit:
                sql += ' LIMIT ?'
                params.append(limit)
            
            if offset:
                sql += ' OFFSET ?'
                params.append(offset)
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            tweets = []
            for row in rows:
                tweet = dict(row)
                # Remove sentiment columns from main tweet dict
                sentiment_keys = [k for k in tweet.keys() if k.startswith('sentiment_') and tweet[k] is not None]
                if sentiment_keys:
                    tweet['sentiment'] = {k.replace('sentiment_', ''): tweet[k] for k in sentiment_keys}
                    for k in sentiment_keys:
                        del tweet[k]
                
                tweets.append(tweet)
            
            return tweets
            
        except Exception as e:
            logger.error(f"Failed to retrieve tweets: {e}")
            return []
    
    def get_tweet_count(self, query: Optional[str] = None) -> int:
        """Get total count of tweets in database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            sql = 'SELECT COUNT(*) as count FROM tweets WHERE 1=1'
            params = []
            
            if query:
                sql += ' AND query = ?'
                params.append(query)
            
            cursor.execute(sql, params)
            result = cursor.fetchone()
            
            return result['count'] if result else 0
            
        except Exception as e:
            logger.error(f"Failed to get tweet count: {e}")
            return 0
    
    def search_tweets(self, search_term: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search tweets by text content.
        
        Args:
            search_term: Term to search for in tweet text
            limit: Maximum number of results
            
        Returns:
            List of matching tweets
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT t.*, s.* FROM tweets t
                LEFT JOIN sentiment_analysis s ON t.id = s.tweet_id
                WHERE t.text LIKE ? OR t.original_text LIKE ?
                ORDER BY t.created_at DESC
                LIMIT ?
            ''', (f'%{search_term}%', f'%{search_term}%', limit))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to search tweets: {e}")
            return []
    
    def get_sentiment_stats(self, query: Optional[str] = None) -> Dict[str, Any]:
        """
        Get sentiment statistics for tweets.
        
        Args:
            query: Filter by search query
            
        Returns:
            Dictionary with sentiment statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            sql = '''
                SELECT 
                    sentiment_label,
                    COUNT(*) as count,
                    AVG(sentiment_compound) as avg_compound,
                    AVG(sentiment_polarity) as avg_polarity
                FROM sentiment_analysis s
                JOIN tweets t ON s.tweet_id = t.id
                WHERE 1=1
            '''
            params = []
            
            if query:
                sql += ' AND t.query = ?'
                params.append(query)
            
            sql += ' GROUP BY sentiment_label'
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            stats = {
                'total': sum(row['count'] for row in rows),
                'by_label': {row['sentiment_label']: row['count'] for row in rows},
                'averages': {}
            }
            
            for row in rows:
                if row['avg_compound'] is not None:
                    stats['averages']['compound'] = row['avg_compound']
                if row['avg_polarity'] is not None:
                    stats['averages']['polarity'] = row['avg_polarity']
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get sentiment stats: {e}")
            return {'total': 0, 'by_label': {}, 'averages': {}}
    
    def record_search(self, query: str, count: int, product: str, fetched_count: int):
        """Record a search operation in the history."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO search_history (query, count, product, fetched_count)
                VALUES (?, ?, ?, ?)
            ''', (query, count, product, fetched_count))
            
        except Exception as e:
            logger.warning(f"Failed to record search: {e}")
    
    def export_to_json(self, query: Optional[str] = None) -> str:
        """
        Export tweets to JSON format.
        
        Args:
            query: Filter by search query
            
        Returns:
            JSON string containing all tweets
        """
        try:
            tweets = self.get_tweets(query)
            return json.dumps(tweets, indent=2, default=str)
            
        except Exception as e:
            logger.error(f"Failed to export to JSON: {e}")
            return "[]"
    
    def close(self):
        """Close database connection."""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            delattr(self._local, 'connection')