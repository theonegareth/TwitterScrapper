import json
import logging
import hashlib
from typing import Any, Optional, Dict
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class CacheManager:
    """In-memory cache manager for API responses and processed data."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        Initialize cache manager.
        
        Args:
            max_size: Maximum number of items in cache
            default_ttl: Default time-to-live in seconds (1 hour)
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from arguments."""
        key_data = f"{prefix}:{str(args)}:{str(sorted(kwargs.items()))}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        try:
            with self._lock:
                if key not in self._cache:
                    return None
                
                entry = self._cache[key]
                if time.time() > entry['expires_at']:
                    self._remove_key(key)
                    return None
                
                # Update access time for LRU
                self._access_times[key] = time.time()
                return entry['value']
                
        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        try:
            with self._lock:
                # Enforce max size using LRU
                if len(self._cache) >= self.max_size and key not in self._cache:
                    self._evict_lru()
                
                ttl = ttl or self.default_ttl
                self._cache[key] = {
                    'value': value,
                    'expires_at': time.time() + ttl,
                    'created_at': time.time()
                }
                self._access_times[key] = time.time()
                
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
    
    def _remove_key(self, key: str):
        """Remove key from cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._access_times:
            del self._access_times[key]
    
    def _evict_lru(self):
        """Evict least recently used item."""
        if not self._access_times:
            return
        
        lru_key = min(self._access_times.keys(), key=lambda k: self._access_times[k])
        self._remove_key(lru_key)
        logger.debug(f"Evicted LRU cache key: {lru_key}")
    
    def delete(self, key: str):
        """Delete key from cache."""
        try:
            with self._lock:
                self._remove_key(key)
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
    
    def clear(self):
        """Clear all cache entries."""
        try:
            with self._lock:
                self._cache.clear()
                self._access_times.clear()
                logger.info("Cache cleared")
        except Exception as e:
            logger.warning(f"Cache clear failed: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            with self._lock:
                current_time = time.time()
                valid_items = sum(
                    1 for entry in self._cache.values()
                    if current_time <= entry['expires_at']
                )
                
                return {
                    'total_items': len(self._cache),
                    'valid_items': valid_items,
                    'expired_items': len(self._cache) - valid_items,
                    'max_size': self.max_size,
                    'default_ttl': self.default_ttl
                }
        except Exception as e:
            logger.warning(f"Failed to get cache stats: {e}")
            return {}

class SearchCache(CacheManager):
    """Cache manager specifically for search results."""
    
    def get_search_results(self, query: str, count: int, product: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached search results."""
        key = self._generate_key('search', query, count, product)
        return self.get(key)
    
    def set_search_results(self, query: str, count: int, product: str, results: List[Dict[str, Any]], ttl: Optional[int] = 1800):
        """Cache search results (default 30 minutes)."""
        key = self._generate_key('search', query, count, product)
        self.set(key, results, ttl)

class SentimentCache(CacheManager):
    """Cache manager for sentiment analysis results."""
    
    def get_sentiment(self, text: str, method: str) -> Optional[Dict[str, Any]]:
        """Get cached sentiment analysis."""
        key = self._generate_key('sentiment', text, method)
        return self.get(key)
    
    def set_sentiment(self, text: str, method: str, sentiment: Dict[str, Any], ttl: Optional[int] = 86400):
        """Cache sentiment analysis (default 24 hours)."""
        key = self._generate_key('sentiment', text, method)
        self.set(key, sentiment, ttl)

class APICache(CacheManager):
    """Cache manager for API responses."""
    
    def get_api_response(self, endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
        """Get cached API response."""
        key = self._generate_key('api', endpoint, **params)
        return self.get(key)
    
    def set_api_response(self, endpoint: str, params: Dict[str, Any], response: Any, ttl: Optional[int] = 600):
        """Cache API response (default 10 minutes)."""
        key = self._generate_key('api', endpoint, **params)
        self.set(key, response, ttl)

# Global cache instances
search_cache = SearchCache(max_size=500, default_ttl=1800)  # 30 minutes
sentiment_cache = SentimentCache(max_size=2000, default_ttl=86400)  # 24 hours
api_cache = APICache(max_size=1000, default_ttl=600)  # 10 minutes