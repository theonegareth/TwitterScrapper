import json
import logging
import hashlib
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import asyncio

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

logger = logging.getLogger(__name__)

class RedisCacheBackend:
    """
    Redis-based cache backend for distributed caching across multiple instances.
    
    This provides a Redis implementation that can replace or supplement the
    in-memory cache for distributed deployments.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, 
                 password: Optional[str] = None, ssl: bool = False,
                 key_prefix: str = 'twitter_scrapper:'):
        """
        Initialize Redis cache backend.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            password: Redis password (optional)
            ssl: Use SSL connection
            key_prefix: Prefix for all cache keys
        """
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed. Run: pip install redis")
        
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.key_prefix = key_prefix
        
        self._redis: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()
        
        # Statistics
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self._deletes = 0
    
    async def connect(self) -> bool:
        """
        Connect to Redis server.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self._redis = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                ssl=self.ssl,
                decode_responses=True
            )
            
            # Test connection
            await self._redis.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._redis = None
            return False
    
    async def disconnect(self):
        """Disconnect from Redis server."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            logger.info("Disconnected from Redis")
    
    def _make_key(self, key: str) -> str:
        """Create prefixed key."""
        return f"{self.key_prefix}{key}"
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        if not self._redis:
            return None
        
        try:
            full_key = self._make_key(key)
            value = await self._redis.get(full_key)
            
            if value is not None:
                self._hits += 1
                logger.debug(f"Redis cache hit for key: {key}")
                return json.loads(value)
            else:
                self._misses += 1
                logger.debug(f"Redis cache miss for key: {key}")
                return None
                
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in cache with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache (must be JSON serializable)
            ttl: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        if not self._redis:
            return False
        
        try:
            full_key = self._make_key(key)
            serialized = json.dumps(value, default=str)
            
            if ttl:
                await self._redis.setex(full_key, ttl, serialized)
            else:
                await self._redis.set(full_key, serialized)
            
            self._sets += 1
            logger.debug(f"Redis cache set for key: {key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted, False otherwise
        """
        if not self._redis:
            return False
        
        try:
            full_key = self._make_key(key)
            result = await self._redis.delete(full_key)
            
            self._deletes += 1
            logger.debug(f"Redis cache delete for key: {key}")
            return result > 0
            
        except Exception as e:
            logger.error(f"Redis delete error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists, False otherwise
        """
        if not self._redis:
            return False
        
        try:
            full_key = self._make_key(key)
            return await self._redis.exists(full_key) > 0
            
        except Exception as e:
            logger.error(f"Redis exists error for key {key}: {e}")
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern.
        
        Args:
            pattern: Key pattern (supports Redis glob-style patterns)
            
        Returns:
            Number of keys deleted
        """
        if not self._redis:
            return 0
        
        try:
            full_pattern = self._make_key(pattern)
            keys = await self._redis.keys(full_pattern)
            
            if keys:
                await self._redis.delete(*keys)
                self._deletes += len(keys)
                logger.info(f"Redis cache cleared {len(keys)} keys matching pattern: {pattern}")
                return len(keys)
            
            return 0
            
        except Exception as e:
            logger.error(f"Redis clear pattern error for {pattern}: {e}")
            return 0
    
    async def get_stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with statistics
        """
        redis_info = {}
        if self._redis:
            try:
                redis_info = await self._redis.info()
            except Exception as e:
                logger.error(f"Redis info error: {e}")
        
        return {
            'connected': self._redis is not None,
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'hits': self._hits,
            'misses': self._misses,
            'sets': self._sets,
            'deletes': self._deletes,
            'hit_rate': self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0,
            'redis_info': redis_info
        }
    
    async def get_memory_usage(self) -> Optional[int]:
        """
        Get Redis memory usage in bytes.
        
        Returns:
            Memory usage in bytes or None if not available
        """
        if not self._redis:
            return None
        
        try:
            info = await self._redis.info('memory')
            return info.get('used_memory')
        except Exception as e:
            logger.error(f"Redis memory usage error: {e}")
            return None


class RedisEnhancedCacheManager:
    """
    Enhanced cache manager that combines in-memory and Redis caching.
    
    This provides a two-tier caching strategy:
    1. L1: In-memory cache (fastest)
    2. L2: Redis cache (distributed, larger capacity)
    """
    
    def __init__(self, redis_backend: RedisCacheBackend, 
                 l1_max_size: int = 1000, l1_default_ttl: int = 300):
        """
        Initialize enhanced cache manager.
        
        Args:
            redis_backend: Redis cache backend instance
            l1_max_size: Maximum size for L1 in-memory cache
            l1_default_ttl: Default TTL for L1 cache in seconds
        """
        self.redis = redis_backend
        self.l1_max_size = l1_max_size
        self.l1_default_ttl = l1_default_ttl
        
        # L1 in-memory cache (simple dict with TTL)
        self._l1_cache: Dict[str, tuple[Any, float]] = {}
        self._l1_lock = asyncio.Lock()
        
        # Statistics
        self._l1_hits = 0
        self._l1_misses = 0
        self._l1_sets = 0
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache (L1 first, then L2).
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        # Try L1 cache first
        async with self._l1_lock:
            if key in self._l1_cache:
                value, expiry = self._l1_cache[key]
                if time.time() < expiry:
                    self._l1_hits += 1
                    logger.debug(f"L1 cache hit for key: {key}")
                    return value
                else:
                    # L1 entry expired, remove it
                    del self._l1_cache[key]
        
        self._l1_misses += 1
        
        # Try L2 (Redis) cache
        value = await self.redis.get(key)
        
        # If found in L2, populate L1
        if value is not None:
            await self._set_l1(key, value)
        
        return value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in both L1 and L2 caches.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            
        Returns:
            True if successful
        """
        # Set in L1
        await self._set_l1(key, value, ttl)
        
        # Set in L2 (Redis)
        # Use L1 TTL if not specified, but cap at 1 hour for Redis
        redis_ttl = ttl or self.l1_default_ttl
        redis_ttl = min(redis_ttl, 3600)  # Cap at 1 hour for Redis
        
        return await self.redis.set(key, value, redis_ttl)
    
    async def _set_l1(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in L1 cache."""
        async with self._l1_lock:
            # Enforce max size with simple eviction (remove oldest)
            if len(self._l1_cache) >= self.l1_max_size:
                # Remove oldest entry
                oldest_key = next(iter(self._l1_cache))
                del self._l1_cache[oldest_key]
            
            # Set expiry
            expiry = time.time() + (ttl or self.l1_default_ttl)
            self._l1_cache[key] = (value, expiry)
            self._l1_sets += 1
    
    async def delete(self, key: str) -> bool:
        """
        Delete value from both caches.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted
        """
        # Delete from L1
        async with self._l1_lock:
            if key in self._l1_cache:
                del self._l1_cache[key]
        
        # Delete from L2
        return await self.redis.delete(key)
    
    async def clear_all(self, pattern: str = "*") -> int:
        """
        Clear all cached entries matching pattern.
        
        Args:
            pattern: Key pattern
            
        Returns:
            Number of entries cleared
        """
        # Clear L1
        async with self._l1_lock:
            if pattern == "*":
                count = len(self._l1_cache)
                self._l1_cache.clear()
            else:
                # Simple pattern matching for L1
                keys_to_delete = [k for k in self._l1_cache.keys() if pattern in k]
                count = len(keys_to_delete)
                for key in keys_to_delete:
                    del self._l1_cache[key]
        
        # Clear L2
        redis_count = await self.redis.clear_pattern(pattern)
        
        return count + redis_count
    
    async def get_stats(self) -> dict:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with statistics for both cache levels
        """
        redis_stats = await self.redis.get_stats()
        
        total_hits = self._l1_hits + redis_stats['hits']
        total_misses = self._l1_misses + redis_stats['misses']
        total_requests = total_hits + total_misses
        
        return {
            'l1': {
                'hits': self._l1_hits,
                'misses': self._l1_misses,
                'sets': self._l1_sets,
                'size': len(self._l1_cache),
                'max_size': self.l1_max_size,
                'hit_rate': self._l1_hits / (self._l1_hits + self._l1_misses) if (self._l1_hits + self._l1_misses) > 0 else 0
            },
            'l2': redis_stats,
            'overall': {
                'hits': total_hits,
                'misses': total_misses,
                'hit_rate': total_hits / total_requests if total_requests > 0 else 0
            }
        }


# Global Redis cache instance (initialized on demand)
_redis_cache: Optional[RedisEnhancedCacheManager] = None

async def get_redis_cache() -> Optional[RedisEnhancedCacheManager]:
    """
    Get or create Redis cache instance.
    
    Returns:
        RedisEnhancedCacheManager instance or None if Redis not available
    """
    global _redis_cache
    
    if _redis_cache is not None:
        return _redis_cache
    
    # Try to create Redis cache
    try:
        redis_backend = RedisCacheBackend(
            host='localhost',
            port=6379,
            db=0,
            key_prefix='twitter_scrapper:'
        )
        
        if await redis_backend.connect():
            _redis_cache = RedisEnhancedCacheManager(redis_backend)
            logger.info("Redis cache initialized successfully")
            return _redis_cache
        else:
            logger.warning("Failed to connect to Redis, cache will use in-memory only")
            return None
            
    except Exception as e:
        logger.warning(f"Redis not available: {e}. Cache will use in-memory only")
        return None