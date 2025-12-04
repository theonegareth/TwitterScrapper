import asyncio
import time
from typing import Optional
from collections import deque
import logging

logger = logging.getLogger(__name__)

class TokenBucketRateLimiter:
    """
    Token Bucket rate limiter for API calls.
    
    This implementation uses the token bucket algorithm to control the rate
    of API requests, preventing rate limit violations and ensuring smooth
    operation even under heavy load.
    """
    
    def __init__(self, capacity: int, refill_rate: float, refill_interval: float = 1.0):
        """
        Initialize the token bucket rate limiter.
        
        Args:
            capacity: Maximum number of tokens in the bucket
            refill_rate: Number of tokens to add per refill_interval
            refill_interval: Time interval in seconds for token refill
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.refill_interval = refill_interval
        
        self._tokens = capacity
        self._last_refill = time.time()
        self._lock = asyncio.Lock()
        
        # Statistics
        self._requests_allowed = 0
        self._requests_denied = 0
        self._total_wait_time = 0.0
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
            
        Returns:
            True if tokens were acquired, False if rate limit exceeded
        """
        async with self._lock:
            await self._refill_tokens()
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._requests_allowed += 1
                logger.debug(f"Rate limiter: acquired {tokens} tokens, remaining: {self._tokens}")
                return True
            else:
                self._requests_denied += 1
                logger.warning(f"Rate limiter: insufficient tokens for {tokens} tokens request")
                return False
    
    async def acquire_with_wait(self, tokens: int = 1, max_wait: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket, waiting if necessary.
        
        Args:
            tokens: Number of tokens to acquire
            max_wait: Maximum time to wait in seconds (None for unlimited)
            
        Returns:
            True if tokens were acquired, False if wait timeout or rate limit exceeded
        """
        start_time = time.time()
        wait_time = 0.0
        
        while True:
            async with self._lock:
                await self._refill_tokens()
                
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    self._requests_allowed += 1
                    self._total_wait_time += wait_time
                    logger.debug(f"Rate limiter: acquired {tokens} tokens after {wait_time:.2f}s wait")
                    return True
                
                # Calculate time until enough tokens are available
                tokens_needed = tokens - self._tokens
                time_needed = (tokens_needed / self.refill_rate) * self.refill_interval
                
                # Check max_wait constraint
                if max_wait is not None:
                    if wait_time + time_needed > max_wait:
                        self._requests_denied += 1
                        logger.warning(f"Rate limiter: wait timeout after {wait_time:.2f}s")
                        return False
            
            # Wait before retrying
            await asyncio.sleep(min(time_needed, 0.1))
            wait_time = time.time() - start_time
    
    async def _refill_tokens(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        
        if elapsed >= self.refill_interval:
            # Calculate tokens to add
            intervals_passed = elapsed / self.refill_interval
            tokens_to_add = intervals_passed * self.refill_rate
            
            # Update token count
            self._tokens = min(self.capacity, self._tokens + tokens_to_add)
            self._last_refill = now
            
            logger.debug(f"Rate limiter: refilled {tokens_to_add:.2f} tokens, total: {self._tokens:.2f}")
    
    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'capacity': self.capacity,
            'refill_rate': self.refill_rate,
            'refill_interval': self.refill_interval,
            'current_tokens': self._tokens,
            'requests_allowed': self._requests_allowed,
            'requests_denied': self._requests_denied,
            'total_wait_time': self._total_wait_time,
            'denial_rate': self._requests_denied / (self._requests_allowed + self._requests_denied) if (self._requests_allowed + self._requests_denied) > 0 else 0
        }
    
    def reset_stats(self):
        """Reset statistics counters."""
        self._requests_allowed = 0
        self._requests_denied = 0
        self._total_wait_time = 0.0


class TwitterRateLimiter:
    """
    Twitter-specific rate limiter with multiple buckets for different endpoints.
    """
    
    def __init__(self):
        # Twitter API v2 rate limits (adjust based on your access level)
        # Standard tier: 300 requests per 15 minutes for most endpoints
        self.search_limiter = TokenBucketRateLimiter(
            capacity=300,
            refill_rate=300/900,  # 300 tokens per 15 minutes (900 seconds)
            refill_interval=1.0
        )
        
        # User timeline: 900 requests per 15 minutes
        self.timeline_limiter = TokenBucketRateLimiter(
            capacity=900,
            refill_rate=900/900,
            refill_interval=1.0
        )
        
        # Posting tweets: 300 requests per 3 hours
        self.post_limiter = TokenBucketRateLimiter(
            capacity=300,
            refill_rate=300/10800,  # 300 tokens per 3 hours (10800 seconds)
            refill_interval=1.0
        )
        
        # App-level rate limit (overall)
        self.app_limiter = TokenBucketRateLimiter(
            capacity=500,
            refill_rate=500/900,
            refill_interval=1.0
        )
    
    async def acquire_for_search(self) -> bool:
        """Acquire tokens for search endpoint."""
        # Check both search-specific and app-level limits
        search_ok = await self.search_limiter.acquire()
        app_ok = await self.app_limiter.acquire()
        
        return search_ok and app_ok
    
    async def acquire_for_timeline(self) -> bool:
        """Acquire tokens for timeline endpoint."""
        timeline_ok = await self.timeline_limiter.acquire()
        app_ok = await self.app_limiter.acquire()
        
        return timeline_ok and app_ok
    
    async def acquire_for_post(self) -> bool:
        """Acquire tokens for posting tweets."""
        post_ok = await self.post_limiter.acquire()
        app_ok = await self.app_limiter.acquire()
        
        return post_ok and app_ok
    
    def get_all_stats(self) -> dict:
        """Get statistics for all limiters."""
        return {
            'search': self.search_limiter.get_stats(),
            'timeline': self.timeline_limiter.get_stats(),
            'post': self.post_limiter.get_stats(),
            'app': self.app_limiter.get_stats()
        }


# Global rate limiter instance
rate_limiter = TwitterRateLimiter()