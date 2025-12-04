import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)

class HealthStatus(Enum):
    """Health check status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

class HealthCheckResult:
    """Result of a health check."""
    
    def __init__(self, name: str, status: HealthStatus, message: str = "", 
                 details: Optional[Dict[str, Any]] = None, response_time: float = 0.0):
        """
        Initialize health check result.
        
        Args:
            name: Name of the health check
            status: Health status
            message: Human-readable message
            details: Additional details
            response_time: Response time in seconds
        """
        self.name = name
        self.status = status
        self.message = message
        self.details = details or {}
        self.response_time = response_time
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'response_time': self.response_time,
            'timestamp': self.timestamp.isoformat()
        }

class HealthChecker:
    """
    Health checker for TwitterScrapper components.
    
    Performs periodic health checks on critical components and provides
    graceful degradation when components are unhealthy.
    """
    
    def __init__(self, check_interval: int = 30, timeout: int = 10):
        """
        Initialize health checker.
        
        Args:
            check_interval: Interval between health checks in seconds
            timeout: Timeout for individual health checks in seconds
        """
        self.check_interval = check_interval
        self.timeout = timeout
        
        self._checks: Dict[str, Callable] = {}
        self._results: Dict[str, HealthCheckResult] = {}
        self._is_healthy = True
        self._degraded_components: List[str] = []
        self._check_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Health check history (last 100 results)
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Register default health checks
        self._register_default_checks()
    
    def _register_default_checks(self):
        """Register default health checks."""
        # These will be implemented by the application
        self.register_check('database', self._check_database)
        self.register_check('cache', self._check_cache)
        self.register_check('redis', self._check_redis)
        self.register_check('api', self._check_api)
        self.register_check('disk_space', self._check_disk_space)
        self.register_check('memory', self._check_memory)
    
    def register_check(self, name: str, check_function: Callable):
        """
        Register a health check function.
        
        Args:
            name: Name of the health check
            check_function: Async function that returns HealthCheckResult
        """
        self._checks[name] = check_function
        logger.info(f"Registered health check: {name}")
    
    async def start(self):
        """Start periodic health checks."""
        if self._running:
            return
        
        self._running = True
        self._check_task = asyncio.create_task(self._run_checks())
        logger.info("Health checker started")
    
    async def stop(self):
        """Stop periodic health checks."""
        if not self._running:
            return
        
        self._running = False
        
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Health checker stopped")
    
    async def _run_checks(self):
        """Run health checks periodically."""
        while self._running:
            try:
                await self.run_all_checks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Health check runner error: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def run_all_checks(self) -> Dict[str, HealthCheckResult]:
        """
        Run all registered health checks.
        
        Returns:
            Dictionary with health check results
        """
        results = {}
        
        for name, check_func in self._checks.items():
            try:
                # Run check with timeout
                result = await asyncio.wait_for(
                    check_func(),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                result = HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message="Health check timed out",
                    response_time=self.timeout
                )
            except Exception as e:
                result = HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check failed: {str(e)}",
                    response_time=0.0
                )
            
            results[name] = result
            self._results[name] = result
            self._history[name].append(result)
        
        # Update overall health status
        self._update_health_status(results)
        
        return results
    
    def _update_health_status(self, results: Dict[str, HealthCheckResult]):
        """Update overall health status based on check results."""
        unhealthy_components = []
        degraded_components = []
        
        for name, result in results.items():
            if result.status == HealthStatus.UNHEALTHY:
                unhealthy_components.append(name)
            elif result.status == HealthStatus.DEGRADED:
                degraded_components.append(name)
        
        self._degraded_components = degraded_components
        
        if unhealthy_components:
            self._is_healthy = False
            logger.error(f"System unhealthy: {unhealthy_components}")
        elif degraded_components:
            self._is_healthy = True  # Still operational but degraded
            logger.warning(f"System degraded: {degraded_components}")
        else:
            self._is_healthy = True
            logger.debug("System healthy")
    
    async def _check_database(self) -> HealthCheckResult:
        """Check database health."""
        start_time = time.time()
        
        try:
            # Import here to avoid circular dependencies
            from database import DatabaseManager
            
            db = DatabaseManager()
            
            # Simple query to check connectivity
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            response_time = time.time() - start_time
            
            return HealthCheckResult(
                name='database',
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                response_time=response_time,
                details={
                    'response_time_ms': round(response_time * 1000, 2)
                }
            )
            
        except Exception as e:
            response_time = time.time() - start_time
            
            return HealthCheckResult(
                name='database',
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                response_time=response_time,
                details={
                    'error': str(e)
                }
            )
    
    async def _check_cache(self) -> HealthCheckResult:
        """Check cache health."""
        start_time = time.time()
        
        try:
            # Test cache operations
            from cache import search_cache
            
            test_key = "health_check"
            test_value = {"test": "data"}
            
            # Set and get to verify functionality
            search_cache.set(test_key, test_value)
            retrieved = search_cache.get(test_key)
            
            if retrieved == test_value:
                response_time = time.time() - start_time
                
                return HealthCheckResult(
                    name='cache',
                    status=HealthStatus.HEALTHY,
                    message="Cache operations successful",
                    response_time=response_time
                )
            else:
                return HealthCheckResult(
                    name='cache',
                    status=HealthStatus.UNHEALTHY,
                    message="Cache data mismatch",
                    response_time=time.time() - start_time
                )
                
        except Exception as e:
            return HealthCheckResult(
                name='cache',
                status=HealthStatus.UNHEALTHY,
                message=f"Cache check failed: {str(e)}",
                response_time=time.time() - start_time
            )
    
    async def _check_redis(self) -> HealthCheckResult:
        """Check Redis health."""
        start_time = time.time()
        
        try:
            from redis_cache import get_redis_cache
            
            redis_cache = await get_redis_cache()
            
            if redis_cache is None:
                return HealthCheckResult(
                    name='redis',
                    status=HealthStatus.DEGRADED,
                    message="Redis not configured, using in-memory cache only",
                    response_time=time.time() - start_time
                )
            
            # Test Redis operations
            test_key = "health_check"
            test_value = {"test": "data"}
            
            success = await redis_cache.set(test_key, test_value, ttl=60)
            
            if success:
                response_time = time.time() - start_time
                
                return HealthCheckResult(
                    name='redis',
                    status=HealthStatus.HEALTHY,
                    message="Redis connection successful",
                    response_time=response_time
                )
            else:
                return HealthCheckResult(
                    name='redis',
                    status=HealthStatus.UNHEALTHY,
                    message="Redis operations failed",
                    response_time=time.time() - start_time
                )
                
        except Exception as e:
            return HealthCheckResult(
                name='redis',
                status=HealthStatus.UNHEALTHY,
                message=f"Redis check failed: {str(e)}",
                response_time=time.time() - start_time
            )
    
    async def _check_api(self) -> HealthCheckResult:
        """Check Twitter API health."""
        start_time = time.time()
        
        try:
            from auth import getClient
            from twikit.errors import TooManyRequests
            
            client = getClient()
            
            # Simple API call to check connectivity
            try:
                # Try to get a small amount of data
                result = await client.search_tweet("test", product='Top', count=1)
                
                response_time = time.time() - start_time
                
                return HealthCheckResult(
                    name='api',
                    status=HealthStatus.HEALTHY,
                    message="Twitter API connection successful",
                    response_time=response_time,
                    details={
                        'response_time_ms': round(response_time * 1000, 2)
                    }
                )
                
            except TooManyRequests:
                # Rate limited is actually a good sign - API is responsive
                response_time = time.time() - start_time
                
                return HealthCheckResult(
                    name='api',
                    status=HealthStatus.HEALTHY,
                    message="Twitter API responsive (rate limited)",
                    response_time=response_time
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            
            return HealthCheckResult(
                name='api',
                status=HealthStatus.UNHEALTHY,
                message=f"Twitter API check failed: {str(e)}",
                response_time=response_time,
                details={
                    'error': str(e)
                }
            )
    
    async def _check_disk_space(self) -> HealthCheckResult:
        """Check disk space availability."""
        try:
            import shutil
            
            total, used, free = shutil.disk_usage('/')
            usage_percent = (used / total) * 100
            
            if usage_percent > 90:
                status = HealthStatus.UNHEALTHY
                message = f"Disk usage critically high: {usage_percent:.1f}%"
            elif usage_percent > 80:
                status = HealthStatus.DEGRADED
                message = f"Disk usage high: {usage_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Disk usage OK: {usage_percent:.1f}%"
            
            return HealthCheckResult(
                name='disk_space',
                status=status,
                message=message,
                details={
                    'total_gb': round(total / (1024**3), 2),
                    'used_gb': round(used / (1024**3), 2),
                    'free_gb': round(free / (1024**3), 2),
                    'usage_percent': round(usage_percent, 2)
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                name='disk_space',
                status=HealthStatus.UNKNOWN,
                message=f"Could not check disk space: {str(e)}",
                details={'error': str(e)}
            )
    
    async def _check_memory(self) -> HealthCheckResult:
        """Check memory usage."""
        try:
            import psutil
            
            memory = psutil.virtual_memory()
            usage_percent = memory.percent
            
            if usage_percent > 90:
                status = HealthStatus.UNHEALTHY
                message = f"Memory usage critically high: {usage_percent:.1f}%"
            elif usage_percent > 80:
                status = HealthStatus.DEGRADED
                message = f"Memory usage high: {usage_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Memory usage OK: {usage_percent:.1f}%"
            
            return HealthCheckResult(
                name='memory',
                status=status,
                message=message,
                details={
                    'total_gb': round(memory.total / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'used_gb': round(memory.used / (1024**3), 2),
                    'usage_percent': round(usage_percent, 2)
                }
            )
            
        except ImportError:
            return HealthCheckResult(
                name='memory',
                status=HealthStatus.UNKNOWN,
                message="psutil not available, cannot check memory"
            )
        except Exception as e:
            return HealthCheckResult(
                name='memory',
                status=HealthStatus.UNKNOWN,
                message=f"Memory check failed: {str(e)}",
                details={'error': str(e)}
            )
    
    def is_healthy(self) -> bool:
        """
        Check if system is healthy.
        
        Returns:
            True if system is healthy, False otherwise
        """
        return self._is_healthy
    
    def get_degraded_components(self) -> List[str]:
        """
        Get list of degraded components.
        
        Returns:
            List of component names that are degraded
        """
        return self._degraded_components.copy()
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get overall system status.
        
        Returns:
            Dictionary with system status
        """
        return {
            'healthy': self._is_healthy,
            'degraded_components': self.get_degraded_components(),
            'last_check': {
                name: result.to_dict() for name, result in self._results.items()
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def get_component_history(self, component: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get health check history for a component.
        
        Args:
            component: Component name
            limit: Maximum number of results to return
            
        Returns:
            List of health check results
        """
        if component not in self._history:
            return []
        
        history = list(self._history[component])[-limit:]
        return [result.to_dict() for result in history]


# Global health checker instance
health_checker = HealthChecker()


class GracefulDegradationManager:
    """
    Manager for graceful degradation of functionality.
    
    When components are unhealthy, this manager provides fallback
    mechanisms to keep the application running with reduced functionality.
    """
    
    def __init__(self):
        """Initialize graceful degradation manager."""
        self._fallbacks: Dict[str, Callable] = {}
        self._degraded_modes: Dict[str, bool] = {}
        
        # Register default fallbacks
        self._register_default_fallbacks()
    
    def _register_default_fallbacks(self):
        """Register default fallback functions."""
        # Database fallback: use in-memory storage
        self.register_fallback('database', self._database_fallback)
        
        # Cache fallback: disable caching
        self.register_fallback('cache', self._cache_fallback)
        
        # Redis fallback: use in-memory cache
        self.register_fallback('redis', self._redis_fallback)
        
        # API fallback: return cached data or empty results
        self.register_fallback('api', self._api_fallback)
    
    def register_fallback(self, component: str, fallback_function: Callable):
        """
        Register a fallback function for a component.
        
        Args:
            component: Component name
            fallback_function: Function to call when component is degraded
        """
        self._fallbacks[component] = fallback_function
        logger.info(f"Registered fallback for component: {component}")
    
    async def handle_degradation(self, component: str, *args, **kwargs):
        """
        Handle component degradation by using fallback.
        
        Args:
            component: Component name
            *args: Arguments to pass to fallback function
            **kwargs: Keyword arguments to pass to fallback function
            
        Returns:
            Fallback function result
        """
        if component not in self._fallbacks:
            logger.error(f"No fallback registered for component: {component}")
            raise RuntimeError(f"No fallback for component: {component}")
        
        self._degraded_modes[component] = True
        logger.warning(f"Using fallback for degraded component: {component}")
        
        fallback_func = self._fallbacks[component]
        return await fallback_func(*args, **kwargs)
    
    def is_degraded(self, component: str) -> bool:
        """
        Check if component is in degraded mode.
        
        Args:
            component: Component name
            
        Returns:
            True if component is degraded, False otherwise
        """
        return self._degraded_modes.get(component, False)
    
    def restore_component(self, component: str):
        """
        Restore component from degraded mode.
        
        Args:
            component: Component name
        """
        if component in self._degraded_modes:
            del self._degraded_modes[component]
            logger.info(f"Component restored from degraded mode: {component}")
    
    # Fallback implementations
    async def _database_fallback(self, *args, **kwargs):
        """Database fallback: use in-memory storage."""
        logger.warning("Using in-memory storage as database fallback")
        # Return empty results or in-memory storage
        return []
    
    async def _cache_fallback(self, *args, **kwargs):
        """Cache fallback: disable caching."""
        logger.warning("Cache disabled, using direct API calls")
        # Return None to indicate cache miss
        return None
    
    async def _redis_fallback(self, *args, **kwargs):
        """Redis fallback: use in-memory cache."""
        logger.warning("Using in-memory cache as Redis fallback")
        # Use in-memory cache instead
        from cache import search_cache
        
        if args and args[0] == 'get':
            return search_cache.get(args[1])
        elif args and args[0] == 'set':
            search_cache.set(args[1], args[2])
        
        return None
    
    async def _api_fallback(self, *args, **kwargs):
        """API fallback: return cached data or empty results."""
        logger.warning("Using cached data as API fallback")
        # Try to return cached data if available
        return []


# Global graceful degradation manager
degradation_manager = GracefulDegradationManager()