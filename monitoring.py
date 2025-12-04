import asyncio
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
from collections import defaultdict, deque

try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Histogram = Gauge = None

logger = logging.getLogger(__name__)

class PerformanceMetrics:
    """
    Performance metrics collector for the TwitterScrapper.
    
    Tracks key performance indicators including API calls, database operations,
    cache performance, sentiment analysis, and system resources.
    """
    
    def __init__(self, enable_prometheus: bool = False):
        """
        Initialize performance metrics collector.
        
        Args:
            enable_prometheus: Enable Prometheus metrics export
        """
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE
        
        # API call metrics
        self.api_calls_total = 0
        self.api_calls_by_endpoint: Dict[str, int] = defaultdict(int)
        self.api_calls_by_status: Dict[str, int] = defaultdict(int)
        self.api_response_times: List[float] = deque(maxlen=1000)
        
        # Database metrics
        self.db_queries_total = 0
        self.db_queries_by_type: Dict[str, int] = defaultdict(int)
        self.db_query_times: List[float] = deque(maxlen=1000)
        self.db_connections_active = 0
        self.db_connections_total = 0
        
        # Cache metrics
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evictions = 0
        
        # Sentiment analysis metrics
        self.sentiment_analyses_total = 0
        self.sentiment_by_method: Dict[str, int] = defaultdict(int)
        self.sentiment_analysis_times: List[float] = deque(maxlen=1000)
        
        # Tweet processing metrics
        self.tweets_processed_total = 0
        self.tweets_by_query: Dict[str, int] = defaultdict(int)
        self.tweets_by_language: Dict[str, int] = defaultdict(int)
        self.tweets_by_sentiment: Dict[str, int] = defaultdict(int)
        
        # Rate limiting metrics
        self.rate_limit_hits = 0
        self.rate_limit_misses = 0
        
        # Error metrics
        self.errors_total = 0
        self.errors_by_type: Dict[str, int] = defaultdict(int)
        
        # System metrics
        self.cpu_usage: List[float] = deque(maxlen=100)
        self.memory_usage: List[float] = deque(maxlen=100)
        self.disk_usage: Optional[float] = None
        
        # Prometheus metrics (if enabled)
        if self.enable_prometheus:
            self._init_prometheus_metrics()
        
        # Start background metrics collection
        self._collecting = False
        self._collection_task: Optional[asyncio.Task] = None
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics."""
        if not PROMETHEUS_AVAILABLE:
            return
        
        # API metrics
        self.prom_api_calls = Counter(
            'twitter_api_calls_total',
            'Total number of API calls',
            ['endpoint', 'status']
        )
        
        self.prom_api_response_time = Histogram(
            'twitter_api_response_time_seconds',
            'API response time in seconds',
            ['endpoint']
        )
        
        # Database metrics
        self.prom_db_queries = Counter(
            'twitter_db_queries_total',
            'Total number of database queries',
            ['query_type']
        )
        
        self.prom_db_query_time = Histogram(
            'twitter_db_query_time_seconds',
            'Database query time in seconds',
            ['query_type']
        )
        
        self.prom_db_connections = Gauge(
            'twitter_db_connections_active',
            'Number of active database connections'
        )
        
        # Cache metrics
        self.prom_cache_hits = Counter(
            'twitter_cache_hits_total',
            'Total number of cache hits'
        )
        
        self.prom_cache_misses = Counter(
            'twitter_cache_misses_total',
            'Total number of cache misses'
        )
        
        # Sentiment analysis metrics
        self.prom_sentiment_analyses = Counter(
            'twitter_sentiment_analyses_total',
            'Total number of sentiment analyses',
            ['method']
        )
        
        self.prom_sentiment_analysis_time = Histogram(
            'twitter_sentiment_analysis_time_seconds',
            'Sentiment analysis time in seconds',
            ['method']
        )
        
        # Tweet processing metrics
        self.prom_tweets_processed = Counter(
            'twitter_tweets_processed_total',
            'Total number of tweets processed',
            ['query', 'language', 'sentiment']
        )
        
        # Rate limiting metrics
        self.prom_rate_limit_hits = Counter(
            'twitter_rate_limit_hits_total',
            'Total number of rate limit hits'
        )
        
        # Error metrics
        self.prom_errors = Counter(
            'twitter_errors_total',
            'Total number of errors',
            ['error_type']
        )
    
    async def start_collection(self):
        """Start background metrics collection."""
        if self._collecting:
            return
        
        self._collecting = True
        self._collection_task = asyncio.create_task(self._collect_system_metrics())
        logger.info("Started background metrics collection")
    
    async def stop_collection(self):
        """Stop background metrics collection."""
        if not self._collecting:
            return
        
        self._collecting = False
        
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Stopped background metrics collection")
    
    async def _collect_system_metrics(self):
        """Collect system metrics in background."""
        try:
            import psutil
            
            while self._collecting:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                self.cpu_usage.append(cpu_percent)
                
                # Memory usage
                memory = psutil.virtual_memory()
                self.memory_usage.append(memory.percent)
                
                # Disk usage
                disk = psutil.disk_usage('/')
                self.disk_usage = (disk.used / disk.total) * 100
                
                # Wait before next collection
                await asyncio.sleep(30)  # Collect every 30 seconds
                
        except ImportError:
            logger.warning("psutil not available, system metrics collection disabled")
        except Exception as e:
            logger.error(f"System metrics collection error: {e}")
    
    def record_api_call(self, endpoint: str, status: str, response_time: float):
        """
        Record API call metrics.
        
        Args:
            endpoint: API endpoint name
            status: Response status (success, error, rate_limited)
            response_time: Response time in seconds
        """
        self.api_calls_total += 1
        self.api_calls_by_endpoint[endpoint] += 1
        self.api_calls_by_status[status] += 1
        self.api_response_times.append(response_time)
        
        if self.enable_prometheus:
            self.prom_api_calls.labels(endpoint=endpoint, status=status).inc()
            self.prom_api_response_time.labels(endpoint=endpoint).observe(response_time)
    
    def record_db_query(self, query_type: str, query_time: float):
        """
        Record database query metrics.
        
        Args:
            query_type: Type of query (select, insert, update, delete)
            query_time: Query execution time in seconds
        """
        self.db_queries_total += 1
        self.db_queries_by_type[query_type] += 1
        self.db_query_times.append(query_time)
        
        if self.enable_prometheus:
            self.prom_db_queries.labels(query_type=query_type).inc()
            self.prom_db_query_time.labels(query_type=query_type).observe(query_time)
    
    def record_db_connection(self, active: bool):
        """
        Record database connection metrics.
        
        Args:
            active: True for new connection, False for closed connection
        """
        if active:
            self.db_connections_active += 1
            self.db_connections_total += 1
        else:
            self.db_connections_active = max(0, self.db_connections_active - 1)
        
        if self.enable_prometheus:
            self.prom_db_connections.set(self.db_connections_active)
    
    def record_cache_hit(self):
        """Record cache hit."""
        self.cache_hits += 1
        
        if self.enable_prometheus:
            self.prom_cache_hits.inc()
    
    def record_cache_miss(self):
        """Record cache miss."""
        self.cache_misses += 1
        
        if self.enable_prometheus:
            self.prom_cache_misses.inc()
    
    def record_cache_eviction(self):
        """Record cache eviction."""
        self.cache_evictions += 1
    
    def record_sentiment_analysis(self, method: str, analysis_time: float):
        """
        Record sentiment analysis metrics.
        
        Args:
            method: Sentiment analysis method (vader, textblob)
            analysis_time: Analysis time in seconds
        """
        self.sentiment_analyses_total += 1
        self.sentiment_by_method[method] += 1
        self.sentiment_analysis_times.append(analysis_time)
        
        if self.enable_prometheus:
            self.prom_sentiment_analyses.labels(method=method).inc()
            self.prom_sentiment_analysis_time.labels(method=method).observe(analysis_time)
    
    def record_tweet_processed(self, query: str, language: str, sentiment: str):
        """
        Record tweet processing metrics.
        
        Args:
            query: Search query
            language: Tweet language
            sentiment: Sentiment label
        """
        self.tweets_processed_total += 1
        self.tweets_by_query[query] += 1
        self.tweets_by_language[language] += 1
        self.tweets_by_sentiment[sentiment] += 1
        
        if self.enable_prometheus:
            self.prom_tweets_processed.labels(
                query=query,
                language=language,
                sentiment=sentiment
            ).inc()
    
    def record_rate_limit_hit(self):
        """Record rate limit hit."""
        self.rate_limit_hits += 1
        
        if self.enable_prometheus:
            self.prom_rate_limit_hits.inc()
    
    def record_rate_limit_miss(self):
        """Record rate limit miss (request allowed)."""
        self.rate_limit_misses += 1
    
    def record_error(self, error_type: str):
        """
        Record error metrics.
        
        Args:
            error_type: Type of error (api, database, cache, etc.)
        """
        self.errors_total += 1
        self.errors_by_type[error_type] += 1
        
        if self.enable_prometheus:
            self.prom_errors.labels(error_type=error_type).inc()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get performance summary.
        
        Returns:
            Dictionary with performance summary
        """
        # Calculate averages
        avg_api_response_time = (
            sum(self.api_response_times) / len(self.api_response_times)
            if self.api_response_times else 0
        )
        
        avg_db_query_time = (
            sum(self.db_query_times) / len(self.db_query_times)
            if self.db_query_times else 0
        )
        
        avg_sentiment_time = (
            sum(self.sentiment_analysis_times) / len(self.sentiment_analysis_times)
            if self.sentiment_analysis_times else 0
        )
        
        avg_cpu = (
            sum(self.cpu_usage) / len(self.cpu_usage)
            if self.cpu_usage else 0
        )
        
        avg_memory = (
            sum(self.memory_usage) / len(self.memory_usage)
            if self.memory_usage else 0
        )
        
        # Cache hit rate
        cache_hit_rate = (
            self.cache_hits / (self.cache_hits + self.cache_misses)
            if (self.cache_hits + self.cache_misses) > 0 else 0
        )
        
        return {
            'timestamp': datetime.now().isoformat(),
            'uptime': self._get_uptime(),
            'api': {
                'total_calls': self.api_calls_total,
                'calls_by_endpoint': dict(self.api_calls_by_endpoint),
                'calls_by_status': dict(self.api_calls_by_status),
                'avg_response_time': avg_api_response_time,
                'rate_limit_hits': self.rate_limit_hits,
                'rate_limit_misses': self.rate_limit_misses
            },
            'database': {
                'total_queries': self.db_queries_total,
                'queries_by_type': dict(self.db_queries_by_type),
                'avg_query_time': avg_db_query_time,
                'active_connections': self.db_connections_active,
                'total_connections': self.db_connections_total
            },
            'cache': {
                'hits': self.cache_hits,
                'misses': self.cache_misses,
                'evictions': self.cache_evictions,
                'hit_rate': cache_hit_rate
            },
            'sentiment': {
                'total_analyses': self.sentiment_analyses_total,
                'by_method': dict(self.sentiment_by_method),
                'avg_analysis_time': avg_sentiment_time
            },
            'tweets': {
                'total_processed': self.tweets_processed_total,
                'by_query': dict(self.tweets_by_query),
                'by_language': dict(self.tweets_by_language),
                'by_sentiment': dict(self.tweets_by_sentiment)
            },
            'errors': {
                'total_errors': self.errors_total,
                'by_type': dict(self.errors_by_type)
            },
            'system': {
                'avg_cpu_usage': avg_cpu,
                'avg_memory_usage': avg_memory,
                'disk_usage': self.disk_usage
            }
        }
    
    def _get_uptime(self) -> float:
        """Get application uptime in seconds."""
        # This would typically track actual uptime
        # For now, return 0 as placeholder
        return 0.0
    
    def get_prometheus_metrics(self) -> Optional[str]:
        """
        Get Prometheus metrics in text format.
        
        Returns:
            Prometheus metrics string or None if not enabled
        """
        if not self.enable_prometheus or not PROMETHEUS_AVAILABLE:
            return None
        
        return generate_latest().decode('utf-8')
    
    def export_to_json(self, filename: str):
        """
        Export metrics to JSON file.
        
        Args:
            filename: Output filename
        """
        summary = self.get_summary()
        
        with open(filename, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Metrics exported to {filename}")


# Global metrics instance
metrics = PerformanceMetrics(enable_prometheus=True)