import asyncio
import time
import logging
import json
import csv
import io
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
import statistics
import sys
from pathlib import Path

# Import our modules
from rate_limiter import rate_limiter, TokenBucketRateLimiter
from redis_cache import get_redis_cache, RedisEnhancedCacheManager
from compression import compression_manager, tweet_compressor
from monitoring import metrics
from health_checks import health_checker
from streaming_export import streaming_exporter, AsyncDataSource
from parallel_sentiment import get_parallel_processor
from database_sharding import get_shard_manager

logger = logging.getLogger(__name__)

class PerformanceTestSuite:
    """
    Automated performance testing suite for TwitterScrapper optimizations.
    
    This suite tests all performance-related features and provides benchmarks
    to ensure optimizations are working correctly.
    """
    
    def __init__(self, output_dir: str = "performance_results"):
        """
        Initialize performance test suite.
        
        Args:
            output_dir: Directory to save test results
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Test results storage
        self.results: Dict[str, Any] = {
            'timestamp': datetime.now().isoformat(),
            'tests': {}
        }
        
        # Sample data for testing
        self.sample_tweets = self._generate_sample_tweets(1000)
        self.large_dataset = self._generate_sample_tweets(10000)
        
        logger.info(f"Initialized performance test suite with {len(self.sample_tweets)} sample tweets")
    
    def _generate_sample_tweets(self, count: int) -> List[Dict[str, Any]]:
        """Generate sample tweet data for testing."""
        sample_texts = [
            "I absolutely love this product! It's amazing! 😊",
            "This is terrible. Worst experience ever. 😠",
            "It's okay, nothing special. 🤷",
            "Great service, would recommend! 👍",
            "Very disappointed with the quality. 👎",
            "Average product, does the job. 😐",
            "Excellent! Exceeded my expectations! ⭐",
            "Poor customer service, not happy. 😞",
            "Good value for money. 🙂",
            "Waste of money, regret buying. 😤"
        ]
        
        tweets = []
        base_time = datetime.now()
        
        for i in range(count):
            text = sample_texts[i % len(sample_texts)]
            sentiment = "positive" if "love" in text or "great" in text or "excellent" in text else \
                       "negative" if "terrible" in text or "poor" in text or "waste" in text else \
                       "neutral"
            
            tweets.append({
                'id': f'tweet_{i}',
                'text': text,
                'created_at': (base_time - timedelta(minutes=i)).isoformat(),
                'username': f'user_{i % 100}',
                'lang': 'en',
                'retweet_count': i % 50,
                'favorite_count': i % 100,
                'sentiment_label': sentiment
            })
        
        return tweets
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """
        Run all performance tests.
        
        Returns:
            Dictionary with all test results
        """
        logger.info("Starting performance test suite")
        
        # Run individual test suites
        await self.test_rate_limiter()
        await self.test_redis_cache()
        await self.test_compression()
        await self.test_streaming_export()
        await self.test_parallel_sentiment()
        await self.test_database_sharding()
        await self.test_health_checks()
        await self.test_monitoring()
        
        # Save results
        self._save_results()
        
        # Generate report
        report = self._generate_report()
        
        logger.info("Performance test suite completed")
        return report
    
    async def test_rate_limiter(self):
        """Test rate limiter performance."""
        logger.info("Testing rate limiter performance")
        
        # Create test rate limiter
        test_limiter = TokenBucketRateLimiter(
            capacity=100,
            refill_rate=10,
            refill_interval=1.0
        )
        
        results = {
            'acquire_time': [],
            'throughput': [],
            'accuracy': []
        }
        
        # Test 1: Single-threaded acquisition time
        start_time = time.time()
        for i in range(50):
            acquire_start = time.time()
            await test_limiter.acquire()
            acquire_time = time.time() - acquire_start
            results['acquire_time'].append(acquire_time)
        
        single_thread_time = time.time() - start_time
        
        # Test 2: Throughput test
        test_limiter = TokenBucketRateLimiter(capacity=1000, refill_rate=100, refill_interval=1.0)
        
        start_time = time.time()
        successful = 0
        
        async def acquire_many():
            nonlocal successful
            for _ in range(100):
                if await test_limiter.acquire():
                    successful += 1
        
        # Run multiple concurrent acquisition tasks
        await asyncio.gather(*[acquire_many() for _ in range(5)])
        
        throughput_time = time.time() - start_time
        results['throughput'] = {
            'successful_acquisitions': successful,
            'time_seconds': throughput_time,
            'acquisitions_per_second': successful / throughput_time if throughput_time > 0 else 0
        }
        
        # Test 3: Accuracy test (check if rate limiting is accurate)
        test_limiter = TokenBucketRateLimiter(capacity=10, refill_rate=10, refill_interval=1.0)
        
        # Empty the bucket
        for _ in range(10):
            await test_limiter.acquire()
        
        # Try to acquire more (should fail)
        failed_acquisitions = 0
        for _ in range(5):
            if not await test_limiter.acquire():
                failed_acquisitions += 1
        
        results['accuracy'] = {
            'expected_failures': 5,
            'actual_failures': failed_acquisitions,
            'accuracy_percent': (failed_acquisitions / 5) * 100 if failed_acquisitions > 0 else 0
        }
        
        # Calculate statistics
        stats = {
            'single_threaded': {
                'total_time': single_thread_time,
                'avg_acquire_time': statistics.mean(results['acquire_time']),
                'min_acquire_time': min(results['acquire_time']),
                'max_acquire_time': max(results['acquire_time'])
            },
            'throughput': results['throughput'],
            'accuracy': results['accuracy']
        }
        
        self.results['tests']['rate_limiter'] = stats
        logger.info(f"Rate limiter test completed: {stats}")
    
    async def test_redis_cache(self):
        """Test Redis cache performance."""
        logger.info("Testing Redis cache performance")
        
        cache = await get_redis_cache()
        
        if cache is None:
            logger.warning("Redis not available, skipping cache performance test")
            self.results['tests']['redis_cache'] = {'status': 'skipped', 'reason': 'Redis not available'}
            return
        
        results = {
            'set_operations': [],
            'get_operations': [],
            'hit_rate': 0
        }
        
        # Test 1: Write performance
        start_time = time.time()
        for i, tweet in enumerate(self.sample_tweets[:100]):
            set_start = time.time()
            await cache.set(f"tweet_{i}", tweet, ttl=300)
            set_time = time.time() - set_start
            results['set_operations'].append(set_time)
        
        write_time = time.time() - start_time
        
        # Test 2: Read performance
        start_time = time.time()
        hits = 0
        for i in range(100):
            get_start = time.time()
            result = await cache.get(f"tweet_{i}")
            get_time = time.time() - get_start
            results['get_operations'].append(get_time)
            
            if result:
                hits += 1
        
        read_time = time.time() - start_time
        
        # Test 3: Cache hit rate
        results['hit_rate'] = (hits / 100) * 100
        
        # Test 4: L1 vs L2 cache performance
        l1_start = time.time()
        for i in range(50):
            await cache._set_l1(f"l1_test_{i}", self.sample_tweets[i % len(self.sample_tweets)])
        l1_write_time = time.time() - l1_start
        
        l1_start = time.time()
        for i in range(50):
            await cache.get(f"l1_test_{i}")
        l1_read_time = time.time() - l1_start
        
        # Calculate statistics
        stats = {
            'write_performance': {
                'total_time': write_time,
                'avg_set_time': statistics.mean(results['set_operations']),
                'sets_per_second': 100 / write_time if write_time > 0 else 0
            },
            'read_performance': {
                'total_time': read_time,
                'avg_get_time': statistics.mean(results['get_operations']),
                'gets_per_second': 100 / read_time if read_time > 0 else 0,
                'hit_rate_percent': results['hit_rate']
            },
            'l1_cache': {
                'write_time': l1_write_time,
                'read_time': l1_read_time,
                'avg_write_time': l1_write_time / 50,
                'avg_read_time': l1_read_time / 50
            }
        }
        
        self.results['tests']['redis_cache'] = stats
        logger.info(f"Redis cache test completed: {stats}")
    
    async def test_compression(self):
        """Test compression performance."""
        logger.info("Testing compression performance")
        
        results = {
            'lz4_compression': [],
            'lz4_decompression': [],
            'zlib_compression': [],
            'zlib_decompression': [],
            'size_reduction': {}
        }
        
        # Test compression on sample tweets
        for tweet in self.sample_tweets[:100]:
            # Original size
            original_data = json.dumps(tweet)
            original_size = len(original_data.encode('utf-8'))
            
            # Test LZ4
            start_time = time.time()
            compressed_lz4 = compression_manager._compress_lz4(original_data)
            compress_time = time.time() - start_time
            
            start_time = time.time()
            decompressed_lz4 = compression_manager._decompress_lz4(compressed_lz4)
            decompress_time = time.time() - start_time
            
            results['lz4_compression'].append(compress_time)
            results['lz4_decompression'].append(decompress_time)
            
            # Test zlib
            start_time = time.time()
            compressed_zlib = compression_manager._compress_zlib(original_data)
            compress_time = time.time() - start_time
            
            start_time = time.time()
            decompressed_zlib = compression_manager._decompress_zlib(compressed_zlib)
            decompress_time = time.time() - start_time
            
            results['zlib_compression'].append(compress_time)
            results['zlib_decompression'].append(decompress_time)
            
            # Calculate size reduction
            if original_size > 0:
                lz4_size = len(compressed_lz4)
                zlib_size = len(compressed_zlib)
                
                results['size_reduction'][tweet['id']] = {
                    'original_size': original_size,
                    'lz4_compressed': lz4_size,
                    'zlib_compressed': zlib_size,
                    'lz4_reduction_percent': ((original_size - lz4_size) / original_size) * 100,
                    'zlib_reduction_percent': ((original_size - zlib_size) / original_size) * 100
                }
        
        # Calculate statistics
        stats = {
            'lz4': {
                'avg_compression_time': statistics.mean(results['lz4_compression']),
                'avg_decompression_time': statistics.mean(results['lz4_decompression']),
                'compression_speed_mb_s': self._calculate_throughput(results['lz4_compression'], self.sample_tweets[:100]),
                'decompression_speed_mb_s': self._calculate_throughput(results['lz4_decompression'], self.sample_tweets[:100])
            },
            'zlib': {
                'avg_compression_time': statistics.mean(results['zlib_compression']),
                'avg_decompression_time': statistics.mean(results['zlib_decompression']),
                'compression_speed_mb_s': self._calculate_throughput(results['zlib_compression'], self.sample_tweets[:100]),
                'decompression_speed_mb_s': self._calculate_throughput(results['zlib_decompression'], self.sample_tweets[:100])
            },
            'size_reduction': {
                'avg_lz4_reduction': statistics.mean([r['lz4_reduction_percent'] for r in results['size_reduction'].values()]),
                'avg_zlib_reduction': statistics.mean([r['zlib_reduction_percent'] for r in results['size_reduction'].values()])
            }
        }
        
        self.results['tests']['compression'] = stats
        logger.info(f"Compression test completed: {stats}")
    
    async def test_streaming_export(self):
        """Test streaming export performance."""
        logger.info("Testing streaming export performance")
        
        results = {
            'json_export': {},
            'csv_export': {},
            'jsonl_export': {}
        }
        
        # Create async generator for test data
        async def data_generator():
            for tweet in self.large_dataset:
                yield tweet
                await asyncio.sleep(0)  # Yield control
        
        # Test JSON export
        json_path = self.output_dir / "test_export.json"
        start_time = time.time()
        json_stats = await streaming_exporter.export_json_stream(
            data_generator(), json_path, pretty_print=False
        )
        results['json_export'] = {
            'time_seconds': time.time() - start_time,
            'records_per_second': json_stats['records_exported'] / (time.time() - start_time),
            **json_stats
        }
        
        # Test CSV export
        csv_path = self.output_dir / "test_export.csv"
        start_time = time.time()
        csv_stats = await streaming_exporter.export_csv_stream(
            data_generator(), csv_path, fieldnames=self.large_dataset[0].keys()
        )
        results['csv_export'] = {
            'time_seconds': time.time() - start_time,
            'records_per_second': csv_stats['records_exported'] / (time.time() - start_time),
            **csv_stats
        }
        
        # Test JSONL export
        jsonl_path = self.output_dir / "test_export.jsonl"
        start_time = time.time()
        jsonl_stats = await streaming_exporter.export_jsonl_stream(
            data_generator(), jsonl_path
        )
        results['jsonl_export'] = {
            'time_seconds': time.time() - start_time,
            'records_per_second': jsonl_stats['records_exported'] / (time.time() - start_time),
            **jsonl_stats
        }
        
        # Clean up test files
        for path in [json_path, csv_path, jsonl_path]:
            if path.exists():
                path.unlink()
        
        self.results['tests']['streaming_export'] = results
        logger.info(f"Streaming export test completed: {results}")
    
    async def test_parallel_sentiment(self):
        """Test parallel sentiment analysis performance."""
        logger.info("Testing parallel sentiment analysis performance")
        
        processor = get_parallel_processor()
        
        results = {
            'sequential_time': 0,
            'parallel_time': 0,
            'speedup': 0,
            'throughput_tweets_per_second': 0
        }
        
        # Test sequential processing (for comparison)
        from sentiment import SentimentAnalyzer
        
        analyzer = SentimentAnalyzer('vader')
        start_time = time.time()
        
        for tweet in self.sample_tweets[:100]:
            analyzer.analyze(tweet['text'])
        
        results['sequential_time'] = time.time() - start_time
        
        # Test parallel processing
        start_time = time.time()
        processed_tweets = await processor.process_tweets(
            self.sample_tweets[:100], clean_text=False, show_progress=False
        )
        results['parallel_time'] = time.time() - start_time
        
        # Calculate speedup
        results['speedup'] = results['sequential_time'] / results['parallel_time'] if results['parallel_time'] > 0 else 0
        
        # Calculate throughput
        results['throughput_tweets_per_second'] = len(processed_tweets) / results['parallel_time'] if results['parallel_time'] > 0 else 0
        
        # Test with different batch sizes
        batch_results = {}
        for batch_size in [10, 50, 100, 200]:
            processor.batch_size = batch_size
            start_time = time.time()
            await processor.process_tweets(self.sample_tweets[:200], clean_text=False)
            batch_time = time.time() - start_time
            
            batch_results[batch_size] = {
                'time_seconds': batch_time,
                'throughput': 200 / batch_time if batch_time > 0 else 0
            }
        
        results['batch_size_comparison'] = batch_results
        
        # Reset processor stats
        processor.reset_stats()
        
        self.results['tests']['parallel_sentiment'] = results
        logger.info(f"Parallel sentiment test completed: {results}")
    
    async def test_database_sharding(self):
        """Test database sharding performance."""
        logger.info("Testing database sharding performance")
        
        shard_manager = get_shard_manager()
        
        results = {
            'shard_distribution': {},
            'query_performance': {},
            'write_performance': {}
        }
        
        # Test 1: Shard distribution
        start_time = time.time()
        for tweet in self.sample_tweets:
            shard_key = shard_manager.get_shard_key(tweet)
            if shard_key not in results['shard_distribution']:
                results['shard_distribution'][shard_key] = 0
            results['shard_distribution'][shard_key] += 1
        
        distribution_time = time.time() - start_time
        
        results['shard_distribution']['calculation_time'] = distribution_time
        
        # Test 2: Write performance
        start_time = time.time()
        shard_manager.save_tweets_batch(self.sample_tweets[:100])
        write_time = time.time() - start_time
        
        results['write_performance'] = {
            'total_time': write_time,
            'tweets_per_second': 100 / write_time if write_time > 0 else 0,
            'avg_time_per_tweet': write_time / 100
        }
        
        # Test 3: Query performance
        start_time = time.time()
        all_tweets = shard_manager.query_all_shards(
            "SELECT * FROM tweets WHERE username = ?",
            ('user_1',)
        )
        query_time = time.time() - start_time
        
        results['query_performance'] = {
            'total_time': query_time,
            'results_found': len(all_tweets),
            'queries_per_second': 1 / query_time if query_time > 0 else 0
        }
        
        # Test 4: Shard stats retrieval
        start_time = time.time()
        stats = shard_manager.get_all_shard_stats()
        stats_time = time.time() - start_time
        
        results['stats_retrieval'] = {
            'time_seconds': stats_time,
            'total_shards': stats['total_shards'],
            'total_tweets': stats['total_tweets']
        }
        
        self.results['tests']['database_sharding'] = results
        logger.info(f"Database sharding test completed: {results}")
    
    async def test_health_checks(self):
        """Test health check performance."""
        logger.info("Testing health check performance")
        
        results = {
            'individual_check_times': {},
            'total_check_time': 0,
            'average_check_time': 0
        }
        
        # Run health checks
        start_time = time.time()
        check_results = await health_checker.run_all_checks()
        total_time = time.time() - start_time
        
        results['total_check_time'] = total_time
        
        # Record individual check times
        for name, result in check_results.items():
            results['individual_check_times'][name] = result.response_time
        
        results['average_check_time'] = statistics.mean(results['individual_check_times'].values())
        
        # Test health check frequency impact
        frequencies = [1, 5, 10, 30]  # seconds
        frequency_results = {}
        
        for freq in frequencies:
            check_start = time.time()
            await health_checker.run_all_checks()
            check_time = time.time() - check_start
            
            frequency_results[freq] = {
                'check_time': check_time,
                'overhead_per_hour': (check_time * 3600 / freq) / 3600 * 100  # percentage
            }
        
        results['frequency_impact'] = frequency_results
        
        self.results['tests']['health_checks'] = results
        logger.info(f"Health checks test completed: {results}")
    
    async def test_monitoring(self):
        """Test monitoring performance overhead."""
        logger.info("Testing monitoring performance overhead")
        
        results = {
            'metrics_collection': {},
            'prometheus_overhead': {}
        }
        
        # Test 1: Metrics collection performance
        start_time = time.time()
        
        # Record various metrics
        for i in range(1000):
            metrics.record_api_call('search', 'success', 0.1 + (i % 10) * 0.01)
            metrics.record_db_query('SELECT', 0.05 + (i % 5) * 0.01)
            metrics.record_cache_hit() if i % 2 == 0 else metrics.record_cache_miss()
            metrics.record_sentiment_analysis('vader', 0.02 + (i % 3) * 0.01)
            metrics.record_tweet_processed('test_query', 'en', 'positive')
        
        collection_time = time.time() - start_time
        
        results['metrics_collection'] = {
            'total_time': collection_time,
            'metrics_per_second': 5000 / collection_time if collection_time > 0 else 0,
            'time_per_metric_ms': (collection_time / 5000) * 1000 if collection_time > 0 else 0
        }
        
        # Test 2: Summary generation
        start_time = time.time()
        summary = metrics.get_summary()
        summary_time = time.time() - start_time
        
        results['summary_generation'] = {
            'time_seconds': summary_time,
            'summary_size_bytes': len(json.dumps(summary).encode('utf-8'))
        }
        
        # Test 3: Prometheus metrics generation overhead
        if metrics.enable_prometheus:
            start_time = time.time()
            prom_metrics = metrics.get_prometheus_metrics()
            prom_time = time.time() - start_time
            
            results['prometheus_overhead'] = {
                'generation_time': prom_time,
                'metrics_size_bytes': len(prom_metrics.encode('utf-8')) if prom_metrics else 0
            }
        
        self.results['tests']['monitoring'] = results
        logger.info(f"Monitoring test completed: {results}")
    
    def _calculate_throughput(self, times: List[float], data: List[Any]) -> float:
        """Calculate throughput in MB/s."""
        total_size = sum(len(json.dumps(item).encode('utf-8')) for item in data)
        total_time = sum(times)
        return (total_size / 1024 / 1024) / total_time if total_time > 0 else 0
    
    def _save_results(self):
        """Save test results to file."""
        results_file = self.output_dir / f"performance_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        logger.info(f"Performance test results saved to {results_file}")
    
    def _generate_report(self) -> Dict[str, Any]:
        """Generate performance test report."""
        report = {
            'summary': {
                'total_tests': len(self.results['tests']),
                'passed_tests': 0,
                'failed_tests': 0,
                'warnings': []
            },
            'recommendations': []
        }
        
        # Analyze results and generate recommendations
        for test_name, test_results in self.results['tests'].items():
            if 'status' in test_results and test_results['status'] == 'skipped':
                continue
            
            # Basic pass/fail criteria (can be made more sophisticated)
            if test_name == 'rate_limiter':
                if test_results['accuracy']['accuracy_percent'] >= 95:
                    report['summary']['passed_tests'] += 1
                else:
                    report['summary']['failed_tests'] += 1
                    report['recommendations'].append("Rate limiter accuracy is low, check implementation")
            
            elif test_name == 'redis_cache':
                if test_results['read_performance']['hit_rate_percent'] >= 80:
                    report['summary']['passed_tests'] += 1
                else:
                    report['summary']['warnings'] += 1
                    report['recommendations'].append("Cache hit rate is low, consider increasing cache size")
            
            elif test_name == 'compression':
                if test_results['size_reduction']['avg_lz4_reduction'] > 20:
                    report['summary']['passed_tests'] += 1
                else:
                    report['summary']['warnings'] += 1
                    report['recommendations'].append("Compression ratio is low, check data patterns")
            
            elif test_name == 'parallel_sentiment':
                if test_results['speedup'] > 1.5:
                    report['summary']['passed_tests'] += 1
                else:
                    report['summary']['failed_tests'] += 1
                    report['recommendations'].append("Parallel processing speedup is insufficient, check worker configuration")
            
            else:
                # Default: pass if we have results
                report['summary']['passed_tests'] += 1
        
        # Save report
        report_file = self.output_dir / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_file, 'w') as f:
            f.write("# TwitterScrapper Performance Test Report\n\n")
            f.write(f"**Test Date:** {self.results['timestamp']}\n\n")
            f.write(f"**Summary:** {report['summary']['passed_tests']} passed, ")
            f.write(f"{report['summary']['failed_tests']} failed, ")
            f.write(f"{len(report['summary']['warnings'])} warnings\n\n")
            
            if report['recommendations']:
                f.write("## Recommendations\n\n")
                for rec in report['recommendations']:
                    f.write(f"- {rec}\n")
                f.write("\n")
            
            f.write("## Detailed Results\n\n")
            f.write("See JSON file for complete test results.\n")
        
        logger.info(f"Performance report saved to {report_file}")
        
        return report


async def run_performance_tests():
    """Run the complete performance test suite."""
    test_suite = PerformanceTestSuite()
    
    try:
        report = await test_suite.run_all_tests()
        
        # Print summary
        print("\n" + "="*60)
        print("PERFORMANCE TEST SUITE SUMMARY")
        print("="*60)
        print(f"Passed: {report['summary']['passed_tests']}")
        print(f"Failed: {report['summary']['failed_tests']}")
        print(f"Warnings: {len(report['summary']['warnings'])}")
        
        if report['recommendations']:
            print("\nRecommendations:")
            for rec in report['recommendations']:
                print(f"  - {rec}")
        
        print("\n" + "="*60)
        
        return report
        
    except Exception as e:
        logger.error(f"Performance test suite failed: {e}")
        raise


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    asyncio.run(run_performance_tests())