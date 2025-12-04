import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import cpu_count
import time
from functools import partial

from sentiment import SentimentAnalyzer
from monitoring import metrics
from utils import clean_tweet_text

logger = logging.getLogger(__name__)

class ParallelSentimentProcessor:
    """
    Parallel sentiment analysis processor for large tweet collections.
    
    Leverages multiple CPU cores to perform sentiment analysis on tweets
    in parallel, significantly reducing processing time for large datasets.
    """
    
    def __init__(self, method: str = 'vader', max_workers: Optional[int] = None, 
                 batch_size: int = 100, use_multiprocessing: bool = True):
        """
        Initialize parallel sentiment processor.
        
        Args:
            method: Sentiment analysis method ('vader' or 'textblob')
            max_workers: Maximum number of worker processes/threads (None for auto)
            batch_size: Number of tweets to process in each batch
            use_multiprocessing: Use multiprocessing (True) or threading (False)
        """
        self.method = method
        self.max_workers = max_workers or max(1, cpu_count() - 1)
        self.batch_size = batch_size
        self.use_multiprocessing = use_multiprocessing
        
        # Statistics
        self.tweets_processed = 0
        self.processing_time = 0.0
        self.batches_processed = 0
        
        # Initialize executor
        self._executor: Optional[ProcessPoolExecutor] = None
        
        logger.info(f"Initialized parallel sentiment processor with {self.max_workers} workers "
                   f"using {'multiprocessing' if use_multiprocessing else 'threading'}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.stop()
    
    def start(self):
        """Start the processor."""
        if self.use_multiprocessing:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        else:
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        logger.info("Parallel sentiment processor started")
    
    def stop(self):
        """Stop the processor and cleanup resources."""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        
        logger.info("Parallel sentiment processor stopped")
    
    async def process_tweets(
        self, 
        tweets: List[Dict[str, Any]], 
        clean_text: bool = False,
        show_progress: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Process tweets in parallel and add sentiment analysis.
        
        Args:
            tweets: List of tweet dictionaries
            clean_text: Whether to clean tweet text before analysis
            show_progress: Whether to show progress updates
            
        Returns:
            List of tweets with sentiment analysis added
        """
        if not tweets:
            return []
        
        start_time = time.time()
        
        # Split tweets into batches
        batches = self._create_batches(tweets)
        total_batches = len(batches)
        
        if show_progress:
            logger.info(f"Processing {len(tweets)} tweets in {total_batches} batches")
        
        # Process batches in parallel
        tasks = []
        for i, batch in enumerate(batches):
            task = self._process_batch(batch, clean_text, batch_num=i + 1, 
                                     show_progress=show_progress)
            tasks.append(task)
        
        # Wait for all batches to complete
        results = await asyncio.gather(*tasks)
        
        # Combine results
        processed_tweets = []
        for batch_result in results:
            processed_tweets.extend(batch_result)
        
        # Update statistics
        self.tweets_processed += len(tweets)
        self.batches_processed += total_batches
        self.processing_time += time.time() - start_time
        
        # Record metrics
        metrics.record_sentiment_analysis(self.method, time.time() - start_time)
        
        if show_progress:
            avg_time = (time.time() - start_time) / len(tweets) if tweets else 0
            logger.info(f"Processed {len(tweets)} tweets in {time.time() - start_time:.2f}s "
                       f"(avg: {avg_time*1000:.2f}ms per tweet)")
        
        return processed_tweets
    
    def _create_batches(self, tweets: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split tweets into batches for parallel processing."""
        return [tweets[i:i + self.batch_size] 
                for i in range(0, len(tweets), self.batch_size)]
    
    async def _process_batch(
        self, 
        batch: List[Dict[str, Any]], 
        clean_text: bool,
        batch_num: int,
        show_progress: bool
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of tweets.
        
        Args:
            batch: Batch of tweets
            clean_text: Whether to clean text
            batch_num: Batch number for progress tracking
            show_progress: Whether to show progress
            
        Returns:
            Processed batch with sentiment analysis
        """
        # Run CPU-intensive sentiment analysis in executor
        loop = asyncio.get_event_loop()
        
        process_func = partial(
            self._analyze_batch_sync,
            batch=batch,
            method=self.method,
            clean_text=clean_text
        )
        
        try:
            result = await loop.run_in_executor(self._executor, process_func)
            
            if show_progress:
                logger.debug(f"Batch {batch_num} completed: {len(result)} tweets")
            
            return result
            
        except Exception as e:
            logger.error(f"Batch {batch_num} processing failed: {e}")
            
            # Fallback: process sequentially
            return self._analyze_batch_sync(batch, self.method, clean_text)
    
    @staticmethod
    def _analyze_batch_sync(
        batch: List[Dict[str, Any]], 
        method: str,
        clean_text: bool
    ) -> List[Dict[str, Any]]:
        """
        Synchronously analyze a batch of tweets (runs in separate process/thread).
        
        Args:
            batch: Batch of tweets
            method: Sentiment analysis method
            clean_text: Whether to clean text
            
        Returns:
            Processed batch with sentiment analysis
        """
        # Create analyzer (this runs in separate process)
        analyzer = SentimentAnalyzer(method)
        
        processed_batch = []
        
        for tweet in batch:
            try:
                # Get text for analysis
                text = tweet.get('text', '')
                
                if clean_text:
                    text = clean_tweet_text(text)
                
                # Analyze sentiment
                sentiment = analyzer.analyze(text)
                
                # Add sentiment fields to tweet
                processed_tweet = tweet.copy()
                processed_tweet.update({
                    f'sentiment_{k}': v for k, v in sentiment.items()
                })
                
                processed_batch.append(processed_tweet)
                
            except Exception as e:
                logger.error(f"Sentiment analysis failed for tweet {tweet.get('id')}: {e}")
                
                # Add neutral sentiment as fallback
                processed_tweet = tweet.copy()
                processed_tweet.update({
                    'sentiment_label': 'neutral',
                    'sentiment_compound': 0.0
                })
                processed_batch.append(processed_tweet)
        
        return processed_batch
    
    async def process_tweet_stream(
        self,
        tweet_generator: Any,
        clean_text: bool = False,
        show_progress: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Process tweets from an async generator in parallel.
        
        Args:
            tweet_generator: Async generator that yields tweets
            clean_text: Whether to clean tweet text
            show_progress: Whether to show progress
            
        Returns:
            List of processed tweets
        """
        all_tweets = []
        current_batch = []
        
        async for tweet in tweet_generator:
            current_batch.append(tweet)
            
            if len(current_batch) >= self.batch_size:
                # Process current batch
                processed_batch = await self.process_tweets(
                    current_batch, clean_text, show_progress
                )
                all_tweets.extend(processed_batch)
                current_batch = []
        
        # Process remaining tweets
        if current_batch:
            processed_batch = await self.process_tweets(
                current_batch, clean_text, show_progress
            )
            all_tweets.extend(processed_batch)
        
        return all_tweets
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Returns:
            Dictionary with statistics
        """
        avg_time_per_tweet = (
            self.processing_time / self.tweets_processed 
            if self.tweets_processed > 0 else 0
        )
        
        avg_batch_size = (
            self.tweets_processed / self.batches_processed 
            if self.batches_processed > 0 else 0
        )
        
        return {
            'method': self.method,
            'max_workers': self.max_workers,
            'batch_size': self.batch_size,
            'use_multiprocessing': self.use_multiprocessing,
            'tweets_processed': self.tweets_processed,
            'batches_processed': self.batches_processed,
            'processing_time': self.processing_time,
            'avg_time_per_tweet': avg_time_per_tweet,
            'avg_time_per_tweet_ms': avg_time_per_tweet * 1000,
            'avg_batch_size': avg_batch_size,
            'throughput_per_second': (
                self.tweets_processed / self.processing_time 
                if self.processing_time > 0 else 0
            )
        }
    
    def reset_stats(self):
        """Reset processing statistics."""
        self.tweets_processed = 0
        self.processing_time = 0.0
        self.batches_processed = 0


class SentimentProcessingQueue:
    """
    Queue-based sentiment processing with automatic batching and parallel execution.
    
    This class provides a higher-level interface for processing large numbers
    of tweets with automatic batching, queue management, and progress tracking.
    """
    
    def __init__(self, processor: ParallelSentimentProcessor, max_queue_size: int = 10000):
        """
        Initialize processing queue.
        
        Args:
            processor: Parallel sentiment processor instance
            max_queue_size: Maximum number of tweets to queue
        """
        self.processor = processor
        self.max_queue_size = max_queue_size
        
        self._queue: List[Dict[str, Any]] = []
        self._results: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._processing = False
        self._process_task: Optional[asyncio.Task] = None
        
        # Statistics
        self.queued_count = 0
        self.processed_count = 0
        
        logger.info(f"Initialized sentiment processing queue (max size: {max_queue_size})")
    
    async def add_tweet(self, tweet: Dict[str, Any]) -> bool:
        """
        Add a tweet to the processing queue.
        
        Args:
            tweet: Tweet to process
            
        Returns:
            True if added, False if queue is full
        """
        async with self._lock:
            if len(self._queue) >= self.max_queue_size:
                logger.warning("Processing queue is full")
                return False
            
            self._queue.append(tweet)
            self.queued_count += 1
            
            # Start processing if not already running
            if not self._processing:
                self._processing = True
                self._process_task = asyncio.create_task(self._process_queue())
            
            return True
    
    async def add_tweets(self, tweets: List[Dict[str, Any]]) -> int:
        """
        Add multiple tweets to the processing queue.
        
        Args:
            tweets: List of tweets to process
            
        Returns:
            Number of tweets actually added
        """
        added = 0
        
        for tweet in tweets:
            if await self.add_tweet(tweet):
                added += 1
            else:
                break
        
        return added
    
    async def _process_queue(self):
        """Process tweets in the queue."""
        while True:
            async with self._lock:
                if not self._queue:
                    # Queue is empty, stop processing
                    self._processing = False
                    break
                
                # Take a batch from the queue
                batch_size = min(len(self._queue), self.processor.batch_size)
                batch = self._queue[:batch_size]
                self._queue = self._queue[batch_size:]
            
            # Process the batch
            try:
                processed_batch = await self.processor.process_tweets(batch)
                
                async with self._lock:
                    self._results.extend(processed_batch)
                    self.processed_count += len(processed_batch)
                
                logger.debug(f"Processed batch of {len(batch)} tweets from queue")
                
            except Exception as e:
                logger.error(f"Queue processing error: {e}")
            
            # Small delay to allow other tasks to run
            await asyncio.sleep(0.01)
    
    async def get_results(self) -> List[Dict[str, Any]]:
        """
        Get all processed results.
        
        Returns:
            List of processed tweets
        """
        async with self._lock:
            # Return copy of results and clear the list
            results = self._results.copy()
            self._results.clear()
            return results
    
    async def wait_until_empty(self):
        """Wait until the queue is empty."""
        while True:
            async with self._lock:
                if not self._queue and not self._processing:
                    break
            
            await asyncio.sleep(0.1)
    
    def get_queue_size(self) -> int:
        """
        Get current queue size.
        
        Returns:
            Number of tweets in queue
        """
        return len(self._queue)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        
        Returns:
            Dictionary with statistics
        """
        processor_stats = self.processor.get_stats()
        
        return {
            'queue': {
                'queued_count': self.queued_count,
                'processed_count': self.processed_count,
                'current_size': self.get_queue_size(),
                'max_size': self.max_queue_size
            },
            'processor': processor_stats
        }


# Global processor instances
_parallel_processor: Optional[ParallelSentimentProcessor] = None
_processing_queue: Optional[SentimentProcessingQueue] = None

def get_parallel_processor() -> ParallelSentimentProcessor:
    """
    Get or create global parallel sentiment processor.
    
    Returns:
        ParallelSentimentProcessor instance
    """
    global _parallel_processor
    
    if _parallel_processor is None:
        _parallel_processor = ParallelSentimentProcessor()
        _parallel_processor.start()
    
    return _parallel_processor

def get_processing_queue() -> SentimentProcessingQueue:
    """
    Get or create global sentiment processing queue.
    
    Returns:
        SentimentProcessingQueue instance
    """
    global _processing_queue
    
    if _processing_queue is None:
        processor = get_parallel_processor()
        _processing_queue = SentimentProcessingQueue(processor)
    
    return _processing_queue


# Example usage
async def example_usage():
    """Example of how to use parallel sentiment processing."""
    
    # Example 1: Process a list of tweets
    async def example_1():
        processor = get_parallel_processor()
        
        tweets = [
            {'id': '1', 'text': 'I love this product!'},
            {'id': '2', 'text': 'This is terrible.'},
            {'id': '3', 'text': 'It is okay I guess.'},
            # ... more tweets
        ]
        
        processed_tweets = await processor.process_tweets(
            tweets, 
            clean_text=True, 
            show_progress=True
        )
        
        print(f"Processed {len(processed_tweets)} tweets")
        print(f"Stats: {processor.get_stats()}")
        
        return processed_tweets
    
    # Example 2: Use processing queue
    async def example_2():
        queue = get_processing_queue()
        
        # Add tweets to queue
        tweets = [
            {'id': f'{i}', 'text': f'Tweet number {i}'}
            for i in range(1000)
        ]
        
        added = await queue.add_tweets(tweets)
        print(f"Added {added} tweets to queue")
        
        # Wait for processing to complete
        await queue.wait_until_empty()
        
        # Get results
        results = await queue.get_results()
        print(f"Got {len(results)} processed tweets")
        
        print(f"Queue stats: {queue.get_stats()}")
        
        return results
    
    # Example 3: Process tweets from async generator
    async def example_3():
        processor = get_parallel_processor()
        
        async def tweet_generator():
            for i in range(100):
                yield {'id': f'{i}', 'text': f'Tweet {i} content'}
                await asyncio.sleep(0)  # Yield control
        
        processed_tweets = await processor.process_tweet_stream(
            tweet_generator(),
            clean_text=True,
            show_progress=True
        )
        
        print(f"Stream processed {len(processed_tweets)} tweets")
        
        return processed_tweets
    
    # Run examples
    await example_1()
    await example_2()
    await example_3()


if __name__ == "__main__":
    # Run example usage
    asyncio.run(example_usage())