import json
import logging
from typing import Any, Optional, Dict
import zlib
import lz4.frame
import gzip

logger = logging.getLogger(__name__)

class CompressionManager:
    """
    Compression manager for tweets and cache entries.
    
    Provides multiple compression algorithms with automatic selection
    based on data size and performance requirements.
    """
    
    def __init__(self, default_algorithm: str = 'lz4', compression_threshold: int = 100):
        """
        Initialize compression manager.
        
        Args:
            default_algorithm: Default compression algorithm ('lz4', 'zlib', 'gzip', 'none')
            compression_threshold: Minimum data size in bytes to apply compression
        """
        self.default_algorithm = default_algorithm
        self.compression_threshold = compression_threshold
        
        # Compression statistics
        self._compressed_count = 0
        self._uncompressed_count = 0
        self._total_original_size = 0
        self._total_compressed_size = 0
    
    def compress(self, data: Any, algorithm: Optional[str] = None) -> Dict[str, Any]:
        """
        Compress data and return metadata wrapper.
        
        Args:
            data: Data to compress (will be JSON serialized)
            algorithm: Compression algorithm to use (None for auto-selection)
            
        Returns:
            Dictionary with compressed data and metadata
        """
        # Serialize data to JSON string
        json_data = json.dumps(data, default=str)
        original_size = len(json_data.encode('utf-8'))
        
        # Check if compression is needed
        if original_size < self.compression_threshold:
            self._uncompressed_count += 1
            return {
                'compressed': False,
                'algorithm': 'none',
                'original_size': original_size,
                'compressed_size': original_size,
                'data': data
            }
        
        # Select algorithm
        algo = algorithm or self.default_algorithm
        
        try:
            if algo == 'lz4':
                compressed = self._compress_lz4(json_data)
            elif algo == 'zlib':
                compressed = self._compress_zlib(json_data)
            elif algo == 'gzip':
                compressed = self._compress_gzip(json_data)
            elif algo == 'none':
                compressed = json_data.encode('utf-8')
            else:
                logger.warning(f"Unknown compression algorithm: {algo}, using default")
                compressed = self._compress_lz4(json_data)
                algo = 'lz4'
            
            compressed_size = len(compressed)
            
            # Update statistics
            self._compressed_count += 1
            self._total_original_size += original_size
            self._total_compressed_size += compressed_size
            
            compression_ratio = original_size / compressed_size if compressed_size > 0 else 1
            
            logger.debug(f"Compressed {original_size} -> {compressed_size} bytes "
                        f"({compression_ratio:.2f}x) using {algo}")
            
            return {
                'compressed': True,
                'algorithm': algo,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': compression_ratio,
                'data': compressed.decode('latin1')  # Store as string for JSON compatibility
            }
            
        except Exception as e:
            logger.error(f"Compression failed: {e}, returning uncompressed")
            self._uncompressed_count += 1
            return {
                'compressed': False,
                'algorithm': 'none',
                'original_size': original_size,
                'compressed_size': original_size,
                'data': data
            }
    
    def decompress(self, compressed_data: Dict[str, Any]) -> Any:
        """
        Decompress data from metadata wrapper.
        
        Args:
            compressed_data: Dictionary with compressed data and metadata
            
        Returns:
            Original uncompressed data
        """
        if not compressed_data.get('compressed', False):
            # Not compressed, return data as-is
            return compressed_data['data']
        
        try:
            algorithm = compressed_data['algorithm']
            compressed_bytes = compressed_data['data'].encode('latin1')
            
            if algorithm == 'lz4':
                decompressed = self._decompress_lz4(compressed_bytes)
            elif algorithm == 'zlib':
                decompressed = self._decompress_zlib(compressed_bytes)
            elif algorithm == 'gzip':
                decompressed = self._decompress_gzip(compressed_bytes)
            else:
                logger.warning(f"Unknown compression algorithm: {algorithm}")
                return compressed_data['data']
            
            # Parse JSON
            return json.loads(decompressed)
            
        except Exception as e:
            logger.error(f"Decompression failed: {e}, returning original data")
            return compressed_data.get('data')
    
    def _compress_lz4(self, data: str) -> bytes:
        """Compress using LZ4."""
        return lz4.frame.compress(data.encode('utf-8'))
    
    def _decompress_lz4(self, compressed: bytes) -> str:
        """Decompress using LZ4."""
        return lz4.frame.decompress(compressed).decode('utf-8')
    
    def _compress_zlib(self, data: str) -> bytes:
        """Compress using zlib."""
        return zlib.compress(data.encode('utf-8'), level=6)
    
    def _decompress_zlib(self, compressed: bytes) -> str:
        """Decompress using zlib."""
        return zlib.decompress(compressed).decode('utf-8')
    
    def _compress_gzip(self, data: str) -> bytes:
        """Compress using gzip."""
        return gzip.compress(data.encode('utf-8'), compresslevel=6)
    
    def _decompress_gzip(self, compressed: bytes) -> str:
        """Decompress using gzip."""
        return gzip.decompress(compressed).decode('utf-8')
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get compression statistics.
        
        Returns:
            Dictionary with compression statistics
        """
        total_processed = self._compressed_count + self._uncompressed_count
        
        if self._total_original_size > 0:
            overall_savings = 1 - (self._total_compressed_size / self._total_original_size)
            avg_compression_ratio = self._total_original_size / self._total_compressed_size if self._total_compressed_size > 0 else 1
        else:
            overall_savings = 0
            avg_compression_ratio = 1
        
        return {
            'algorithm': self.default_algorithm,
            'threshold': self.compression_threshold,
            'compressed_count': self._compressed_count,
            'uncompressed_count': self._uncompressed_count,
            'total_processed': total_processed,
            'compression_rate': self._compressed_count / total_processed if total_processed > 0 else 0,
            'original_size': self._total_original_size,
            'compressed_size': self._total_compressed_size,
            'space_savings': overall_savings,
            'avg_compression_ratio': avg_compression_ratio
        }
    
    def estimate_compression(self, data: Any, algorithm: Optional[str] = None) -> Dict[str, Any]:
        """
        Estimate compression ratio without actually compressing.
        
        Args:
            data: Data to estimate compression for
            algorithm: Algorithm to test (None for default)
            
        Returns:
            Dictionary with estimated compression metrics
        """
        json_data = json.dumps(data, default=str)
        original_size = len(json_data.encode('utf-8'))
        
        if original_size < self.compression_threshold:
            return {
                'algorithm': 'none',
                'original_size': original_size,
                'estimated_compressed_size': original_size,
                'estimated_ratio': 1.0,
                'estimated_savings': 0.0
            }
        
        # Quick estimation based on data characteristics
        # This is a simplified estimation - real compression would be more accurate
        
        # JSON data with repetitive keys compresses well
        key_repetition_score = json_data.count('"') / len(json_data) if len(json_data) > 0 else 0
        
        # Text with spaces and common words compresses well
        text_score = json_data.count(' ') / len(json_data) if len(json_data) > 0 else 0
        
        # Estimate compression ratio (very rough)
        estimated_ratio = 1.5 + (key_repetition_score * 2) + (text_score * 2)
        estimated_ratio = min(estimated_ratio, 10)  # Cap at 10x
        
        estimated_compressed_size = int(original_size / estimated_ratio)
        estimated_savings = 1 - (estimated_compressed_size / original_size)
        
        return {
            'algorithm': algorithm or self.default_algorithm,
            'original_size': original_size,
            'estimated_compressed_size': estimated_compressed_size,
            'estimated_ratio': estimated_ratio,
            'estimated_savings': estimated_savings,
            'key_repetition_score': key_repetition_score,
            'text_score': text_score
        }


# Global compression manager instance
compression_manager = CompressionManager()


class TweetCompressor:
    """
    Specialized compressor for tweet data.
    
    Optimized for the typical structure of tweet data with
    repetitive fields and text content.
    """
    
    def __init__(self, compression_manager: Optional[CompressionManager] = None):
        """
        Initialize tweet compressor.
        
        Args:
            compression_manager: Compression manager instance (None for default)
        """
        self.compression_manager = compression_manager or CompressionManager(
            default_algorithm='lz4',
            compression_threshold=200  # Tweets are usually small
        )
    
    def compress_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compress a single tweet.
        
        Args:
            tweet: Tweet dictionary
            
        Returns:
            Compressed tweet wrapper
        """
        return self.compression_manager.compress(tweet)
    
    def decompress_tweet(self, compressed_tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decompress a single tweet.
        
        Args:
            compressed_tweet: Compressed tweet wrapper
            
        Returns:
            Original tweet dictionary
        """
        return self.compression_manager.decompress(compressed_tweet)
    
    def compress_tweet_batch(self, tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compress a batch of tweets.
        
        Args:
            tweets: List of tweet dictionaries
            
        Returns:
            List of compressed tweet wrappers
        """
        return [self.compress_tweet(tweet) for tweet in tweets]
    
    def decompress_tweet_batch(self, compressed_tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Decompress a batch of tweets.
        
        Args:
            compressed_tweets: List of compressed tweet wrappers
            
        Returns:
            List of original tweet dictionaries
        """
        return [self.decompress_tweet(ct) for ct in compressed_tweets]


# Global tweet compressor instance
tweet_compressor = TweetCompressor()