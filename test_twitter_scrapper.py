import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import clean_tweet_text, safe_filename, load_config, setup_logging
from sentiment import SentimentAnalyzer


class TestUtils:
    """Test cases for utility functions."""
    
    def test_clean_tweet_text(self):
        """Test tweet text cleaning functionality."""
        # Test with URL
        text = "Check this out https://example.com it's great!"
        cleaned = clean_tweet_text(text)
        assert "https://example.com" not in cleaned
        assert "Check this out it's great!" == cleaned
        
        # Test with mentions
        text = "Hello @user how are you?"
        cleaned = clean_tweet_text(text)
        assert "@user" not in cleaned
        assert "Hello how are you?" == cleaned
        
        # Test with hashtags
        text = "Love #Python programming #coding"
        cleaned = clean_tweet_text(text)
        assert "#Python" not in cleaned
        assert "#coding" not in cleaned
        assert "Love programming" == cleaned
        
        # Test with multiple spaces
        text = "Hello    world   test"
        cleaned = clean_tweet_text(text)
        assert "Hello world test" == cleaned
        
        # Test empty text
        text = ""
        cleaned = clean_tweet_text(text)
        assert "" == cleaned
    
    def test_safe_filename(self):
        """Test safe filename generation."""
        # Test normal text
        filename = safe_filename("bitcoin price analysis")
        assert "bitcoin_price_analysis" == filename
        
        # Test special characters
        filename = safe_filename("gold/silver & crypto!")
        assert "gold_silver___crypto_" == filename
        
        # Test with max length
        long_text = "a" * 50
        filename = safe_filename(long_text, max_length=30)
        assert len(filename) == 30
        assert "a" * 30 == filename
    
    def test_load_config(self, tmp_path):
        """Test configuration loading."""
        # Create a temporary config file
        config_data = {
            "default_count": 20,
            "default_product": "Latest",
            "log_level": "DEBUG"
        }
        config_file = tmp_path / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        # Test loading config
        config = load_config(str(config_file))
        assert config["default_count"] == 20
        assert config["default_product"] == "Latest"
        assert config["log_level"] == "DEBUG"
    
    def test_load_config_file_not_found(self):
        """Test configuration loading with missing file."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_config.json")
    
    def test_setup_logging(self):
        """Test logging setup."""
        logger = setup_logging("DEBUG")
        assert logger.level == 10  # DEBUG level
        assert logger.name == "__main__"


class TestSentimentAnalyzer:
    """Test cases for sentiment analysis."""
    
    def test_sentiment_analyzer_initialization_vader(self):
        """Test VADER sentiment analyzer initialization."""
        if not SentimentAnalyzer("vader")._vader_available():
            pytest.skip("VADER not available")
        
        analyzer = SentimentAnalyzer("vader")
        assert analyzer.method == "vader"
        assert analyzer.analyzer is not None
    
    def test_sentiment_analyzer_initialization_textblob(self):
        """Test TextBlob sentiment analyzer initialization."""
        if not SentimentAnalyzer("textblob")._textblob_available():
            pytest.skip("TextBlob not available")
        
        analyzer = SentimentAnalyzer("textblob")
        assert analyzer.method == "textblob"
    
    def test_sentiment_analyzer_invalid_method(self):
        """Test invalid sentiment analysis method."""
        with pytest.raises(ValueError):
            SentimentAnalyzer("invalid_method")
    
    def test_vader_sentiment_positive(self):
        """Test VADER sentiment analysis for positive text."""
        if not SentimentAnalyzer("vader")._vader_available():
            pytest.skip("VADER not available")
        
        analyzer = SentimentAnalyzer("vader")
        result = analyzer.analyze("I love this product! It's amazing!")
        
        assert "label" in result
        assert "compound" in result
        assert result["label"] == "positive"
        assert result["compound"] > 0.05
    
    def test_vader_sentiment_negative(self):
        """Test VADER sentiment analysis for negative text."""
        if not SentimentAnalyzer("vader")._vader_available():
            pytest.skip("VADER not available")
        
        analyzer = SentimentAnalyzer("vader")
        result = analyzer.analyze("I hate this terrible product!")
        
        assert result["label"] == "negative"
        assert result["compound"] < -0.05
    
    def test_vader_sentiment_neutral(self):
        """Test VADER sentiment analysis for neutral text."""
        if not SentimentAnalyzer("vader")._vader_available():
            pytest.skip("VADER not available")
        
        analyzer = SentimentAnalyzer("vader")
        result = analyzer.analyze("The product is okay.")
        
        assert result["label"] == "neutral"
        assert -0.05 <= result["compound"] <= 0.05
    
    def test_textblob_sentiment_positive(self):
        """Test TextBlob sentiment analysis for positive text."""
        if not SentimentAnalyzer("textblob")._textblob_available():
            pytest.skip("TextBlob not available")
        
        analyzer = SentimentAnalyzer("textblob")
        result = analyzer.analyze("I love this product! It's amazing!")
        
        assert "label" in result
        assert "polarity" in result
        assert result["label"] == "positive"
        assert result["polarity"] > 0
    
    def test_textblob_sentiment_negative(self):
        """Test TextBlob sentiment analysis for negative text."""
        if not SentimentAnalyzer("textblob")._textblob_available():
            pytest.skip("TextBlob not available")
        
        analyzer = SentimentAnalyzer("textblob")
        result = analyzer.analyze("I hate this terrible product!")
        
        assert result["label"] == "negative"
        assert result["polarity"] < 0
    
    def test_empty_text(self):
        """Test sentiment analysis with empty text."""
        analyzer = SentimentAnalyzer("vader")
        result = analyzer.analyze("")
        
        assert result["label"] == "neutral"
        assert result["compound"] == 0.0
    
    def test_none_text(self):
        """Test sentiment analysis with None text."""
        analyzer = SentimentAnalyzer("vader")
        result = analyzer.analyze(None)
        
        assert result["label"] == "neutral"
        assert result["compound"] == 0.0


class TestAuth:
    """Test cases for authentication."""
    
    def test_get_client_no_cookies(self):
        """Test client initialization without cookies."""
        from auth import getClient
        
        # Mock the Path.exists to return False
        with patch('auth.cookiesPath.exists', return_value=False), \
             patch('auth.cookiesJsonPath.exists', return_value=False):
            
            with pytest.raises(RuntimeError):
                getClient()
    
    def test_load_cookies_invalid_json(self, tmp_path):
        """Test loading invalid cookies JSON."""
        from auth import _load_cookies
        
        # Create invalid JSON file
        cookies_file = tmp_path / "cookies.json"
        cookies_file.write_text("invalid json")
        
        with patch('auth.cookiesPath', cookies_file):
            client = Mock()
            result = _load_cookies(client)
            assert result is False


class TestSearch:
    """Test cases for search functionality."""
    
    @pytest.mark.asyncio
    async def test_search_no_results(self):
        """Test search with no results."""
        from search import search
        
        # Mock client
        mock_client = Mock()
        mock_client.search_tweet = Mock(return_value=[])
        
        result = await search(mock_client, "test query", count=10)
        assert result == []
    
    @pytest.mark.asyncio
    async def test_search_rate_limit(self):
        """Test search with rate limit handling."""
        from search import search
        from twikit.errors import TooManyRequests
        
        # Mock client that raises TooManyRequests
        mock_client = Mock()
        mock_client.search_tweet = Mock(side_effect=TooManyRequests())
        
        result = await search(mock_client, "test query", count=10, retry_wait=1)
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__])