import logging
from typing import Dict, Any

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Sentiment analysis wrapper supporting multiple analyzers."""
    
    def __init__(self, method: str = "vader"):
        """
        Initialize sentiment analyzer.
        
        Args:
            method: Analysis method - "vader" or "textblob"
        """
        self.method = method.lower()
        self.analyzer = None
        
        if self.method == "vader":
            if not VADER_AVAILABLE:
                raise ImportError("vaderSentiment not installed. Run: pip install vaderSentiment")
            self.analyzer = SentimentIntensityAnalyzer()
        elif self.method == "textblob":
            if not TEXTBLOB_AVAILABLE:
                raise ImportError("textblob not installed. Run: pip install textblob")
        else:
            raise ValueError(f"Unsupported sentiment analysis method: {method}")
    
    def analyze(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of given text.
        
        Returns:
            Dictionary with sentiment scores
        """
        if not text or not text.strip():
            return {"polarity": 0.0, "subjectivity": 0.0, "compound": 0.0, "label": "neutral"}
        
        try:
            if self.method == "vader":
                return self._analyze_vader(text)
            elif self.method == "textblob":
                return self._analyze_textblob(text)
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return {"polarity": 0.0, "subjectivity": 0.0, "compound": 0.0, "label": "neutral"}
    
    def _analyze_vader(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment using VADER."""
        scores = self.analyzer.polarity_scores(text)
        compound = scores["compound"]
        
        # Determine label
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        
        return {
            "compound": compound,
            "positive": scores["pos"],
            "negative": scores["neg"],
            "neutral": scores["neu"],
            "label": label
        }
    
    def _analyze_textblob(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment using TextBlob."""
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        
        # Determine label
        if polarity > 0:
            label = "positive"
        elif polarity < 0:
            label = "negative"
        else:
            label = "neutral"
        
        return {
            "polarity": polarity,
            "subjectivity": subjectivity,
            "label": label
        }