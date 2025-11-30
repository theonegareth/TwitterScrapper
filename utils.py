import json
import logging
import re
from pathlib import Path
from typing import Dict, Any

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """Load configuration from JSON file."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def clean_tweet_text(text: str) -> str:
    """Clean and preprocess tweet text."""
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    # Remove user mentions
    text = re.sub(r'@\w+', '', text)
    # Remove hashtags (optional, can be kept for analysis)
    text = re.sub(r'#\w+', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text.strip()

def safe_filename(text: str, max_length: int = 30) -> str:
    """Create safe filename from query text."""
    return ''.join(c if c.isalnum() else '_' for c in text)[:max_length]