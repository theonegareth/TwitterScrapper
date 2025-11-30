**⚠️THIS IS STRICTLY FOR EDUCATIONAL USE ONLY!⚠️**
**⚠️DO NOT REDISTRIBUTE⚠️**
**⚠️DO NOT USE YOUR MAIN ACCOUNT⚠️**

# Twitter Scrapper with Sentiment Analysis

An enhanced Twitter scraping tool with built-in sentiment analysis, data preprocessing, and multiple output formats. Perfect for academic research and data analysis projects.

## 🚀 Features

- **Advanced Search**: Supports all Twitter/X search operators (exact phrases, user mentions, hashtags, date ranges, etc.)
- **Sentiment Analysis**: Integrated VADER and TextBlob sentiment analysis
- **Data Preprocessing**: Automatic tweet text cleaning (URLs, mentions, hashtags)
- **Multiple Output Formats**: CSV, JSON, and Excel (XLSX) export
- **Command-Line Interface**: Flexible CLI with argument parsing
- **Interactive Mode**: User-friendly interactive prompts
- **Configuration File**: Customizable settings via `config.json`
- **Robust Error Handling**: Comprehensive error handling and logging
- **Rate Limit Management**: Automatic retry with exponential backoff
- **Unit Tests**: Included test suite for core functionality

## 📋 Requirements

- Python 3.8+
- Twitter/X account (for cookie authentication)
- Modern web browser (Chrome, Firefox, Edge)

## 🔧 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/theonegareth/TwitterScrapper.git
cd TwitterScrapper
```

### 2. Set Up Authentication

**⚠️ IMPORTANT**: You need to obtain Twitter/X cookies for authentication.

1. Install the [Cookie Editor](https://cookie-editor.com/) browser extension
2. Go to [x.com](https://x.com/) and log in
3. Click the Cookie Editor extension icon
4. Click "Export" and select "JSON" format
5. Save the exported JSON as `cookie.json` in the project directory

### 3. Install Dependencies

```bash
# Optional: Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r req.txt

# Download NLTK data for TextBlob (if using TextBlob)
python -m textblob.download_corpora
```

## 🎯 Usage

### Command-Line Mode

```bash
# Basic search
python main.py "bitcoin price"

# Advanced search with options
python main.py "bitcoin since:2024-01-01" -c 50 -p Latest -f csv json -s vader --clean-text

# Search with multiple formats
python main.py "#AI" -c 100 -f csv xlsx json --sentiment textblob

# Help
python main.py --help
```

### Interactive Mode

```bash
python main.py
```

Then follow the prompts:
1. Enter your search query
2. Specify number of tweets
3. Choose Top or Latest tweets
4. Select output format(s)
5. Enable/disable sentiment analysis
6. Choose text cleaning option

### Command-Line Arguments

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `query` | - | Search query (interactive mode if omitted) | - |
| `--count` | `-c` | Number of tweets to fetch | 10 |
| `--product` | `-p` | Search type: Top or Latest | Top |
| `--formats` | `-f` | Output formats: csv, xlsx, json | csv |
| `--sentiment` | `-s` | Sentiment analyzer: vader or textblob | vader |
| `--no-sentiment` | - | Skip sentiment analysis | False |
| `--clean-text` | - | Clean tweet text before analysis | False |
| `--help` | - | Show help message | - |

### Search Query Examples

```bash
# Basic keyword search
python main.py "gold price"

# Exact phrase
python main.py '"machine learning"'

# From specific user
python main.py "from:elonmusk"

# User mentions
python main.py "@binance"

# Hashtag search
python main.py "#AI"

# Date range
python main.py "bitcoin since:2024-01-01 until:2024-06-01"

# Complex query
python main.py "(gold OR silver) lang:en -crypto"

# Exclude terms
python main.py "bitcoin -dogecoin"
```

## 📊 Output Format

### CSV/Excel Format
- `id`: Tweet ID
- `text`: Tweet text (cleaned if --clean-text enabled)
- `original_text`: Original tweet text (if --clean-text enabled)
- `created_at`: Timestamp
- `user`: User display name
- `username`: User handle
- `retweet_count`: Number of retweets
- `favorite_count`: Number of likes
- `view_count`: Number of views (if available)
- `reply_count`: Number of replies
- `quote_count`: Number of quotes
- `lang`: Language code
- `sentiment_label`: Sentiment classification (positive/negative/neutral)
- `sentiment_compound`: VADER compound score (-1 to 1)
- `sentiment_positive`: VADER positive score
- `sentiment_negative`: VADER negative score
- `sentiment_neutral`: VADER neutral score

### JSON Format
Same fields as CSV, structured as a JSON array of objects.

## ⚙️ Configuration

Edit `config.json` to customize default settings:

```json
{
  "default_count": 10,
  "default_product": "Top",
  "default_formats": ["csv"],
  "rate_limit_retry_seconds": 900,
  "batch_wait_seconds": [1.2, 2.8],
  "max_query_length": 30,
  "log_level": "INFO",
  "sentiment_analyzer": "vader"
}
```

## 🧪 Testing

Run the included test suite:

```bash
# Run all tests
pytest test_twitter_scrapper.py

# Run with verbose output
pytest test_twitter_scrapper.py -v

# Run specific test class
pytest test_twitter_scrapper.py::TestUtils -v
```

## 📦 Dependencies

- **twikit**: Twitter API client
- **pandas**: Data manipulation
- **openpyxl**: Excel file support
- **textblob**: Alternative sentiment analysis
- **vaderSentiment**: VADER sentiment analysis
- **pytest**: Testing framework

## 🔍 Troubleshooting

### Authentication Errors
- Ensure `cookie.json` exists and contains valid Twitter cookies
- Cookies expire after some time; you may need to re-export them
- Make sure you're logged into Twitter/X in your browser

### Rate Limit Errors
- The tool automatically waits and retries when rate limited
- Default wait time is 15 minutes (900 seconds)
- You can adjust this in `config.json`

### Module Not Found Errors
- Ensure all dependencies are installed: `pip install -r req.txt`
- For TextBlob: Run `python -m textblob.download_corpora`

### No Tweets Found
- Check your search query for typos
- Some queries may be restricted by Twitter
- Try simpler queries first to test connectivity

## 📚 Best Practices

1. **Start Small**: Test with small tweet counts (10-20) before scaling up
2. **Use Specific Queries**: More specific queries yield better results
3. **Respect Rate Limits**: Don't make too many requests in a short time
4. **Data Privacy**: Don't share cookies or personal data
5. **Academic Use**: Always comply with Twitter's Terms of Service

## 🤝 Contributing

This is an educational project. Feel free to fork and improve it for your research needs.

## 📄 License

For educational use only. Please respect Twitter/X Terms of Service.

## 📞 Support

For issues and questions:
- Check the troubleshooting section above
- Review the test cases for usage examples
- Ensure all dependencies are properly installed

---

**Happy researching! 🎓**
