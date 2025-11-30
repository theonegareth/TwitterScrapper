import argparse
import asyncio
import logging
from auth import getClient
from search import search, csvSave, jsonSave, xlsxSave
from sentiment import SentimentAnalyzer
from utils import setup_logging, load_config, clean_tweet_text

async def main():
    # Setup logging
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Twitter Scraper with Sentiment Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Advanced Search Examples:
  gold price                      → basic keyword search
  "gold price"                    → exact phrase
  from:elonmusk                   → tweets from a user
  @binance                        → tweets mentioning a user
  #AI                             → hashtag search
  bitcoin since:2024-01-01        → tweets since a date
  gold until:2024-06-01           → tweets before a date
  (gold OR silver) lang:en        → multiple keywords, only English
  bitcoin -dogecoin               → include bitcoin, exclude dogecoin
  (gold OR bitcoin) since:2024-01-01 until:2024-02-01 lang:en → full advanced search
        '''
    )
    
    parser.add_argument('query', nargs='?', help='Search query (interactive mode if not provided)')
    parser.add_argument('-c', '--count', type=int, default=10, help='Number of tweets to fetch (default: 10)')
    parser.add_argument('-p', '--product', choices=['Top', 'Latest'], default='Top', help='Search product type (default: Top)')
    parser.add_argument('-f', '--formats', choices=['csv', 'xlsx', 'json'], nargs='+', default=['csv'],
                       help='Output formats: csv, xlsx, json (default: csv)')
    parser.add_argument('-s', '--sentiment', choices=['vader', 'textblob'], default='vader',
                       help='Sentiment analysis method (default: vader)')
    parser.add_argument('--no-sentiment', action='store_true', help='Skip sentiment analysis')
    parser.add_argument('--clean-text', action='store_true', help='Clean tweet text before analysis')
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.warning(f"Could not load config file: {e}. Using defaults.")
        config = {
            "default_count": 10,
            "default_product": "Top",
            "default_formats": ["csv"],
            "rate_limit_retry_seconds": 900,
            "batch_wait_seconds": [1.2, 2.8],
            "max_query_length": 30,
            "log_level": "INFO",
            "sentiment_analyzer": "vader"
        }
    
    # Interactive mode if no query provided
    if not args.query:
        print("Twitter Scraper with Sentiment Analysis")
        print("=" * 40)
        print("Enter '-quit' to exit, '-help' for search examples")
        print()
        
        while True:
            query = input("Enter your search query: ").strip()
            
            if not query:
                print('Please enter a search query.\n')
                continue
                
            if query.lower() == '-quit':
                print('Goodbye!')
                return
                
            if query.lower() == '-help':
                parser.print_help()
                print()
                continue
                
            break
        
        # Ask for count if using interactive mode
        count_input = input(f'How many tweets to fetch (default: {config["default_count"]}): ').strip()
        count = int(count_input) if count_input.isdigit() else config["default_count"]
        
        # Ask for product type
        product_input = input(f'Search Top or Latest tweets? (default: {config["default_product"]}): ').strip().capitalize()
        product = product_input if product_input in ('Top', 'Latest') else config["default_product"]
        
        # Ask for formats
        format_input = input(f'Output formats (csv, xlsx, json, default: {",".join(config["default_formats"])}): ').strip()
        if format_input:
            formats = [f.strip() for f in format_input.split(',')]
        else:
            formats = config["default_formats"]
            
        # Ask about sentiment analysis
        sentiment_input = input(f'Perform sentiment analysis? (y/n, default: y): ').strip().lower()
        no_sentiment = sentiment_input == 'n'
        
        # Ask about text cleaning
        clean_input = input(f'Clean tweet text? (y/n, default: n): ').strip().lower()
        clean_text = clean_input == 'y'
        
    else:
        # Use command line arguments
        query = args.query
        count = args.count
        product = args.product
        formats = args.formats
        no_sentiment = args.no_sentiment
        clean_text = args.clean_text
    
    # Initialize client
    try:
        client = getClient()
        logger.info("Twitter client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Twitter client: {e}")
        print(f"Error: Could not authenticate with Twitter. Please check your cookies.json file.")
        return
    
    # Initialize sentiment analyzer if needed
    sentiment_analyzer = None
    if not no_sentiment:
        try:
            method = args.sentiment if args.query else config.get("sentiment_analyzer", "vader")
            sentiment_analyzer = SentimentAnalyzer(method)
            logger.info(f"Sentiment analyzer initialized using {method}")
        except Exception as e:
            logger.warning(f"Could not initialize sentiment analyzer: {e}")
            print(f"Warning: Sentiment analysis disabled: {e}")
            no_sentiment = True
    
    # Search for tweets
    try:
        logger.info(f"Searching for '{query}' - {count} {product} tweets")
        tweets = await search(client, query, count=count, product=product)
        
        if not tweets:
            logger.warning("No tweets found")
            print("No tweets found for the given query.")
            return
            
    except Exception as e:
        logger.error(f"Search failed: {e}")
        print(f"Error: Search failed: {e}")
        return
    
    # Process tweets (clean text and add sentiment)
    processed_tweets = []
    for tweet in tweets:
        processed_tweet = tweet.copy()
        
        # Clean text if requested
        if clean_text:
            processed_tweet['original_text'] = tweet['text']
            processed_tweet['text'] = clean_tweet_text(tweet['text'])
        
        # Add sentiment analysis
        if not no_sentiment and sentiment_analyzer:
            try:
                text_for_analysis = processed_tweet.get('text', tweet['text'])
                sentiment = sentiment_analyzer.analyze(text_for_analysis)
                processed_tweet.update({f'sentiment_{k}': v for k, v in sentiment.items()})
            except Exception as e:
                logger.debug(f"Sentiment analysis failed for tweet {tweet['id']}: {e}")
                processed_tweet.update({
                    'sentiment_label': 'neutral',
                    'sentiment_compound': 0.0
                })
        
        processed_tweets.append(processed_tweet)
    
    # Save tweets
    format_map = {
        'csv': csvSave,
        'xlsx': xlsxSave,
        'json': jsonSave
    }
    
    saved_files = []
    for fmt in formats:
        if fmt in format_map:
            try:
                format_map[fmt](processed_tweets, query)
                saved_files.append(fmt)
            except Exception as e:
                logger.error(f"Failed to save {fmt.upper()}: {e}")
                print(f"Error: Could not save {fmt.upper()} file")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"Search completed!")
    print(f"Query: {query}")
    print(f"Tweets fetched: {len(processed_tweets)}")
    print(f"Formats saved: {', '.join(saved_files).upper()}")
    if not no_sentiment:
        print(f"Sentiment analysis: Enabled ({args.sentiment if args.query else config.get('sentiment_analyzer', 'vader')})")
    else:
        print("Sentiment analysis: Disabled")
    print("=" * 50)

if __name__ == '__main__':
    asyncio.run(main())