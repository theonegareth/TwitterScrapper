import argparse
import asyncio
import logging
from auth import getClient
from search import search, csvSave, jsonSave, xlsxSave
from sentiment import SentimentAnalyzer
from utils import setup_logging, load_config, clean_tweet_text
from database import DatabaseManager
from cache import search_cache, sentiment_cache

async def main():
    # Setup logging
    logger = setup_logging()
    
    # Initialize database and cache
    db = DatabaseManager()
    logger.info("Database manager initialized")
    
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
    parser.add_argument('-f', '--formats', choices=['csv', 'xlsx', 'json', 'db'], nargs='+', default=['csv'],
                       help='Output formats: csv, xlsx, json, db (default: csv)')
    parser.add_argument('-s', '--sentiment', choices=['vader', 'textblob'], default='vader',
                       help='Sentiment analysis method (default: vader)')
    parser.add_argument('--no-sentiment', action='store_true', help='Skip sentiment analysis')
    parser.add_argument('--clean-text', action='store_true', help='Clean tweet text before analysis')
    parser.add_argument('--use-cache', action='store_true', help='Use cached results if available')
    parser.add_argument('--save-to-db', action='store_true', help='Save results to database')
    parser.add_argument('--export-from-db', action='store_true', help='Export existing data from database')
    
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
    
    # Handle database export
    if args.export_from_db:
        print("Exporting data from database...")
        tweets = db.get_tweets()
        if tweets:
            filename = f"db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, indent=2, default=str)
            print(f"✓ Exported {len(tweets)} tweets to {filename}")
        else:
            print("No tweets found in database")
        return
    
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
        format_input = input(f'Output formats (csv, xlsx, json, db, default: {",".join(config["default_formats"])}): ').strip()
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
        
        # Ask about caching
        cache_input = input(f'Use cache if available? (y/n, default: n): ').strip().lower()
        use_cache = cache_input == 'y'
        
        # Ask about database storage
        db_input = input(f'Save results to database? (y/n, default: n): ').strip().lower()
        save_to_db = db_input == 'y'
        
    else:
        # Use command line arguments
        query = args.query
        count = args.count
        product = args.product
        formats = args.formats
        no_sentiment = args.no_sentiment
        clean_text = args.clean_text
        use_cache = args.use_cache
        save_to_db = args.save_to_db
    
    # Check cache first if enabled
    cached_tweets = None
    if use_cache:
        logger.info("Checking cache for existing results")
        cached_tweets = search_cache.get_search_results(query, count, product)
        if cached_tweets:
            logger.info(f"Found {len(cached_tweets)} cached tweets")
            print(f"✓ Using cached results ({len(cached_tweets)} tweets)")
    
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
    
    # Use cached tweets or search for new ones
    if cached_tweets:
        tweets = cached_tweets
    else:
        try:
            logger.info(f"Searching for '{query}' - {count} {product} tweets")
            tweets = await search(client, query, count=count, product=product)
            
            if not tweets:
                logger.warning("No tweets found")
                print("No tweets found for the given query.")
                return
            
            # Cache the results
            if use_cache:
                search_cache.set_search_results(query, count, product, tweets)
                logger.info("Search results cached")
                
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
                
                # Check sentiment cache first
                cached_sentiment = sentiment_cache.get_sentiment(text_for_analysis, method)
                if cached_sentiment:
                    sentiment = cached_sentiment
                    logger.debug("Using cached sentiment analysis")
                else:
                    sentiment = sentiment_analyzer.analyze(text_for_analysis)
                    sentiment_cache.set_sentiment(text_for_analysis, method, sentiment)
                    logger.debug("Sentiment analysis cached")
                
                processed_tweet.update({f'sentiment_{k}': v for k, v in sentiment.items()})
                processed_tweet['sentiment_method'] = method
                
            except Exception as e:
                logger.debug(f"Sentiment analysis failed for tweet {tweet['id']}: {e}")
                processed_tweet.update({
                    'sentiment_label': 'neutral',
                    'sentiment_compound': 0.0
                })
        
        processed_tweets.append(processed_tweet)
    
    # Save to database if requested
    if save_to_db:
        try:
            inserted = db.insert_tweets_batch(processed_tweets, query)
            print(f"✓ Saved {inserted} tweets to database")
            db.record_search(query, count, product, inserted)
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            print(f"Warning: Could not save to database: {e}")
    
    # Save tweets to files
    format_map = {
        'csv': csvSave,
        'xlsx': xlsxSave,
        'json': jsonSave
    }
    
    saved_files = []
    for fmt in formats:
        if fmt == 'db':
            continue  # Already handled above
        
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
    if save_to_db:
        print("Database: Saved")
    if not no_sentiment:
        print(f"Sentiment analysis: Enabled ({method})")
    else:
        print("Sentiment analysis: Disabled")
    if use_cache:
        print("Cache: Used" if cached_tweets else "Cache: Updated")
    print("=" * 50)
    
    # Close database connection
    db.close()

if __name__ == '__main__':
    asyncio.run(main())