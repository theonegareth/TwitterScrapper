import asyncio
import csv
import json
import random
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from twikit.errors import NotFound, TooManyRequests
import logging

from utils import safe_filename

logger = logging.getLogger(__name__)

async def search(client, query: str, count: int = 10, product: str = 'Top', retry_wait: int = 900) -> List[Dict[str, Any]]:
    """Search for tweets using the provided client."""
    tweets_data = []
    
    logger.info(f'Searching for: {query}')
    logger.info(f'Fetching {count} {product} tweets...')
    
    remaining = max(count, 1)
    batch_index = 0
    result = None
    seen_ids: set[str] = set()

    try:
        while remaining > 0:
            try:
                fetch_size = min(20, remaining)

                if result is None:
                    result = await client.search_tweet(query, product=product, count=fetch_size)
                else:
                    if not getattr(result, 'next_cursor', None):
                        break
                    wait_for = random.uniform(1.2, 2.8)
                    logger.info(f'Waiting {wait_for:.1f}s before next batch...')
                    await asyncio.sleep(wait_for)
                    result = await result.next()

                if not result:
                    break

                batch_index += 1
                logger.info(f'Processing batch {batch_index} (size {len(result)})')

                for tweet in result:
                    if tweet.id in seen_ids:
                        continue
                    seen_ids.add(tweet.id)
                    data = {
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'user': tweet.user.name,
                        'username': tweet.user.screen_name,
                        'retweet_count': tweet.retweet_count,
                        'favorite_count': tweet.favorite_count,
                        'view_count': getattr(tweet, 'view_count', 0),
                        'reply_count': getattr(tweet, 'reply_count', 0),
                        'quote_count': getattr(tweet, 'quote_count', 0),
                        'lang': tweet.lang,
                    }
                    tweets_data.append(data)
                    logger.debug(f'{len(tweets_data)}. @{data["username"]}: {data["text"][:50]}...')

                remaining = count - len(tweets_data)
                if not getattr(result, 'next_cursor', None):
                    break

            except TooManyRequests:
                logger.warning(f'Rate limit hit. Waiting {retry_wait} seconds before retrying...')
                await asyncio.sleep(retry_wait)
                continue

    except NotFound:
        logger.error('Twitter returned 404 for this search. The query may be restricted or the session expired.')

    logger.info(f'Fetched {len(tweets_data)} tweets')
    return tweets_data


def _get_filename(query: str, ext: str) -> str:
    """Generate filename for saved tweets."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_query = safe_filename(query)
    return f'tweets_{safe_query}_{timestamp}.{ext}'


def csvSave(tweets: List[Dict[str, Any]], query: str) -> None:
    """Save tweets to CSV file."""
    if not tweets:
        logger.warning('No tweets to save')
        return
    
    filename = _get_filename(query, 'csv')
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=tweets[0].keys())
            writer.writeheader()
            writer.writerows(tweets)
        logger.info(f'✓ Saved to {filename}')
    except Exception as e:
        logger.error(f'Failed to save CSV: {e}')


def jsonSave(tweets: List[Dict[str, Any]], query: str) -> None:
    """Save tweets to JSON file."""
    if not tweets:
        logger.warning('No tweets to save')
        return
    
    filename = _get_filename(query, 'json')
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f'✓ Saved to {filename}')
    except Exception as e:
        logger.error(f'Failed to save JSON: {e}')


def xlsxSave(tweets: List[Dict[str, Any]], query: str) -> None:
    """Save tweets to XLSX file."""
    try:
        import openpyxl
        from openpyxl import Workbook
    except ImportError:
        logger.error('Error: openpyxl not installed. Install with: pip install openpyxl')
        return
    
    if not tweets:
        logger.warning('No tweets to save')
        return
    
    filename = _get_filename(query, 'xlsx')
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = 'Tweets'
        headers = list(tweets[0].keys())
        ws.append(headers)
        for tweet in tweets:
            ws.append(list(tweet.values()))
        wb.save(filename)
        logger.info(f'✓ Saved to {filename}')
    except Exception as e:
        logger.error(f'Failed to save XLSX: {e}')
