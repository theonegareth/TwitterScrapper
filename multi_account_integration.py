"""
Integration module for multi-account scraping with TwitterScrapper

⚠️⚠️⚠️ WARNING: LEGAL AND ETHICAL CONSIDERATIONS ⚠️⚠️⚠️

This module integrates multi-account rotation with the TwitterScrapper.
Using multiple accounts to circumvent rate limits may violate Twitter's Terms of Service.

LEGAL RISKS:
- Account suspension or permanent bans
- IP address blocking
- Legal action from Twitter/X Corp
- Violation of computer fraud and abuse laws in some jurisdictions

RECOMMENDED ALTERNATIVES:
1. Use Twitter API v2 with elevated access
2. Implement intelligent rate limiting
3. Optimize queries for maximum efficiency
4. Use single account with respectful intervals

By using this module, you acknowledge and accept all legal risks.
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

from multi_account import MultiAccountManager
from search import search
from monitoring import metrics
from health_checks import health_checker

logger = logging.getLogger(__name__)


class MultiAccountScraper:
    """
    Twitter scraper with multi-account rotation support.
    
    ⚠️ WARNING: May violate Twitter's Terms of Service
    """
    
    def __init__(self, 
                 accounts_config_path: str = "accounts.json",
                 enable_auto_rotation: bool = True,
                 max_retries_per_query: int = 3):
        """
        Initialize multi-account scraper.
        
        Args:
            accounts_config_path: Path to accounts configuration
            enable_auto_rotation: Enable automatic account rotation on rate limits
            max_retries_per_query: Maximum retries per query across accounts
        """
        self.accounts_config_path = accounts_config_path
        self.enable_auto_rotation = enable_auto_rotation
        self.max_retries_per_query = max_retries_per_query
        
        # Initialize multi-account manager
        self.account_manager = MultiAccountManager(accounts_config_path)
        
        # Statistics
        self.total_queries = 0
        self.successful_queries = 0
        self.total_rotations = 0
        
        logger.warning("⚠️ Multi-account scraper initialized")
        logger.warning("⚠️ Legal risk: May violate Twitter Terms of Service")
    
    async def initialize(self):
        """Initialize the scraper and accounts."""
        logger.info("Initializing multi-account scraper")
        
        # Initialize account clients
        await self.account_manager.initialize_clients()
        
        # Register health check
        health_checker.register_check(
            'multi_account', 
            self._health_check
        )
        
        logger.info("Multi-account scraper initialization complete")
    
    async def scrape_with_rotation(self, 
                                   query: str, 
                                   count: int = 10, 
                                   product: str = 'Top',
                                   max_tweets_per_account: int = 100) -> List[Dict[str, Any]]:
        """
        Scrape tweets with automatic account rotation on rate limits.
        
        Args:
            query: Search query
            count: Number of tweets to fetch
            product: Search product type ('Top' or 'Latest')
            max_tweets_per_account: Maximum tweets per account before rotation
            
        Returns:
            List of tweet dictionaries
        """
        self.total_queries += 1
        
        all_tweets = []
        remaining = count
        retry_count = 0
        
        logger.info(f"Starting multi-account scrape for '{query}' (target: {count} tweets)")
        
        while remaining > 0 and retry_count < self.max_retries_per_query:
            # Get next available account
            account_info = await self.account_manager.get_next_account()
            
            if not account_info:
                logger.error("No accounts available for scraping")
                break
            
            account_id, client = account_info
            
            try:
                logger.info(f"Scraping with account {account_id} (remaining: {remaining} tweets)")
                
                # Calculate how many tweets to fetch with this account
                fetch_count = min(remaining, max_tweets_per_account)
                
                # Attempt to scrape
                tweets = await search(client, query, count=fetch_count, product=product)
                
                if tweets:
                    all_tweets.extend(tweets)
                    remaining -= len(tweets)
                    
                    # Record successful request
                    await self.account_manager.record_request(account_id, success=True)
                    
                    logger.info(f"Account {account_id} fetched {len(tweets)} tweets")
                    
                    # If we got all requested tweets, break
                    if remaining <= 0:
                        break
                
                # If we didn't get enough tweets, continue with same account or rotate
                if len(tweets) < fetch_count:
                    logger.info(f"Account {account_id} returned fewer tweets than requested")
                    
                    # Check if we should rotate based on low results
                    if self.enable_auto_rotation and len(self.account_manager.get_available_accounts()) > 1:
                        logger.info("Rotating to next account due to low results")
                        await asyncio.sleep(self.account_manager.min_switch_delay)
                
            except Exception as e:
                logger.error(f"Error scraping with account {account_id}: {e}")
                
                # Record failed request
                await self.account_manager.record_request(account_id, success=False)
                
                # Check if we should rotate
                if self.enable_auto_rotation:
                    if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
                        logger.warning(f"Rate limited on account {account_id}, rotating...")
                        await self.account_manager.record_rate_limit_hit(account_id)
                        
                        # Rotate to next account
                        new_account_info = await self.account_manager.rotate_on_rate_limit()
                        if new_account_info:
                            self.total_rotations += 1
                            logger.info(f"Rotated to account: {new_account_info[0]}")
                        else:
                            logger.error("No accounts available for rotation")
                            break
                    else:
                        logger.error(f"Non-rate-limit error, stopping scrape: {e}")
                        break
                else:
                    logger.error(f"Error on account {account_id}, auto-rotation disabled")
                    break
                
                retry_count += 1
        
        # Record overall success
        if all_tweets:
            self.successful_queries += 1
        
        logger.info(f"Scrape complete: fetched {len(all_tweets)} tweets across {self.total_rotations + 1} accounts")
        
        # Log statistics
        stats = self.account_manager.get_overall_stats()
        metrics.record_multi_account_stats(stats)
        
        return all_tweets
    
    async def scrape_single_account(self, 
                                    query: str, 
                                    count: int = 10, 
                                    product: str = 'Top',
                                    account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Scrape using a single specified account without rotation.
        
        Args:
            query: Search query
            count: Number of tweets to fetch
            product: Search product type
            account_id: Specific account to use (None for next available)
            
        Returns:
            List of tweet dictionaries
        """
        if account_id:
            if account_id not in self.account_manager._clients:
                raise ValueError(f"Account {account_id} not found")
            client = self.account_manager._clients[account_id]
            used_account_id = account_id
        else:
            account_info = await self.account_manager.get_next_account()
            if not account_info:
                raise RuntimeError("No accounts available")
            used_account_id, client = account_info
        
        try:
            logger.info(f"Scraping with single account {used_account_id}")
            
            tweets = await search(client, query, count=count, product=product)
            
            # Record success
            await self.account_manager.record_request(used_account_id, success=True)
            
            logger.info(f"Account {used_account_id} fetched {len(tweets)} tweets")
            
            return tweets
            
        except Exception as e:
            logger.error(f"Error with account {used_account_id}: {e}")
            
            # Record failure
            await self.account_manager.record_request(used_account_id, success=False)
            
            if "rate limit" in str(e).lower():
                await self.account_manager.record_rate_limit_hit(used_account_id)
            
            raise
    
    def get_account_statistics(self) -> Dict[str, Any]:
        """Get comprehensive account statistics."""
        stats = self.account_manager.get_overall_stats()
        
        # Add scraper-specific stats
        stats.update({
            'scraper_total_queries': self.total_queries,
            'scraper_successful_queries': self.successful_queries,
            'scraper_success_rate': (self.successful_queries / self.total_queries * 100) if self.total_queries > 0 else 0,
            'scraper_total_rotations': self.total_rotations,
            'auto_rotation_enabled': self.enable_auto_rotation,
            'max_retries_per_query': self.max_retries_per_query
        })
        
        return stats
    
    def print_account_warning(self):
        """Print legal warning about multi-account usage."""
        print("=" * 80)
        print("⚠️⚠️⚠️ LEGAL WARNING: Multi-Account Scraping ⚠️⚠️⚠️")
        print("=" * 80)
        print()
        print("Using multiple accounts to circumvent rate limits may violate:")
        print("- Twitter's Terms of Service")
        print("- Twitter Developer Agreement")
        print("- Computer fraud and abuse laws (in some jurisdictions)")
        print()
        print("POTENTIAL CONSEQUENCES:")
        print("- Account suspension or permanent bans")
        print("- IP address blocking")
        print("- Legal action from Twitter/X Corp")
        print()
        print("RECOMMENDED ALTERNATIVES:")
        print("1. Use Twitter API v2 with elevated access")
        print("2. Implement intelligent rate limiting")
        print("3. Optimize queries for efficiency")
        print("4. Use single account with respectful intervals")
        print()
        print("By continuing, you acknowledge and accept all legal risks.")
        print("This tool is for educational purposes only.")
        print("=" * 80)
        print()
    
    async def _health_check(self):
        """Health check for multi-account system."""
        from health_checks import HealthCheckResult, HealthStatus
        
        available_accounts = self.account_manager.get_available_accounts()
        total_accounts = len(self.account_manager._accounts)
        
        if len(available_accounts) == 0:
            return HealthCheckResult(
                name='multi_account',
                status=HealthStatus.UNHEALTHY,
                message="No accounts available for scraping",
                response_time=0,
                details={
                    'total_accounts': total_accounts,
                    'available_accounts': 0,
                    'legal_warning': 'Multi-account usage has legal risks'
                }
            )
        
        elif len(available_accounts) < total_accounts / 2:
            return HealthCheckResult(
                name='multi_account',
                status=HealthStatus.WARNING,
                message=f"Only {len(available_accounts)}/{total_accounts} accounts available",
                response_time=0,
                details={
                    'total_accounts': total_accounts,
                    'available_accounts': len(available_accounts),
                    'banned_accounts': sum(1 for s in self.account_manager._account_status.values() if s.is_banned)
                }
            )
        
        else:
            return HealthCheckResult(
                name='multi_account',
                status=HealthStatus.HEALTHY,
                message=f"{len(available_accounts)}/{total_accounts} accounts available",
                response_time=0,
                details={
                    'total_accounts': total_accounts,
                    'available_accounts': len(available_accounts)
                }
            )


# Integration with main scraper
async def scrape_with_multi_account(query: str, 
                                    count: int = 10,
                                    product: str = 'Top',
                                    accounts_config: str = "accounts.json",
                                    enable_rotation: bool = True) -> List[Dict[str, Any]]:
    """
    Convenience function to scrape with multi-account support.
    
    Args:
        query: Search query
        count: Number of tweets to fetch
        product: Search product type
        accounts_config: Path to accounts configuration
        enable_rotation: Enable automatic account rotation
        
    Returns:
        List of tweet dictionaries
    """
    scraper = MultiAccountScraper(
        accounts_config_path=accounts_config,
        enable_auto_rotation=enable_rotation
    )
    
    # Print legal warning
    scraper.print_account_warning()
    
    # Confirm user wants to proceed
    response = input("Do you understand and accept the legal risks? (yes/no): ")
    if response.lower() != 'yes':
        print("Scraping cancelled due to legal risk acknowledgment.")
        return []
    
    # Initialize and scrape
    await scraper.initialize()
    
    tweets = await scraper.scrape_with_rotation(
        query=query,
        count=count,
        product=product
    )
    
    # Print statistics
    stats = scraper.get_account_statistics()
    print("\nScraping Complete:")
    print(f"Total tweets fetched: {len(tweets)}")
    print(f"Accounts used: {stats['scraper_total_rotations'] + 1}")
    print(f"Success rate: {stats['scraper_success_rate']:.1f}%")
    
    return tweets


if __name__ == "__main__":
    # Example usage
    async def main():
        # Scrape with multi-account rotation
        tweets = await scrape_with_multi_account(
            query="bitcoin",
            count=50,
            product="Top",
            accounts_config="accounts.json",
            enable_rotation=True
        )
        
        print(f"\nFetched {len(tweets)} tweets")
        
        if tweets:
            print("\nFirst tweet:")
            print(json.dumps(tweets[0], indent=2, default=str))
    
    asyncio.run(main())