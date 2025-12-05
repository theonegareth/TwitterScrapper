"""
Multi-Account Rotation Manager for TwitterScrapper

⚠️⚠️⚠️ WARNING: LEGAL AND ETHICAL CONSIDERATIONS ⚠️⚠️⚠️

This module implements multi-account rotation for Twitter scraping.

IMPORTANT LEGAL NOTICE:
- Using multiple accounts to circumvent rate limits may violate Twitter's Terms of Service
- This can result in account suspension or permanent bans
- Twitter may detect and block automated multi-account usage
- This tool is for educational purposes only
- Users assume all legal risks and responsibilities

RECOMMENDED ALTERNATIVES (Compliant with ToS):
1. Use Twitter API v2 with elevated access for higher rate limits
2. Implement intelligent rate limiting and backoff strategies
3. Optimize queries to maximize data per request
4. Use single account with respectful scraping intervals

USAGE REQUIREMENTS:
- Each account must be manually created and authenticated
- Accounts should have legitimate, varied usage patterns
- Implement significant delays between account switches
- Monitor account health and stop on first warning
- Respect Twitter's robots.txt and terms of service

By using this module, you acknowledge that you understand and accept
all legal risks and responsibilities.
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import random

from twikit import Client
from twikit.errors import TooManyRequests, Unauthorized

logger = logging.getLogger(__name__)


@dataclass
class AccountStatus:
    """Track status of a Twitter account."""
    account_id: str
    username: str = ""
    is_active: bool = True
    is_banned: bool = False
    rate_limit_hits: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    last_used: Optional[datetime] = None
    cooldown_until: Optional[datetime] = None
    consecutive_failures: int = 0
    warnings_received: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'account_id': self.account_id,
            'username': self.username,
            'is_active': self.is_active,
            'is_banned': self.is_banned,
            'rate_limit_hits': self.rate_limit_hits,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'cooldown_until': self.cooldown_until.isoformat() if self.cooldown_until else None,
            'consecutive_failures': self.consecutive_failures,
            'warnings_received': self.warnings_received
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountStatus':
        """Create from dictionary."""
        if 'last_used' in data and data['last_used']:
            data['last_used'] = datetime.fromisoformat(data['last_used'])
        if 'cooldown_until' in data and data['cooldown_until']:
            data['cooldown_until'] = datetime.fromisoformat(data['cooldown_until'])
        return cls(**data)


class MultiAccountManager:
    """
    Manages multiple Twitter accounts for rotation during scraping.
    
    ⚠️ WARNING: This may violate Twitter's Terms of Service.
    Use at your own legal risk.
    """
    
    def __init__(self, 
                 accounts_config_path: str = "accounts.json",
                 status_tracking_path: str = "account_status.json",
                 min_switch_delay: int = 60,
                 max_consecutive_failures: int = 3,
                 enable_cooldown: bool = True):
        """
        Initialize multi-account manager.
        
        Args:
            accounts_config_path: Path to accounts configuration file
            status_tracking_path: Path to account status tracking file
            min_switch_delay: Minimum seconds between account switches
            max_consecutive_failures: Max failures before disabling account
            enable_cooldown: Whether to enable cooldown periods
        """
        self.accounts_config_path = Path(accounts_config_path)
        self.status_tracking_path = Path(status_tracking_path)
        self.min_switch_delay = min_switch_delay
        self.max_consecutive_failures = max_consecutive_failures
        self.enable_cooldown = enable_cooldown
        
        # Account storage
        self._accounts: Dict[str, Dict[str, Any]] = {}
        self._account_status: Dict[str, AccountStatus] = {}
        self._clients: Dict[str, Client] = {}
        
        # Current account tracking
        self._current_account_id: Optional[str] = None
        self._last_switch_time: Optional[datetime] = None
        
        # Rotation statistics
        self.total_rotations = 0
        self.total_requests = 0
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Load configuration
        self._load_accounts()
        self._load_account_status()
        
        logger.warning("⚠️ Multi-account manager initialized")
        logger.warning("⚠️ Using multiple accounts may violate Twitter's Terms of Service")
        logger.warning("⚠️ Legal risks include account suspension and permanent bans")
        logger.warning(f"⚠️ Loaded {len(self._accounts)} accounts from configuration")
    
    def _load_accounts(self):
        """Load account configurations from file."""
        if not self.accounts_config_path.exists():
            logger.error(f"Accounts configuration not found: {self.accounts_config_path}")
            logger.error("Create accounts.json with account credentials")
            raise FileNotFoundError(f"Accounts configuration not found: {self.accounts_config_path}")
        
        try:
            with open(self.accounts_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self._accounts = config.get('accounts', {})
            
            if not self._accounts:
                logger.error("No accounts found in configuration")
                raise ValueError("No accounts configured")
            
            logger.info(f"Loaded {len(self._accounts)} account configurations")
            
        except Exception as e:
            logger.error(f"Failed to load accounts configuration: {e}")
            raise
    
    def _load_account_status(self):
        """Load or initialize account status tracking."""
        if self.status_tracking_path.exists():
            try:
                with open(self.status_tracking_path, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)
                
                for account_id, status_dict in status_data.items():
                    self._account_status[account_id] = AccountStatus.from_dict(status_dict)
                
                logger.info(f"Loaded status for {len(self._account_status)} accounts")
            except Exception as e:
                logger.warning(f"Failed to load account status: {e}")
                self._initialize_account_status()
        else:
            self._initialize_account_status()
    
    def _initialize_account_status(self):
        """Initialize status tracking for all accounts."""
        for account_id in self._accounts.keys():
            self._account_status[account_id] = AccountStatus(account_id=account_id)
        
        logger.info(f"Initialized status tracking for {len(self._accounts)} accounts")
        self._save_account_status()
    
    def _save_account_status(self):
        """Save account status to file."""
        try:
            status_data = {
                account_id: status.to_dict()
                for account_id, status in self._account_status.items()
            }
            
            with open(self.status_tracking_path, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, indent=2, default=str)
            
            logger.debug("Account status saved")
        except Exception as e:
            logger.error(f"Failed to save account status: {e}")
    
    def get_available_accounts(self) -> List[str]:
        """Get list of available (active and not in cooldown) account IDs."""
        available = []
        now = datetime.now()
        
        for account_id, status in self._account_status.items():
            if not status.is_active or status.is_banned:
                continue
            
            if self.enable_cooldown and status.cooldown_until and status.cooldown_until > now:
                continue
            
            available.append(account_id)
        
        return available
    
    async def get_next_account(self) -> Optional[Tuple[str, Client]]:
        """
        Get the next available account for scraping.
        
        Returns:
            Tuple of (account_id, client) or None if no accounts available
        """
        async with self._lock:
            available_accounts = self.get_available_accounts()
            
            if not available_accounts:
                logger.error("No accounts available for scraping")
                return None
            
            # If current account is still available and minimum switch time hasn't passed, keep it
            if (self._current_account_id and 
                self._current_account_id in available_accounts and
                self._last_switch_time and
                datetime.now() - self._last_switch_time < timedelta(seconds=self.min_switch_delay)):
                
                return self._current_account_id, self._clients[self._current_account_id]
            
            # Select next account (round-robin or random)
            if self._current_account_id and self._current_account_id in available_accounts:
                # Round-robin: find next in list
                current_index = available_accounts.index(self._current_account_id)
                next_index = (current_index + 1) % len(available_accounts)
                next_account_id = available_accounts[next_index]
            else:
                # Random selection
                next_account_id = random.choice(available_accounts)
            
            # Switch accounts
            old_account = self._current_account_id
            self._current_account_id = next_account_id
            self._last_switch_time = datetime.now()
            self.total_rotations += 1
            
            if old_account and old_account != next_account_id:
                logger.info(f"Switched from account {old_account} to {next_account_id}")
                logger.warning(f"⚠️ Account rotation #{self.total_rotations} - Legal risk increased")
            
            return next_account_id, self._clients[next_account_id]
    
    async def record_request(self, account_id: str, success: bool, response_time: float = 0):
        """Record a request attempt for an account."""
        async with self._lock:
            status = self._account_status[account_id]
            
            status.total_requests += 1
            status.last_used = datetime.now()
            
            if success:
                status.successful_requests += 1
                status.consecutive_failures = 0
            else:
                status.consecutive_failures += 1
                
                # Disable account if too many consecutive failures
                if status.consecutive_failures >= self.max_consecutive_failures:
                    logger.warning(f"Disabling account {account_id} due to {status.consecutive_failures} consecutive failures")
                    status.is_active = False
            
            self.total_requests += 1
            self._save_account_status()
    
    async def record_rate_limit_hit(self, account_id: str):
        """Record a rate limit hit for an account."""
        async with self._lock:
            status = self._account_status[account_id]
            status.rate_limit_hits += 1
            
            # Put account in cooldown
            if self.enable_cooldown:
                # Exponential backoff based on hit count
                cooldown_minutes = min(15 * (2 ** (status.rate_limit_hits - 1)), 120)
                status.cooldown_until = datetime.now() + timedelta(minutes=cooldown_minutes)
                
                logger.warning(f"Account {account_id} hit rate limit #{status.rate_limit_hits}")
                logger.warning(f"Account {account_id} in cooldown until {status.cooldown_until}")
            
            self._save_account_status()
    
    async def record_warning(self, account_id: str, warning_message: str):
        """Record a warning received for an account."""
        async with self._lock:
            status = self._account_status[account_id]
            status.warnings_received += 1
            
            logger.warning(f"Account {account_id} received warning #{status.warnings_received}: {warning_message}")
            
            # Consider disabling account after multiple warnings
            if status.warnings_received >= 2:
                logger.error(f"Account {account_id} has {status.warnings_received} warnings - consider disabling")
                # Don't auto-disable, but log strongly worded warning
            
            self._save_account_status()
    
    async def mark_account_banned(self, account_id: str, reason: str = ""):
        """Mark an account as banned."""
        async with self._lock:
            status = self._account_status[account_id]
            status.is_banned = True
            status.is_active = False
            
            logger.error(f"Account {account_id} marked as banned: {reason}")
            logger.error(f"⚠️ ACCOUNT BANNED: {account_id} - {reason}")
            logger.error("⚠️ This demonstrates the risk of multi-account scraping")
            
            self._save_account_status()
    
    def get_account_stats(self, account_id: str) -> Optional[AccountStatus]:
        """Get statistics for a specific account."""
        return self._account_status.get(account_id)
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all accounts."""
        return {
            account_id: status.to_dict()
            for account_id, status in self._account_status.items()
        }
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Get overall multi-account statistics."""
        total_accounts = len(self._accounts)
        active_accounts = sum(1 for s in self._account_status.values() if s.is_active)
        banned_accounts = sum(1 for s in self._account_status.values() if s.is_banned)
        
        total_requests = sum(s.total_requests for s in self._account_status.values())
        total_successful = sum(s.successful_requests for s in self._account_status.values())
        total_rate_limits = sum(s.rate_limit_hits for s in self._account_status.values())
        total_warnings = sum(s.warnings_received for s in self._account_status.values())
        
        success_rate = (total_successful / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'total_accounts': total_accounts,
            'active_accounts': active_accounts,
            'banned_accounts': banned_accounts,
            'total_requests': total_requests,
            'total_successful': total_successful,
            'success_rate_percent': round(success_rate, 2),
            'total_rate_limits': total_rate_limits,
            'total_warnings': total_warnings,
            'total_rotations': self.total_rotations,
            'current_account': self._current_account_id,
            'legal_warning': 'Multi-account scraping may violate Twitter Terms of Service'
        }
    
    async def rotate_on_rate_limit(self) -> Optional[Tuple[str, Client]]:
        """
        Rotate to next account when rate limited.
        
        Returns:
            Next account tuple or None if no accounts available
        """
        if self._current_account_id:
            await self.record_rate_limit_hit(self._current_account_id)
        
        # Wait minimum switch delay
        if self._last_switch_time:
            time_since_switch = (datetime.now() - self._last_switch_time).total_seconds()
            if time_since_switch < self.min_switch_delay:
                wait_time = self.min_switch_delay - time_since_switch
                logger.info(f"Waiting {wait_time:.1f}s before account rotation")
                await asyncio.sleep(wait_time)
        
        next_account = await self.get_next_account()
        
        if next_account and next_account[0] == self._current_account_id:
            logger.warning("No other accounts available, continuing with same account")
        
        return next_account


# Example accounts.json configuration template
ACCOUNTS_CONFIG_TEMPLATE = {
    "warning": "⚠️ Multi-account usage may violate Twitter's Terms of Service",
    "legal_notice": "Use at your own risk. Account suspension or bans may occur.",
    "recommendation": "Consider using Twitter API v2 with elevated access instead",
    "accounts": {
        "account_1": {
            "cookies_path": "cookies_account_1.json",
            "description": "Primary account",
            "created_at": "2024-01-01"
        },
        "account_2": {
            "cookies_path": "cookies_account_2.json",
            "description": "Secondary account",
            "created_at": "2024-01-15"
        }
    },
    "settings": {
        "min_switch_delay_seconds": 60,
        "max_consecutive_failures": 3,
        "enable_cooldown": True,
        "rotation_strategy": "round_robin"
    }
}


# Example usage
async def example_usage():
    """Example of how to use the multi-account manager."""
    
    print("⚠️⚠️⚠️ WARNING: Multi-account scraping may violate Twitter's Terms of Service ⚠️⚠️⚠️")
    print("⚠️ This can result in account suspension or permanent bans")
    print("⚠️ Use at your own legal risk")
    print()
    
    # Create manager
    manager = MultiAccountManager()
    
    # Initialize clients
    await manager.initialize_clients()
    
    # Get first account
    account_info = await manager.get_next_account()
    if not account_info:
        print("No accounts available")
        return
    
    account_id, client = account_info
    print(f"Using account: {account_id}")
    
    # Simulate scraping with rate limit handling
    try:
        # ... scraping code ...
        success = True
        await manager.record_request(account_id, success=success)
        
    except TooManyRequests:
        print(f"Rate limited on account {account_id}")
        
        # Rotate to next account
        new_account_info = await manager.rotate_on_rate_limit()
        if new_account_info:
            new_account_id, new_client = new_account_info
            print(f"Rotated to account: {new_account_id}")
        else:
            print("No accounts available for rotation")
    
    # Print statistics
    stats = manager.get_overall_stats()
    print("\nMulti-Account Statistics:")
    print(f"Total accounts: {stats['total_accounts']}")
    print(f"Active accounts: {stats['active_accounts']}")
    print(f"Banned accounts: {stats['banned_accounts']}")
    print(f"Total requests: {stats['total_requests']}")
    print(f"Success rate: {stats['success_rate_percent']}%")
    print(f"Total rotations: {stats['total_rotations']}")


if __name__ == "__main__":
    # Create template configuration file
    template_path = Path("accounts.json.template")
    if not template_path.exists():
        with open(template_path, 'w', encoding='utf-8') as f:
            json.dump(ACCOUNTS_CONFIG_TEMPLATE, f, indent=2)
        print(f"Created template configuration: {template_path}")
        print("⚠️ Review legal warnings before using multi-account features")
    
    # Run example
    asyncio.run(example_usage())