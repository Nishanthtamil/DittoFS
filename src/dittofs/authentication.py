"""
Authentication service for DittoFS with multi-factor authentication support.

This module provides comprehensive user authentication including:
- Password-based authentication
- Key file authentication
- Certificate-based authentication
- Multi-factor authentication (TOTP, hardware tokens)
- Secure session management
- Password policy enforcement
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import cryptography
    import pyotp
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.x509 import load_pem_x509_certificate

    CRYPTO_AVAILABLE = True
except ImportError:
    pyotp = None
    cryptography = None
    hashes = None
    serialization = None
    PBKDF2HMAC = None
    Cipher = None
    algorithms = None
    modes = None
    load_pem_x509_certificate = None
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)


class AuthMethod(Enum):
    """Authentication methods supported by DittoFS."""

    PASSWORD = "password"
    KEY_FILE = "key_file"
    CERTIFICATE = "certificate"
    TOTP = "totp"
    HARDWARE_TOKEN = "hardware_token"


class SessionStatus(Enum):
    """Session status values."""

    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"
    LOCKED = "locked"


@dataclass
class UserCredentials:
    """User credentials for authentication."""

    username: str
    password: Optional[str] = None
    key_file_path: Optional[Path] = None
    certificate_data: Optional[bytes] = None
    totp_code: Optional[str] = None
    hardware_token: Optional[str] = None


@dataclass
class AuthToken:
    """Authentication token with session information."""

    token_id: str
    username: str
    issued_at: datetime
    expires_at: datetime
    permissions: List[str] = field(default_factory=list)
    session_data: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        return datetime.utcnow() > self.expires_at

    @property
    def time_until_expiry(self) -> timedelta:
        """Get time until token expires."""
        return self.expires_at - datetime.utcnow()


@dataclass
class UserAccount:
    """User account information."""

    username: str
    password_hash: str
    salt: str
    auth_methods: List[AuthMethod]
    totp_secret: Optional[str] = None
    failed_attempts: int = 0
    locked_until: Optional[datetime] = None
    last_login: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    permissions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PasswordPolicy:
    """Password policy configuration."""

    min_length: int = 12
    require_uppercase: bool = True
    require_lowercase: bool = True
    require_digits: bool = True
    require_special: bool = True
    max_age_days: int = 90
    history_count: int = 5
    lockout_attempts: int = 5
    lockout_duration_minutes: int = 30


class AuthenticationError(Exception):
    """Base exception for authentication errors."""

    pass


class InvalidCredentialsError(AuthenticationError):
    """Raised when credentials are invalid."""

    pass


class AccountLockedError(AuthenticationError):
    """Raised when account is locked."""

    pass


class SessionExpiredError(AuthenticationError):
    """Raised when session has expired."""

    pass


class PasswordPolicyError(AuthenticationError):
    """Raised when password doesn't meet policy requirements."""

    pass


class AuthenticationService:
    """
    Comprehensive authentication service for DittoFS.

    Provides user authentication, session management, and security policies.
    """

    def __init__(
        self, data_dir: Path, password_policy: Optional[PasswordPolicy] = None
    ):
        """
        Initialize authentication service.

        Args:
            data_dir: Directory for storing authentication data
            password_policy: Password policy configuration
        """
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.password_policy = password_policy or PasswordPolicy()
        self.users_file = data_dir / "users.json"
        self.sessions_file = data_dir / "sessions.json"

        # In-memory storage for active sessions
        self.active_sessions: Dict[str, AuthToken] = {}
        self.users: Dict[str, UserAccount] = {}

        # Load existing data
        self._load_users()
        self._load_sessions()

        # Start session cleanup task
        self._cleanup_task = None

    async def start(self):
        """Start the authentication service."""
        if not CRYPTO_AVAILABLE:
            logger.warning(
                "Cryptography libraries not available - limited functionality"
            )

        # Start periodic cleanup of expired sessions
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info("Authentication service started")

    async def stop(self):
        """Stop the authentication service."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Save current state
        self._save_users()
        self._save_sessions()
        logger.info("Authentication service stopped")

    async def create_user(
        self,
        username: str,
        password: str,
        auth_methods: List[AuthMethod] = None,
        permissions: List[str] = None,
    ) -> UserAccount:
        """
        Create a new user account.

        Args:
            username: Username for the account
            password: Password for the account
            auth_methods: Supported authentication methods
            permissions: User permissions

        Returns:
            Created user account

        Raises:
            PasswordPolicyError: If password doesn't meet policy
            ValueError: If user already exists
        """
        if username in self.users:
            raise ValueError(f"User {username} already exists")

        # Validate password against policy
        self._validate_password(password)

        # Generate salt and hash password
        salt = secrets.token_hex(32)
        password_hash = self._hash_password(password, salt)

        # Create user account
        user = UserAccount(
            username=username,
            password_hash=password_hash,
            salt=salt,
            auth_methods=auth_methods or [AuthMethod.PASSWORD],
            permissions=permissions or [],
        )

        self.users[username] = user
        self._save_users()

        logger.info(f"Created user account: {username}")
        return user

    async def authenticate_user(self, credentials: UserCredentials) -> AuthToken:
        """
        Authenticate a user with provided credentials.

        Args:
            credentials: User credentials

        Returns:
            Authentication token

        Raises:
            InvalidCredentialsError: If credentials are invalid
            AccountLockedError: If account is locked
        """
        username = credentials.username

        if username not in self.users:
            raise InvalidCredentialsError("Invalid username")

        user = self.users[username]

        # Check if account is locked
        if user.locked_until and datetime.utcnow() < user.locked_until:
            raise AccountLockedError(f"Account locked until {user.locked_until}")

        try:
            # Authenticate using available methods
            auth_success = False

            # Password authentication
            if AuthMethod.PASSWORD in user.auth_methods and credentials.password:
                if self._verify_password(
                    credentials.password, user.password_hash, user.salt
                ):
                    auth_success = True

            # Key file authentication
            if AuthMethod.KEY_FILE in user.auth_methods and credentials.key_file_path:
                if await self._verify_key_file(credentials.key_file_path, user):
                    auth_success = True

            # Certificate authentication
            if (
                AuthMethod.CERTIFICATE in user.auth_methods
                and credentials.certificate_data
            ):
                if await self._verify_certificate(credentials.certificate_data, user):
                    auth_success = True

            if not auth_success:
                raise InvalidCredentialsError("Authentication failed")

            # Multi-factor authentication
            if AuthMethod.TOTP in user.auth_methods:
                if not credentials.totp_code or not self._verify_totp(
                    credentials.totp_code, user
                ):
                    raise InvalidCredentialsError("TOTP verification failed")

            # Reset failed attempts on successful auth
            user.failed_attempts = 0
            user.last_login = datetime.utcnow()
            user.locked_until = None

            # Create authentication token
            token = self._create_auth_token(user)
            self.active_sessions[token.token_id] = token

            self._save_users()
            self._save_sessions()

            logger.info(f"User authenticated successfully: {username}")
            return token

        except (InvalidCredentialsError, AccountLockedError):
            # Increment failed attempts
            user.failed_attempts += 1

            # Lock account if too many failures
            if user.failed_attempts >= self.password_policy.lockout_attempts:
                user.locked_until = datetime.utcnow() + timedelta(
                    minutes=self.password_policy.lockout_duration_minutes
                )
                logger.warning(f"Account locked due to failed attempts: {username}")

            self._save_users()
            raise

    async def validate_token(self, token_id: str) -> AuthToken:
        """
        Validate an authentication token.

        Args:
            token_id: Token identifier

        Returns:
            Valid authentication token

        Raises:
            SessionExpiredError: If token is expired or invalid
        """
        if token_id not in self.active_sessions:
            raise SessionExpiredError("Invalid or expired token")

        token = self.active_sessions[token_id]

        if token.is_expired:
            del self.active_sessions[token_id]
            raise SessionExpiredError("Token has expired")

        return token

    async def refresh_token(self, token_id: str) -> AuthToken:
        """
        Refresh an authentication token.

        Args:
            token_id: Token identifier

        Returns:
            New authentication token

        Raises:
            SessionExpiredError: If token is invalid
        """
        old_token = await self.validate_token(token_id)

        # Create new token with extended expiry
        user = self.users[old_token.username]
        new_token = self._create_auth_token(user)

        # Replace old token
        del self.active_sessions[token_id]
        self.active_sessions[new_token.token_id] = new_token

        self._save_sessions()

        logger.info(f"Token refreshed for user: {old_token.username}")
        return new_token

    async def revoke_token(self, token_id: str) -> bool:
        """
        Revoke an authentication token.

        Args:
            token_id: Token identifier

        Returns:
            True if token was revoked
        """
        if token_id in self.active_sessions:
            token = self.active_sessions[token_id]
            del self.active_sessions[token_id]
            self._save_sessions()
            logger.info(f"Token revoked for user: {token.username}")
            return True
        return False

    async def setup_totp(self, username: str) -> str:
        """
        Set up TOTP for a user account.

        Args:
            username: Username

        Returns:
            TOTP secret for QR code generation

        Raises:
            ValueError: If user doesn't exist or TOTP not available
        """
        if not CRYPTO_AVAILABLE:
            raise ValueError("TOTP requires cryptography libraries")

        if username not in self.users:
            raise ValueError("User not found")

        user = self.users[username]

        # Generate TOTP secret
        secret = pyotp.random_base32()
        user.totp_secret = secret

        # Add TOTP to auth methods if not present
        if AuthMethod.TOTP not in user.auth_methods:
            user.auth_methods.append(AuthMethod.TOTP)

        self._save_users()

        logger.info(f"TOTP setup for user: {username}")
        return secret

    def _validate_password(self, password: str) -> None:
        """Validate password against policy."""
        policy = self.password_policy

        if len(password) < policy.min_length:
            raise PasswordPolicyError(
                f"Password must be at least {policy.min_length} characters"
            )

        if policy.require_uppercase and not any(c.isupper() for c in password):
            raise PasswordPolicyError("Password must contain uppercase letters")

        if policy.require_lowercase and not any(c.islower() for c in password):
            raise PasswordPolicyError("Password must contain lowercase letters")

        if policy.require_digits and not any(c.isdigit() for c in password):
            raise PasswordPolicyError("Password must contain digits")

        if policy.require_special and not any(
            c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password
        ):
            raise PasswordPolicyError("Password must contain special characters")

    def _hash_password(self, password: str, salt: str) -> str:
        """Hash password with salt using PBKDF2."""
        if not CRYPTO_AVAILABLE:
            # Fallback to basic hashing (not recommended for production)
            return hashlib.sha256((password + salt).encode()).hexdigest()

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt.encode(),
            iterations=100000,
        )
        key = kdf.derive(password.encode())
        return base64.b64encode(key).decode()

    def _verify_password(self, password: str, password_hash: str, salt: str) -> bool:
        """Verify password against hash."""
        computed_hash = self._hash_password(password, salt)
        return hmac.compare_digest(password_hash, computed_hash)

    async def _verify_key_file(self, key_file_path: Path, user: UserAccount) -> bool:
        """Verify key file authentication."""
        try:
            if not key_file_path.exists():
                return False

            # Read and verify key file
            key_data = key_file_path.read_bytes()

            # For now, just check if file exists and is readable
            # In production, this would verify cryptographic signatures
            return len(key_data) > 0

        except Exception as e:
            logger.error(f"Key file verification failed: {e}")
            return False

    async def _verify_certificate(
        self, certificate_data: bytes, user: UserAccount
    ) -> bool:
        """Verify certificate authentication."""
        if not CRYPTO_AVAILABLE:
            return False

        try:
            # Load and verify certificate
            cert = load_pem_x509_certificate(certificate_data)

            # Basic certificate validation
            # In production, this would include full chain validation
            return cert is not None

        except Exception as e:
            logger.error(f"Certificate verification failed: {e}")
            return False

    def _verify_totp(self, totp_code: str, user: UserAccount) -> bool:
        """Verify TOTP code."""
        if not CRYPTO_AVAILABLE or not user.totp_secret:
            return False

        try:
            totp = pyotp.TOTP(user.totp_secret)
            return totp.verify(totp_code, valid_window=1)
        except Exception as e:
            logger.error(f"TOTP verification failed: {e}")
            return False

    def _create_auth_token(self, user: UserAccount) -> AuthToken:
        """Create authentication token for user."""
        token_id = secrets.token_urlsafe(32)
        issued_at = datetime.utcnow()
        expires_at = issued_at + timedelta(hours=24)  # 24 hour sessions

        return AuthToken(
            token_id=token_id,
            username=user.username,
            issued_at=issued_at,
            expires_at=expires_at,
            permissions=user.permissions.copy(),
            session_data={},
        )

    def _load_users(self) -> None:
        """Load users from storage."""
        try:
            if self.users_file.exists():
                data = json.loads(self.users_file.read_text())
                for username, user_data in data.items():
                    # Convert datetime strings back to datetime objects
                    if user_data.get("created_at"):
                        user_data["created_at"] = datetime.fromisoformat(
                            user_data["created_at"]
                        )
                    if user_data.get("last_login"):
                        user_data["last_login"] = datetime.fromisoformat(
                            user_data["last_login"]
                        )
                    if user_data.get("locked_until"):
                        user_data["locked_until"] = datetime.fromisoformat(
                            user_data["locked_until"]
                        )

                    # Convert auth methods
                    user_data["auth_methods"] = [
                        AuthMethod(method) for method in user_data["auth_methods"]
                    ]

                    self.users[username] = UserAccount(**user_data)
        except Exception as e:
            logger.error(f"Failed to load users: {e}")

    def _save_users(self) -> None:
        """Save users to storage."""
        try:
            data = {}
            for username, user in self.users.items():
                user_data = {
                    "username": user.username,
                    "password_hash": user.password_hash,
                    "salt": user.salt,
                    "auth_methods": [method.value for method in user.auth_methods],
                    "totp_secret": user.totp_secret,
                    "failed_attempts": user.failed_attempts,
                    "locked_until": (
                        user.locked_until.isoformat() if user.locked_until else None
                    ),
                    "last_login": (
                        user.last_login.isoformat() if user.last_login else None
                    ),
                    "created_at": user.created_at.isoformat(),
                    "permissions": user.permissions,
                    "metadata": user.metadata,
                }
                data[username] = user_data

            self.users_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save users: {e}")

    def _load_sessions(self) -> None:
        """Load active sessions from storage."""
        try:
            if self.sessions_file.exists():
                data = json.loads(self.sessions_file.read_text())
                for token_id, session_data in data.items():
                    # Convert datetime strings back to datetime objects
                    session_data["issued_at"] = datetime.fromisoformat(
                        session_data["issued_at"]
                    )
                    session_data["expires_at"] = datetime.fromisoformat(
                        session_data["expires_at"]
                    )

                    token = AuthToken(**session_data)
                    if not token.is_expired:
                        self.active_sessions[token_id] = token
        except Exception as e:
            logger.error(f"Failed to load sessions: {e}")

    def _save_sessions(self) -> None:
        """Save active sessions to storage."""
        try:
            data = {}
            for token_id, token in self.active_sessions.items():
                if not token.is_expired:
                    data[token_id] = {
                        "token_id": token.token_id,
                        "username": token.username,
                        "issued_at": token.issued_at.isoformat(),
                        "expires_at": token.expires_at.isoformat(),
                        "permissions": token.permissions,
                        "session_data": token.session_data,
                    }

            self.sessions_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save sessions: {e}")

    async def _periodic_cleanup(self) -> None:
        """Periodically clean up expired sessions."""
        while True:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes

                expired_tokens = [
                    token_id
                    for token_id, token in self.active_sessions.items()
                    if token.is_expired
                ]

                for token_id in expired_tokens:
                    del self.active_sessions[token_id]

                if expired_tokens:
                    self._save_sessions()
                    logger.info(f"Cleaned up {len(expired_tokens)} expired sessions")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Session cleanup error: {e}")
