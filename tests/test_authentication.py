"""
Tests for the authentication service.
"""

import asyncio
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.dittofs.authentication import (
    AccountLockedError,
    AuthenticationError,
    AuthenticationService,
    AuthMethod,
    AuthToken,
    InvalidCredentialsError,
    PasswordPolicy,
    PasswordPolicyError,
    SessionExpiredError,
    UserAccount,
    UserCredentials,
)


@pytest.fixture
async def auth_service():
    """Create authentication service for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AuthenticationService(Path(temp_dir))
        await service.start()
        yield service
        await service.stop()


@pytest.fixture
def password_policy():
    """Create test password policy."""
    return PasswordPolicy(
        min_length=8,
        require_uppercase=True,
        require_lowercase=True,
        require_digits=True,
        require_special=False,
        lockout_attempts=3,
        lockout_duration_minutes=5,
    )


@pytest.fixture
async def auth_service_with_policy(password_policy):
    """Create authentication service with custom policy."""
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AuthenticationService(Path(temp_dir), password_policy)
        await service.start()
        yield service
        await service.stop()


class TestAuthenticationService:
    """Test authentication service functionality."""

    async def test_create_user(self, auth_service):
        """Test user creation."""
        username = "testuser"
        password = "TestPassword123!"

        user = await auth_service.create_user(username, password)

        assert user.username == username
        assert user.password_hash != password  # Should be hashed
        assert len(user.salt) > 0
        assert AuthMethod.PASSWORD in user.auth_methods
        assert user.failed_attempts == 0
        assert user.locked_until is None

    async def test_create_duplicate_user(self, auth_service):
        """Test creating duplicate user fails."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        with pytest.raises(ValueError, match="already exists"):
            await auth_service.create_user(username, password)

    async def test_password_policy_validation(self, auth_service_with_policy):
        """Test password policy enforcement."""
        username = "testuser"

        # Test too short
        with pytest.raises(PasswordPolicyError, match="at least 8 characters"):
            await auth_service_with_policy.create_user(username, "short")

        # Test missing uppercase
        with pytest.raises(PasswordPolicyError, match="uppercase"):
            await auth_service_with_policy.create_user(username, "lowercase123")

        # Test missing lowercase
        with pytest.raises(PasswordPolicyError, match="lowercase"):
            await auth_service_with_policy.create_user(username, "UPPERCASE123")

        # Test missing digits
        with pytest.raises(PasswordPolicyError, match="digits"):
            await auth_service_with_policy.create_user(username, "NoDigitsHere")

        # Test valid password
        user = await auth_service_with_policy.create_user(username, "ValidPass123")
        assert user.username == username

    async def test_authenticate_user_success(self, auth_service):
        """Test successful user authentication."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        token = await auth_service.authenticate_user(credentials)

        assert token.username == username
        assert not token.is_expired
        assert len(token.token_id) > 0

    async def test_authenticate_invalid_user(self, auth_service):
        """Test authentication with invalid username."""
        credentials = UserCredentials(username="nonexistent", password="password")

        with pytest.raises(InvalidCredentialsError, match="Invalid username"):
            await auth_service.authenticate_user(credentials)

    async def test_authenticate_invalid_password(self, auth_service):
        """Test authentication with invalid password."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password="wrongpassword")

        with pytest.raises(InvalidCredentialsError, match="Authentication failed"):
            await auth_service.authenticate_user(credentials)

    async def test_account_lockout(self, auth_service_with_policy):
        """Test account lockout after failed attempts."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service_with_policy.create_user(username, password)

        # Make failed attempts
        credentials = UserCredentials(username=username, password="wrongpassword")

        for i in range(3):  # Policy allows 3 attempts
            with pytest.raises(InvalidCredentialsError):
                await auth_service_with_policy.authenticate_user(credentials)

        # Next attempt should lock account
        with pytest.raises(AccountLockedError, match="Account locked"):
            await auth_service_with_policy.authenticate_user(credentials)

        # Even correct password should fail when locked
        correct_credentials = UserCredentials(username=username, password=password)
        with pytest.raises(AccountLockedError, match="Account locked"):
            await auth_service_with_policy.authenticate_user(correct_credentials)

    async def test_validate_token(self, auth_service):
        """Test token validation."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        token = await auth_service.authenticate_user(credentials)

        # Valid token should pass
        validated_token = await auth_service.validate_token(token.token_id)
        assert validated_token.username == username

        # Invalid token should fail
        with pytest.raises(SessionExpiredError, match="Invalid or expired"):
            await auth_service.validate_token("invalid_token")

    async def test_refresh_token(self, auth_service):
        """Test token refresh."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        original_token = await auth_service.authenticate_user(credentials)

        # Refresh token
        new_token = await auth_service.refresh_token(original_token.token_id)

        assert new_token.username == username
        assert new_token.token_id != original_token.token_id

        # Original token should be invalid
        with pytest.raises(SessionExpiredError):
            await auth_service.validate_token(original_token.token_id)

        # New token should be valid
        await auth_service.validate_token(new_token.token_id)

    async def test_revoke_token(self, auth_service):
        """Test token revocation."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        token = await auth_service.authenticate_user(credentials)

        # Token should be valid initially
        await auth_service.validate_token(token.token_id)

        # Revoke token
        result = await auth_service.revoke_token(token.token_id)
        assert result is True

        # Token should be invalid after revocation
        with pytest.raises(SessionExpiredError):
            await auth_service.validate_token(token.token_id)

        # Revoking non-existent token should return False
        result = await auth_service.revoke_token("nonexistent")
        assert result is False

    async def test_setup_totp(self, auth_service):
        """Test TOTP setup."""
        with (
            patch("src.dittofs.authentication.CRYPTO_AVAILABLE", True),
            patch("src.dittofs.authentication.pyotp") as mock_pyotp,
        ):
            mock_pyotp.random_base32.return_value = "TESTSECRET123"

            username = "testuser"
            password = "TestPassword123!"

            await auth_service.create_user(username, password)

            secret = await auth_service.setup_totp(username)

            assert secret == "TESTSECRET123"
            user = auth_service.users[username]
            assert user.totp_secret == "TESTSECRET123"
            assert AuthMethod.TOTP in user.auth_methods

    async def test_setup_totp_no_crypto(self, auth_service):
        """Test TOTP setup without crypto libraries."""
        with patch("src.dittofs.authentication.CRYPTO_AVAILABLE", False):
            username = "testuser"
            password = "TestPassword123!"

            await auth_service.create_user(username, password)

            with pytest.raises(ValueError, match="requires cryptography"):
                await auth_service.setup_totp(username)

    async def test_setup_totp_invalid_user(self, auth_service):
        """Test TOTP setup for non-existent user."""
        with patch("src.dittofs.authentication.CRYPTO_AVAILABLE", True):
            with pytest.raises(ValueError, match="User not found"):
                await auth_service.setup_totp("nonexistent")

    async def test_totp_authentication(self, auth_service):
        """Test TOTP authentication."""
        with (
            patch("src.dittofs.authentication.CRYPTO_AVAILABLE", True),
            patch("src.dittofs.authentication.pyotp") as mock_pyotp,
        ):
            # Setup mocks
            mock_totp_instance = MagicMock()
            mock_totp_instance.verify.return_value = True
            mock_pyotp.TOTP.return_value = mock_totp_instance
            mock_pyotp.random_base32.return_value = "TESTSECRET123"

            username = "testuser"
            password = "TestPassword123!"

            # Create user and setup TOTP
            await auth_service.create_user(username, password)
            await auth_service.setup_totp(username)

            # Authenticate with TOTP
            credentials = UserCredentials(
                username=username, password=password, totp_code="123456"
            )

            token = await auth_service.authenticate_user(credentials)
            assert token.username == username

            # Verify TOTP was called
            mock_pyotp.TOTP.assert_called_with("TESTSECRET123")
            mock_totp_instance.verify.assert_called_with("123456", valid_window=1)

    async def test_totp_authentication_failure(self, auth_service):
        """Test TOTP authentication failure."""
        with (
            patch("src.dittofs.authentication.CRYPTO_AVAILABLE", True),
            patch("src.dittofs.authentication.pyotp") as mock_pyotp,
        ):
            # Setup mocks
            mock_totp_instance = MagicMock()
            mock_totp_instance.verify.return_value = False
            mock_pyotp.TOTP.return_value = mock_totp_instance
            mock_pyotp.random_base32.return_value = "TESTSECRET123"

            username = "testuser"
            password = "TestPassword123!"

            # Create user and setup TOTP
            await auth_service.create_user(username, password)
            await auth_service.setup_totp(username)

            # Authenticate with invalid TOTP
            credentials = UserCredentials(
                username=username, password=password, totp_code="invalid"
            )

            with pytest.raises(
                InvalidCredentialsError, match="TOTP verification failed"
            ):
                await auth_service.authenticate_user(credentials)

    async def test_key_file_authentication(self, auth_service):
        """Test key file authentication."""
        username = "testuser"
        password = "TestPassword123!"

        # Create user with key file auth
        user = await auth_service.create_user(
            username, password, auth_methods=[AuthMethod.KEY_FILE]
        )

        # Create temporary key file
        with tempfile.NamedTemporaryFile(delete=False) as key_file:
            key_file.write(b"test key data")
            key_file_path = Path(key_file.name)

        try:
            credentials = UserCredentials(
                username=username, key_file_path=key_file_path
            )

            token = await auth_service.authenticate_user(credentials)
            assert token.username == username

        finally:
            key_file_path.unlink()

    async def test_key_file_authentication_missing_file(self, auth_service):
        """Test key file authentication with missing file."""
        username = "testuser"
        password = "TestPassword123!"

        # Create user with key file auth
        await auth_service.create_user(
            username, password, auth_methods=[AuthMethod.KEY_FILE]
        )

        credentials = UserCredentials(
            username=username, key_file_path=Path("/nonexistent/key/file")
        )

        with pytest.raises(InvalidCredentialsError, match="Authentication failed"):
            await auth_service.authenticate_user(credentials)

    async def test_persistence(self, auth_service):
        """Test data persistence across service restarts."""
        username = "testuser"
        password = "TestPassword123!"

        # Create user
        await auth_service.create_user(username, password)

        # Authenticate and get token
        credentials = UserCredentials(username=username, password=password)
        token = await auth_service.authenticate_user(credentials)

        # Stop and restart service
        data_dir = auth_service.data_dir
        await auth_service.stop()

        new_service = AuthenticationService(data_dir)
        await new_service.start()

        try:
            # User should still exist
            assert username in new_service.users

            # Should be able to authenticate
            new_token = await new_service.authenticate_user(credentials)
            assert new_token.username == username

            # Old token should still be valid (if not expired)
            if not token.is_expired:
                validated_token = await new_service.validate_token(token.token_id)
                assert validated_token.username == username

        finally:
            await new_service.stop()

    async def test_expired_token_cleanup(self, auth_service):
        """Test cleanup of expired tokens."""
        username = "testuser"
        password = "TestPassword123!"

        await auth_service.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        token = await auth_service.authenticate_user(credentials)

        # Manually expire the token
        token.expires_at = datetime.utcnow() - timedelta(hours=1)

        # Token should be expired
        assert token.is_expired

        # Validation should fail and clean up token
        with pytest.raises(SessionExpiredError):
            await auth_service.validate_token(token.token_id)

        # Token should be removed from active sessions
        assert token.token_id not in auth_service.active_sessions


class TestPasswordPolicy:
    """Test password policy functionality."""

    def test_default_policy(self):
        """Test default password policy."""
        policy = PasswordPolicy()

        assert policy.min_length == 12
        assert policy.require_uppercase is True
        assert policy.require_lowercase is True
        assert policy.require_digits is True
        assert policy.require_special is True
        assert policy.lockout_attempts == 5
        assert policy.lockout_duration_minutes == 30

    def test_custom_policy(self):
        """Test custom password policy."""
        policy = PasswordPolicy(min_length=8, require_special=False, lockout_attempts=3)

        assert policy.min_length == 8
        assert policy.require_special is False
        assert policy.lockout_attempts == 3


class TestAuthToken:
    """Test authentication token functionality."""

    def test_token_expiry(self):
        """Test token expiry checking."""
        # Create expired token
        expired_token = AuthToken(
            token_id="test",
            username="user",
            issued_at=datetime.utcnow() - timedelta(hours=2),
            expires_at=datetime.utcnow() - timedelta(hours=1),
        )

        assert expired_token.is_expired is True
        assert expired_token.time_until_expiry.total_seconds() < 0

        # Create valid token
        valid_token = AuthToken(
            token_id="test",
            username="user",
            issued_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=1),
        )

        assert valid_token.is_expired is False
        assert valid_token.time_until_expiry.total_seconds() > 0


class TestUserAccount:
    """Test user account functionality."""

    def test_user_account_creation(self):
        """Test user account creation."""
        user = UserAccount(
            username="testuser",
            password_hash="hash",
            salt="salt",
            auth_methods=[AuthMethod.PASSWORD],
            permissions=["read", "write"],
        )

        assert user.username == "testuser"
        assert user.password_hash == "hash"
        assert user.salt == "salt"
        assert AuthMethod.PASSWORD in user.auth_methods
        assert "read" in user.permissions
        assert "write" in user.permissions
        assert user.failed_attempts == 0
        assert user.locked_until is None


if __name__ == "__main__":
    pytest.main([__file__])
