#!/usr/bin/env python3
"""
Demo script for DittoFS Authentication Service.

This script demonstrates the comprehensive authentication features including:
- User account creation and management
- Password-based authentication
- Multi-factor authentication (TOTP)
- Key file authentication
- Session management
- Password policy enforcement
- Account lockout protection
"""

import asyncio
import tempfile
from datetime import datetime
from pathlib import Path

from src.dittofs.authentication import (
    AccountLockedError,
    AuthenticationError,
    AuthenticationService,
    AuthMethod,
    InvalidCredentialsError,
    PasswordPolicy,
    PasswordPolicyError,
    UserCredentials,
)


async def demo_basic_authentication():
    """Demonstrate basic password authentication."""
    print("=== Basic Authentication Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create authentication service
        auth_service = AuthenticationService(Path(temp_dir))
        await auth_service.start()

        try:
            # Create a user account
            print("Creating user account...")
            username = "alice"
            password = "SecurePassword123!"

            user = await auth_service.create_user(username, password)
            print(f"✓ Created user: {user.username}")
            print(f"  Auth methods: {[method.value for method in user.auth_methods]}")
            print(f"  Created at: {user.created_at}")

            # Authenticate user
            print("\nAuthenticating user...")
            credentials = UserCredentials(username=username, password=password)
            token = await auth_service.authenticate_user(credentials)

            print(f"✓ Authentication successful!")
            print(f"  Token ID: {token.token_id[:16]}...")
            print(f"  Username: {token.username}")
            print(f"  Expires at: {token.expires_at}")
            print(f"  Time until expiry: {token.time_until_expiry}")

            # Validate token
            print("\nValidating token...")
            validated_token = await auth_service.validate_token(token.token_id)
            print(f"✓ Token is valid for user: {validated_token.username}")

            # Refresh token
            print("\nRefreshing token...")
            new_token = await auth_service.refresh_token(token.token_id)
            print(f"✓ Token refreshed!")
            print(f"  New token ID: {new_token.token_id[:16]}...")
            print(f"  New expiry: {new_token.expires_at}")

            # Revoke token
            print("\nRevoking token...")
            revoked = await auth_service.revoke_token(new_token.token_id)
            print(f"✓ Token revoked: {revoked}")

        finally:
            await auth_service.stop()


async def demo_password_policy():
    """Demonstrate password policy enforcement."""
    print("\n=== Password Policy Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create strict password policy
        policy = PasswordPolicy(
            min_length=10,
            require_uppercase=True,
            require_lowercase=True,
            require_digits=True,
            require_special=True,
            lockout_attempts=3,
            lockout_duration_minutes=1,
        )

        auth_service = AuthenticationService(Path(temp_dir), policy)
        await auth_service.start()

        try:
            username = "bob"

            # Test various password policy violations
            weak_passwords = [
                ("short", "Password too short"),
                ("nouppercase123!", "Missing uppercase"),
                ("NOLOWERCASE123!", "Missing lowercase"),
                ("NoDigitsHere!", "Missing digits"),
                ("NoSpecialChars123", "Missing special characters"),
            ]

            print("Testing password policy violations...")
            for password, description in weak_passwords:
                try:
                    await auth_service.create_user(username, password)
                    print(f"✗ {description}: Should have failed but didn't")
                except PasswordPolicyError as e:
                    print(f"✓ {description}: {e}")
                except Exception as e:
                    print(f"? {description}: Unexpected error: {e}")

            # Create user with valid password
            print("\nCreating user with valid password...")
            valid_password = "ValidPassword123!"
            user = await auth_service.create_user(username, valid_password)
            print(f"✓ User created with valid password: {user.username}")

        finally:
            await auth_service.stop()


async def demo_account_lockout():
    """Demonstrate account lockout protection."""
    print("\n=== Account Lockout Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create policy with quick lockout for demo
        policy = PasswordPolicy(lockout_attempts=3, lockout_duration_minutes=1)

        auth_service = AuthenticationService(Path(temp_dir), policy)
        await auth_service.start()

        try:
            # Create user
            username = "charlie"
            password = "CorrectPassword123!"
            await auth_service.create_user(username, password)
            print(f"Created user: {username}")

            # Make failed authentication attempts
            print("\nMaking failed authentication attempts...")
            wrong_credentials = UserCredentials(
                username=username, password="WrongPassword"
            )

            for attempt in range(1, 5):
                try:
                    await auth_service.authenticate_user(wrong_credentials)
                    print(f"✗ Attempt {attempt}: Should have failed")
                except InvalidCredentialsError:
                    print(f"✓ Attempt {attempt}: Authentication failed as expected")
                except AccountLockedError as e:
                    print(f"🔒 Attempt {attempt}: {e}")
                    break

            # Try with correct password while locked
            print("\nTrying correct password while account is locked...")
            correct_credentials = UserCredentials(username=username, password=password)
            try:
                await auth_service.authenticate_user(correct_credentials)
                print("✗ Should have failed due to account lock")
            except AccountLockedError as e:
                print(f"🔒 Correct password rejected: {e}")

            # Check user status
            user = auth_service.users[username]
            print(f"\nUser status:")
            print(f"  Failed attempts: {user.failed_attempts}")
            print(f"  Locked until: {user.locked_until}")

        finally:
            await auth_service.stop()


async def demo_totp_authentication():
    """Demonstrate TOTP multi-factor authentication."""
    print("\n=== TOTP Authentication Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        auth_service = AuthenticationService(Path(temp_dir))
        await auth_service.start()

        try:
            # Create user
            username = "diana"
            password = "SecurePassword123!"
            await auth_service.create_user(username, password)
            print(f"Created user: {username}")

            # Setup TOTP
            print("\nSetting up TOTP...")
            try:
                secret = await auth_service.setup_totp(username)
                print(f"✓ TOTP secret generated: {secret}")
                print(
                    f"  QR Code URL: otpauth://totp/DittoFS:{username}?secret={secret}&issuer=DittoFS"
                )

                # Check user auth methods
                user = auth_service.users[username]
                print(
                    f"  Auth methods: {[method.value for method in user.auth_methods]}"
                )

            except ValueError as e:
                print(f"⚠ TOTP setup failed: {e}")
                print("  This is expected if cryptography libraries are not available")

        finally:
            await auth_service.stop()


async def demo_key_file_authentication():
    """Demonstrate key file authentication."""
    print("\n=== Key File Authentication Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        auth_service = AuthenticationService(Path(temp_dir))
        await auth_service.start()

        try:
            # Create user with key file authentication
            username = "eve"
            password = "Password123!"
            user = await auth_service.create_user(
                username, password, auth_methods=[AuthMethod.KEY_FILE]
            )
            print(f"Created user with key file auth: {user.username}")

            # Create a key file
            key_file_path = Path(temp_dir) / "user.key"
            key_file_path.write_text("This is a test key file for user authentication")
            print(f"Created key file: {key_file_path}")

            # Authenticate using key file
            print("\nAuthenticating with key file...")
            credentials = UserCredentials(
                username=username, key_file_path=key_file_path
            )

            token = await auth_service.authenticate_user(credentials)
            print(f"✓ Key file authentication successful!")
            print(f"  Token: {token.token_id[:16]}...")

            # Test with non-existent key file
            print("\nTesting with non-existent key file...")
            bad_credentials = UserCredentials(
                username=username, key_file_path=Path("/nonexistent/key/file")
            )

            try:
                await auth_service.authenticate_user(bad_credentials)
                print("✗ Should have failed")
            except InvalidCredentialsError:
                print("✓ Authentication failed as expected with missing key file")

        finally:
            await auth_service.stop()


async def demo_session_management():
    """Demonstrate session management features."""
    print("\n=== Session Management Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        auth_service = AuthenticationService(Path(temp_dir))
        await auth_service.start()

        try:
            # Create user
            username = "frank"
            password = "SessionTest123!"
            await auth_service.create_user(username, password)

            # Create multiple sessions
            print("Creating multiple sessions...")
            sessions = []
            for i in range(3):
                credentials = UserCredentials(username=username, password=password)
                token = await auth_service.authenticate_user(credentials)
                sessions.append(token)
                print(f"  Session {i+1}: {token.token_id[:16]}...")

            print(f"\nActive sessions: {len(auth_service.active_sessions)}")

            # Validate all sessions
            print("\nValidating sessions...")
            for i, token in enumerate(sessions):
                try:
                    validated = await auth_service.validate_token(token.token_id)
                    print(f"  Session {i+1}: ✓ Valid ({validated.username})")
                except Exception as e:
                    print(f"  Session {i+1}: ✗ Invalid ({e})")

            # Revoke one session
            print(f"\nRevoking session 2...")
            revoked = await auth_service.revoke_token(sessions[1].token_id)
            print(f"  Revoked: {revoked}")
            print(f"  Active sessions: {len(auth_service.active_sessions)}")

            # Validate remaining sessions
            print("\nValidating remaining sessions...")
            for i, token in enumerate(sessions):
                try:
                    validated = await auth_service.validate_token(token.token_id)
                    print(f"  Session {i+1}: ✓ Valid ({validated.username})")
                except Exception as e:
                    print(f"  Session {i+1}: ✗ Invalid ({e})")

        finally:
            await auth_service.stop()


async def demo_persistence():
    """Demonstrate data persistence across service restarts."""
    print("\n=== Persistence Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir)

        # First service instance
        print("Creating first service instance...")
        auth_service1 = AuthenticationService(data_dir)
        await auth_service1.start()

        # Create user and session
        username = "grace"
        password = "PersistenceTest123!"
        await auth_service1.create_user(username, password)

        credentials = UserCredentials(username=username, password=password)
        token = await auth_service1.authenticate_user(credentials)

        print(f"Created user and session in first instance")
        print(f"  User: {username}")
        print(f"  Token: {token.token_id[:16]}...")

        await auth_service1.stop()

        # Second service instance
        print("\nCreating second service instance...")
        auth_service2 = AuthenticationService(data_dir)
        await auth_service2.start()

        try:
            # Check if user persisted
            if username in auth_service2.users:
                print(f"✓ User persisted: {username}")

                # Try to authenticate
                new_token = await auth_service2.authenticate_user(credentials)
                print(f"✓ Authentication successful with persisted user")
                print(f"  New token: {new_token.token_id[:16]}...")

                # Check if old session persisted (if not expired)
                try:
                    validated = await auth_service2.validate_token(token.token_id)
                    print(f"✓ Old session persisted: {validated.username}")
                except Exception as e:
                    print(f"⚠ Old session not available: {e}")
            else:
                print("✗ User did not persist")

        finally:
            await auth_service2.stop()


async def main():
    """Run all authentication demos."""
    print("DittoFS Authentication Service Demo")
    print("=" * 50)

    demos = [
        demo_basic_authentication,
        demo_password_policy,
        demo_account_lockout,
        demo_totp_authentication,
        demo_key_file_authentication,
        demo_session_management,
        demo_persistence,
    ]

    for demo in demos:
        try:
            await demo()
        except Exception as e:
            print(f"Demo failed: {e}")
        print()  # Add spacing between demos

    print("Authentication demo completed!")


if __name__ == "__main__":
    asyncio.run(main())
