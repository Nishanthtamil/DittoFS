"""
Tests for the peer authentication service.
"""

import asyncio
import pytest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.dittofs.peer_authentication import (
    PeerAuthenticationService,
    PeerCertificate,
    PeerIdentity,
    TrustRelationship,
    AuthenticationChallenge,
    TrustLevel,
    CertificateStatus,
    PeerAuthenticationError,
    CertificateError,
    TrustError,
    ChallengeError
)


@pytest.fixture
async def peer_auth_service():
    """Create peer authentication service for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        service = PeerAuthenticationService(Path(temp_dir), "test_peer")
        await service.start()
        yield service
        await service.stop()


@pytest.fixture
async def peer_auth_service_with_crypto():
    """Create peer authentication service with crypto mocked."""
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch('src.dittofs.peer_authentication.CRYPTO_AVAILABLE', True):
            service = PeerAuthenticationService(Path(temp_dir), "test_peer")
            await service.start()
            yield service
            await service.stop()


class TestPeerAuthenticationService:
    """Test peer authentication service functionality."""
    
    async def test_service_initialization(self, peer_auth_service):
        """Test service initialization."""
        assert peer_auth_service.peer_id == "test_peer"
        assert peer_auth_service.data_dir.exists()
        assert isinstance(peer_auth_service.certificates, dict)
        assert isinstance(peer_auth_service.peers, dict)
        assert isinstance(peer_auth_service.trust_relationships, dict)
    
    async def test_generate_certificate_no_crypto(self, peer_auth_service):
        """Test certificate generation without crypto libraries."""
        with pytest.raises(CertificateError, match="requires cryptography"):
            await peer_auth_service.generate_peer_certificate("peer1")
    
    @patch('src.dittofs.peer_authentication.CRYPTO_AVAILABLE', True)
    @patch('src.dittofs.peer_authentication.rsa')
    @patch('src.dittofs.peer_authentication.x509')
    async def test_generate_certificate_with_crypto(self, mock_x509, mock_rsa, peer_auth_service):
        """Test certificate generation with crypto libraries."""
        # Setup mocks
        mock_private_key = MagicMock()
        mock_public_key = MagicMock()
        mock_cert = MagicMock()
        
        mock_rsa.generate_private_key.return_value = mock_private_key
        mock_private_key.public_key.return_value = mock_public_key
        
        mock_cert_builder = MagicMock()
        mock_x509.CertificateBuilder.return_value = mock_cert_builder
        mock_cert_builder.subject_name.return_value = mock_cert_builder
        mock_cert_builder.issuer_name.return_value = mock_cert_builder
        mock_cert_builder.public_key.return_value = mock_cert_builder
        mock_cert_builder.serial_number.return_value = mock_cert_builder
        mock_cert_builder.not_valid_before.return_value = mock_cert_builder
        mock_cert_builder.not_valid_after.return_value = mock_cert_builder
        mock_cert_builder.add_extension.return_value = mock_cert_builder
        mock_cert_builder.sign.return_value = mock_cert
        
        mock_cert.public_bytes.return_value = b"mock_cert_pem"
        mock_cert.serial_number = 12345
        mock_private_key.private_bytes.return_value = b"mock_private_key_pem"
        mock_public_key.public_bytes.return_value = b"mock_public_key_pem"
        
        # Generate certificate
        cert = await peer_auth_service.generate_peer_certificate("peer1")
        
        assert cert.peer_id == "peer1"
        assert cert.certificate_pem == b"mock_cert_pem"
        assert cert.private_key_pem == b"mock_private_key_pem"
        assert cert.public_key_pem == b"mock_public_key_pem"
        assert "peer1" in peer_auth_service.certificates
    
    async def test_validate_certificate_no_crypto(self, peer_auth_service):
        """Test certificate validation without crypto libraries."""
        status = await peer_auth_service.validate_certificate(b"cert_data", "peer1")
        assert status == CertificateStatus.INVALID
    
    @patch('src.dittofs.peer_authentication.CRYPTO_AVAILABLE', True)
    @patch('src.dittofs.peer_authentication.x509')
    async def test_validate_certificate_valid(self, mock_x509, peer_auth_service):
        """Test valid certificate validation."""
        # Setup mock certificate
        mock_cert = MagicMock()
        mock_x509.load_pem_x509_certificate.return_value = mock_cert
        
        # Mock certificate attributes
        mock_cert.not_valid_after = datetime.utcnow() + timedelta(days=30)
        
        mock_attribute = MagicMock()
        mock_attribute.oid = mock_x509.oid.NameOID.COMMON_NAME
        mock_attribute.value = "peer1"
        mock_cert.subject = [mock_attribute]
        
        status = await peer_auth_service.validate_certificate(b"cert_data", "peer1")
        assert status == CertificateStatus.VALID
    
    @patch('src.dittofs.peer_authentication.CRYPTO_AVAILABLE', True)
    @patch('src.dittofs.peer_authentication.x509')
    async def test_validate_certificate_expired(self, mock_x509, peer_auth_service):
        """Test expired certificate validation."""
        # Setup mock certificate
        mock_cert = MagicMock()
        mock_x509.load_pem_x509_certificate.return_value = mock_cert
        
        # Mock expired certificate
        mock_cert.not_valid_after = datetime.utcnow() - timedelta(days=1)
        
        mock_attribute = MagicMock()
        mock_attribute.oid = mock_x509.oid.NameOID.COMMON_NAME
        mock_attribute.value = "peer1"
        mock_cert.subject = [mock_attribute]
        
        status = await peer_auth_service.validate_certificate(b"cert_data", "peer1")
        assert status == CertificateStatus.EXPIRED
    
    @patch('src.dittofs.peer_authentication.CRYPTO_AVAILABLE', True)
    @patch('src.dittofs.peer_authentication.x509')
    async def test_validate_certificate_revoked(self, mock_x509, peer_auth_service):
        """Test revoked certificate validation."""
        # Add certificate fingerprint to revoked list
        import hashlib
        fingerprint = hashlib.sha256(b"cert_data").hexdigest()
        peer_auth_service.revoked_certificates.add(fingerprint)
        
        # Setup mock certificate
        mock_cert = MagicMock()
        mock_x509.load_pem_x509_certificate.return_value = mock_cert
        mock_cert.not_valid_after = datetime.utcnow() + timedelta(days=30)
        
        status = await peer_auth_service.validate_certificate(b"cert_data", "peer1")
        assert status == CertificateStatus.REVOKED
    
    async def test_authenticate_peer_invalid_certificate(self, peer_auth_service):
        """Test peer authentication with invalid certificate."""
        with patch.object(peer_auth_service, 'validate_certificate') as mock_validate:
            mock_validate.return_value = CertificateStatus.INVALID
            
            with pytest.raises(PeerAuthenticationError, match="Certificate validation failed"):
                await peer_auth_service.authenticate_peer("peer1", b"invalid_cert")
    
    async def test_authenticate_peer_success(self, peer_auth_service):
        """Test successful peer authentication."""
        with patch.object(peer_auth_service, 'validate_certificate') as mock_validate:
            mock_validate.return_value = CertificateStatus.VALID
            
            peer = await peer_auth_service.authenticate_peer("peer1", b"valid_cert")
            
            assert peer.peer_id == "peer1"
            assert peer.trust_level == TrustLevel.BASIC
            assert peer.successful_auths == 1
            assert "peer1" in peer_auth_service.peers
    
    async def test_authenticate_existing_peer(self, peer_auth_service):
        """Test authentication of existing peer."""
        # Create existing peer
        existing_peer = PeerIdentity(
            peer_id="peer1",
            public_key=b"public_key",
            certificate=PeerCertificate(peer_id="peer1", certificate_pem=b"cert"),
            successful_auths=5
        )
        peer_auth_service.peers["peer1"] = existing_peer
        
        with patch.object(peer_auth_service, 'validate_certificate') as mock_validate:
            mock_validate.return_value = CertificateStatus.VALID
            
            peer = await peer_auth_service.authenticate_peer("peer1", b"valid_cert")
            
            assert peer.successful_auths == 6  # Incremented
            assert peer.last_seen > existing_peer.last_seen
    
    async def test_create_challenge(self, peer_auth_service):
        """Test challenge creation."""
        challenge = await peer_auth_service.create_challenge("peer1")
        
        assert challenge.peer_id == "peer1"
        assert len(challenge.challenge_data) == 32
        assert len(challenge.challenge_id) > 0
        assert not challenge.is_expired
        assert challenge.challenge_id in peer_auth_service.active_challenges
    
    async def test_verify_challenge_response_success(self, peer_auth_service):
        """Test successful challenge response verification."""
        challenge = await peer_auth_service.create_challenge("peer1")
        
        # Use correct response
        result = await peer_auth_service.verify_challenge_response(
            challenge.challenge_id, 
            challenge.expected_response
        )
        
        assert result is True
        assert challenge.challenge_id not in peer_auth_service.active_challenges
    
    async def test_verify_challenge_response_failure(self, peer_auth_service):
        """Test failed challenge response verification."""
        challenge = await peer_auth_service.create_challenge("peer1")
        
        # Use incorrect response
        result = await peer_auth_service.verify_challenge_response(
            challenge.challenge_id, 
            b"wrong_response"
        )
        
        assert result is False
        assert challenge.challenge_id not in peer_auth_service.active_challenges
    
    async def test_verify_challenge_not_found(self, peer_auth_service):
        """Test challenge verification with non-existent challenge."""
        with pytest.raises(ChallengeError, match="Challenge not found"):
            await peer_auth_service.verify_challenge_response("nonexistent", b"response")
    
    async def test_verify_challenge_expired(self, peer_auth_service):
        """Test challenge verification with expired challenge."""
        challenge = await peer_auth_service.create_challenge("peer1")
        
        # Manually expire the challenge
        challenge.expires_at = datetime.utcnow() - timedelta(minutes=1)
        
        with pytest.raises(ChallengeError, match="Challenge expired"):
            await peer_auth_service.verify_challenge_response(
                challenge.challenge_id, 
                challenge.expected_response
            )
    
    async def test_establish_trust_new(self, peer_auth_service):
        """Test establishing new trust relationship."""
        relationship = await peer_auth_service.establish_trust(
            "peer1", 
            TrustLevel.TRUSTED,
            ["peer2", "peer3"]
        )
        
        assert relationship.peer_a == "test_peer"
        assert relationship.peer_b == "peer1"
        assert relationship.trust_level == TrustLevel.TRUSTED
        assert "peer2" in relationship.verified_by
        assert "peer3" in relationship.verified_by
        
        key = ("test_peer", "peer1")
        assert key in peer_auth_service.trust_relationships
    
    async def test_establish_trust_existing(self, peer_auth_service):
        """Test updating existing trust relationship."""
        # Create existing relationship
        key = ("test_peer", "peer1")
        existing = TrustRelationship(
            peer_a="test_peer",
            peer_b="peer1",
            trust_level=TrustLevel.BASIC,
            interaction_count=5
        )
        peer_auth_service.trust_relationships[key] = existing
        
        relationship = await peer_auth_service.establish_trust("peer1", TrustLevel.VERIFIED)
        
        assert relationship.trust_level == TrustLevel.VERIFIED
        assert relationship.interaction_count == 6  # Incremented
    
    async def test_revoke_certificate(self, peer_auth_service):
        """Test certificate revocation."""
        # Create certificate
        cert = PeerCertificate(
            peer_id="peer1",
            certificate_pem=b"cert_data",
            fingerprint="test_fingerprint"
        )
        peer_auth_service.certificates["peer1"] = cert
        
        # Create peer
        peer = PeerIdentity(
            peer_id="peer1",
            public_key=b"key",
            certificate=cert,
            trust_level=TrustLevel.TRUSTED
        )
        peer_auth_service.peers["peer1"] = peer
        
        result = await peer_auth_service.revoke_certificate("peer1", "Test revocation")
        
        assert result is True
        assert "test_fingerprint" in peer_auth_service.revoked_certificates
        assert peer_auth_service.peers["peer1"].trust_level == TrustLevel.UNTRUSTED
    
    async def test_revoke_certificate_not_found(self, peer_auth_service):
        """Test revoking non-existent certificate."""
        result = await peer_auth_service.revoke_certificate("nonexistent")
        assert result is False
    
    async def test_get_peer_reputation(self, peer_auth_service):
        """Test getting peer reputation."""
        # Test non-existent peer
        reputation = await peer_auth_service.get_peer_reputation("nonexistent")
        assert reputation == 0.0
        
        # Test existing peer
        peer = PeerIdentity(
            peer_id="peer1",
            public_key=b"key",
            certificate=PeerCertificate(peer_id="peer1", certificate_pem=b"cert"),
            reputation_score=0.75
        )
        peer_auth_service.peers["peer1"] = peer
        
        reputation = await peer_auth_service.get_peer_reputation("peer1")
        assert reputation == 0.75
    
    async def test_create_ssl_context_no_crypto(self, peer_auth_service):
        """Test SSL context creation without crypto."""
        context = await peer_auth_service.create_ssl_context()
        assert context is None
    
    async def test_persistence(self, peer_auth_service):
        """Test data persistence across service restarts."""
        # Create test data
        cert = PeerCertificate(
            peer_id="peer1",
            certificate_pem=b"cert_data",
            public_key_pem=b"public_key"
        )
        peer_auth_service.certificates["peer1"] = cert
        
        peer = PeerIdentity(
            peer_id="peer1",
            public_key=b"public_key",
            certificate=cert,
            trust_level=TrustLevel.TRUSTED,
            reputation_score=0.8
        )
        peer_auth_service.peers["peer1"] = peer
        
        relationship = TrustRelationship(
            peer_a="test_peer",
            peer_b="peer1",
            trust_level=TrustLevel.TRUSTED
        )
        peer_auth_service.trust_relationships[("test_peer", "peer1")] = relationship
        
        peer_auth_service.revoked_certificates.add("revoked_fingerprint")
        
        # Stop and restart service
        data_dir = peer_auth_service.data_dir
        await peer_auth_service.stop()
        
        new_service = PeerAuthenticationService(data_dir, "test_peer")
        await new_service.start()
        
        try:
            # Verify data persisted
            assert "peer1" in new_service.certificates
            assert "peer1" in new_service.peers
            assert ("test_peer", "peer1") in new_service.trust_relationships
            assert "revoked_fingerprint" in new_service.revoked_certificates
            
            # Verify data integrity
            loaded_peer = new_service.peers["peer1"]
            assert loaded_peer.trust_level == TrustLevel.TRUSTED
            assert loaded_peer.reputation_score == 0.8
            
        finally:
            await new_service.stop()


class TestPeerCertificate:
    """Test peer certificate functionality."""
    
    def test_certificate_creation(self):
        """Test certificate creation."""
        cert = PeerCertificate(
            peer_id="test_peer",
            certificate_pem=b"cert_data"
        )
        
        assert cert.peer_id == "test_peer"
        assert cert.certificate_pem == b"cert_data"
        assert cert.subject == "CN=test_peer"
        assert len(cert.fingerprint) > 0
    
    def test_certificate_expiry(self):
        """Test certificate expiry checking."""
        # Create expired certificate
        expired_cert = PeerCertificate(
            peer_id="test_peer",
            certificate_pem=b"cert_data",
            expires_at=datetime.utcnow() - timedelta(days=1)
        )
        
        assert expired_cert.is_expired is True
        assert expired_cert.days_until_expiry == 0
        
        # Create valid certificate
        valid_cert = PeerCertificate(
            peer_id="test_peer",
            certificate_pem=b"cert_data",
            expires_at=datetime.utcnow() + timedelta(days=30)
        )
        
        assert valid_cert.is_expired is False
        assert valid_cert.days_until_expiry == 30


class TestPeerIdentity:
    """Test peer identity functionality."""
    
    def test_identity_creation(self):
        """Test peer identity creation."""
        cert = PeerCertificate(peer_id="peer1", certificate_pem=b"cert")
        
        identity = PeerIdentity(
            peer_id="peer1",
            public_key=b"public_key",
            certificate=cert,
            trust_level=TrustLevel.TRUSTED,
            successful_auths=10,
            failed_auths=2
        )
        
        assert identity.peer_id == "peer1"
        assert identity.trust_level == TrustLevel.TRUSTED
        assert identity.success_rate == 10/12  # 10 successful out of 12 total
    
    def test_success_rate_no_attempts(self):
        """Test success rate with no authentication attempts."""
        cert = PeerCertificate(peer_id="peer1", certificate_pem=b"cert")
        
        identity = PeerIdentity(
            peer_id="peer1",
            public_key=b"public_key",
            certificate=cert
        )
        
        assert identity.success_rate == 0.0


class TestTrustRelationship:
    """Test trust relationship functionality."""
    
    def test_relationship_creation(self):
        """Test trust relationship creation."""
        relationship = TrustRelationship(
            peer_a="peer1",
            peer_b="peer2",
            trust_level=TrustLevel.VERIFIED,
            verified_by=["peer3", "peer4"],
            reputation_score=0.9
        )
        
        assert relationship.peer_a == "peer1"
        assert relationship.peer_b == "peer2"
        assert relationship.trust_level == TrustLevel.VERIFIED
        assert "peer3" in relationship.verified_by
        assert relationship.reputation_score == 0.9


class TestAuthenticationChallenge:
    """Test authentication challenge functionality."""
    
    def test_challenge_creation(self):
        """Test challenge creation."""
        challenge = AuthenticationChallenge(
            challenge_id="test_id",
            peer_id="peer1",
            challenge_data=b"challenge",
            expected_response=b"response"
        )
        
        assert challenge.challenge_id == "test_id"
        assert challenge.peer_id == "peer1"
        assert challenge.challenge_data == b"challenge"
        assert challenge.expected_response == b"response"
        assert not challenge.is_expired
    
    def test_challenge_expiry(self):
        """Test challenge expiry checking."""
        expired_challenge = AuthenticationChallenge(
            challenge_id="test_id",
            peer_id="peer1",
            challenge_data=b"challenge",
            expected_response=b"response",
            expires_at=datetime.utcnow() - timedelta(minutes=1)
        )
        
        assert expired_challenge.is_expired is True


if __name__ == "__main__":
    pytest.main([__file__])