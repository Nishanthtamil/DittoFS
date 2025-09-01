"""
Peer-to-peer authentication system for DittoFS.

This module provides comprehensive peer authentication including:
- Peer certificate generation and management
- Mutual TLS authentication for peer connections
- Certificate validation and revocation checking
- Peer trust management with reputation scoring
"""

import asyncio
import hashlib
import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import logging

try:
    import cryptography
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
    from cryptography.x509.oid import NameOID, ExtensionOID
    import ssl
    CRYPTO_AVAILABLE = True
except ImportError:
    cryptography = None
    x509 = None
    hashes = None
    serialization = None
    rsa = None
    padding = None
    ssl = None
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)


class TrustLevel(Enum):
    """Trust levels for peer relationships."""
    UNKNOWN = "unknown"
    UNTRUSTED = "untrusted"
    BASIC = "basic"
    TRUSTED = "trusted"
    VERIFIED = "verified"


class CertificateStatus(Enum):
    """Certificate status values."""
    VALID = "valid"
    EXPIRED = "expired"
    REVOKED = "revoked"
    INVALID = "invalid"


@dataclass
class PeerCertificate:
    """Peer certificate information."""
    peer_id: str
    certificate_pem: bytes
    private_key_pem: Optional[bytes] = None
    public_key_pem: bytes = field(default=b"")
    issued_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(days=365))
    issuer: str = "DittoFS"
    subject: str = ""
    serial_number: int = 0
    fingerprint: str = ""
    
    def __post_init__(self):
        """Initialize computed fields."""
        if not self.subject:
            self.subject = f"CN={self.peer_id}"
        if not self.fingerprint and self.certificate_pem:
            self.fingerprint = self._compute_fingerprint()
    
    def _compute_fingerprint(self) -> str:
        """Compute certificate fingerprint."""
        return hashlib.sha256(self.certificate_pem).hexdigest()
    
    @property
    def is_expired(self) -> bool:
        """Check if certificate is expired."""
        return datetime.utcnow() > self.expires_at
    
    @property
    def days_until_expiry(self) -> int:
        """Get days until certificate expires."""
        delta = self.expires_at - datetime.utcnow()
        return max(0, delta.days)


@dataclass
class PeerIdentity:
    """Peer identity information."""
    peer_id: str
    public_key: bytes
    certificate: PeerCertificate
    trust_level: TrustLevel = TrustLevel.UNKNOWN
    capabilities: List[str] = field(default_factory=list)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    reputation_score: float = 0.0
    connection_count: int = 0
    successful_auths: int = 0
    failed_auths: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        """Calculate authentication success rate."""
        total = self.successful_auths + self.failed_auths
        return self.successful_auths / total if total > 0 else 0.0


@dataclass
class TrustRelationship:
    """Trust relationship between peers."""
    peer_a: str
    peer_b: str
    trust_level: TrustLevel
    established_at: datetime = field(default_factory=datetime.utcnow)
    verified_by: List[str] = field(default_factory=list)
    reputation_score: float = 0.0
    last_interaction: datetime = field(default_factory=datetime.utcnow)
    interaction_count: int = 0
    notes: str = ""


@dataclass
class AuthenticationChallenge:
    """Authentication challenge for peer verification."""
    challenge_id: str
    peer_id: str
    challenge_data: bytes
    expected_response: bytes
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    
    @property
    def is_expired(self) -> bool:
        """Check if challenge is expired."""
        return datetime.utcnow() > self.expires_at


class PeerAuthenticationError(Exception):
    """Base exception for peer authentication errors."""
    pass


class CertificateError(PeerAuthenticationError):
    """Raised when certificate validation fails."""
    pass


class TrustError(PeerAuthenticationError):
    """Raised when trust verification fails."""
    pass


class ChallengeError(PeerAuthenticationError):
    """Raised when challenge-response fails."""
    pass


class PeerAuthenticationService:
    """
    Comprehensive peer-to-peer authentication service.
    
    Provides certificate management, mutual authentication, and trust scoring.
    """
    
    def __init__(self, data_dir: Path, peer_id: str):
        """
        Initialize peer authentication service.
        
        Args:
            data_dir: Directory for storing authentication data
            peer_id: Unique identifier for this peer
        """
        self.data_dir = data_dir
        self.peer_id = peer_id
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Storage files
        self.certificates_file = data_dir / "certificates.json"
        self.peers_file = data_dir / "peers.json"
        self.trust_file = data_dir / "trust_relationships.json"
        self.revoked_file = data_dir / "revoked_certificates.json"
        
        # In-memory storage
        self.certificates: Dict[str, PeerCertificate] = {}
        self.peers: Dict[str, PeerIdentity] = {}
        self.trust_relationships: Dict[Tuple[str, str], TrustRelationship] = {}
        self.revoked_certificates: Set[str] = set()
        self.active_challenges: Dict[str, AuthenticationChallenge] = {}
        
        # Own certificate
        self.own_certificate: Optional[PeerCertificate] = None
        
        # Load existing data
        self._load_data()
        
        # Cleanup task
        self._cleanup_task = None
    
    async def start(self):
        """Start the peer authentication service."""
        if not CRYPTO_AVAILABLE:
            logger.warning("Cryptography libraries not available - limited functionality")
            return
        
        # Generate own certificate if not exists
        if not self.own_certificate:
            await self.generate_peer_certificate(self.peer_id)
        
        # Start periodic cleanup
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info(f"Peer authentication service started for peer: {self.peer_id}")
    
    async def stop(self):
        """Stop the peer authentication service."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Save current state
        self._save_data()
        logger.info("Peer authentication service stopped")
    
    async def generate_peer_certificate(self, peer_id: str, 
                                      validity_days: int = 365) -> PeerCertificate:
        """
        Generate a new peer certificate.
        
        Args:
            peer_id: Peer identifier
            validity_days: Certificate validity period in days
            
        Returns:
            Generated peer certificate
            
        Raises:
            CertificateError: If certificate generation fails
        """
        if not CRYPTO_AVAILABLE:
            raise CertificateError("Certificate generation requires cryptography libraries")
        
        try:
            # Generate private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
            )
            
            # Generate certificate
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, peer_id),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DittoFS"),
                x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Peer Network"),
            ])
            
            now = datetime.utcnow()
            expires = now + timedelta(days=validity_days)
            
            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                issuer
            ).public_key(
                private_key.public_key()
            ).serial_number(
                secrets.randbits(64)
            ).not_valid_before(
                now
            ).not_valid_after(
                expires
            ).add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName(peer_id),
                ]),
                critical=False,
            ).add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            ).add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    key_encipherment=True,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    content_commitment=False,
                    data_encipherment=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            ).sign(private_key, hashes.SHA256())
            
            # Serialize certificate and keys
            cert_pem = cert.public_bytes(Encoding.PEM)
            private_key_pem = private_key.private_bytes(
                Encoding.PEM,
                PrivateFormat.PKCS8,
                NoEncryption()
            )
            public_key_pem = private_key.public_key().public_bytes(
                Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            # Create certificate object
            peer_cert = PeerCertificate(
                peer_id=peer_id,
                certificate_pem=cert_pem,
                private_key_pem=private_key_pem,
                public_key_pem=public_key_pem,
                issued_at=now,
                expires_at=expires,
                serial_number=cert.serial_number,
                subject=f"CN={peer_id}"
            )
            
            # Store certificate
            self.certificates[peer_id] = peer_cert
            
            # Set as own certificate if generating for self
            if peer_id == self.peer_id:
                self.own_certificate = peer_cert
            
            self._save_data()
            
            logger.info(f"Generated certificate for peer: {peer_id}")
            return peer_cert
            
        except Exception as e:
            raise CertificateError(f"Failed to generate certificate: {e}")
    
    async def validate_certificate(self, certificate_pem: bytes, 
                                 peer_id: str) -> CertificateStatus:
        """
        Validate a peer certificate.
        
        Args:
            certificate_pem: Certificate in PEM format
            peer_id: Expected peer identifier
            
        Returns:
            Certificate validation status
        """
        if not CRYPTO_AVAILABLE:
            return CertificateStatus.INVALID
        
        try:
            # Load certificate
            cert = x509.load_pem_x509_certificate(certificate_pem)
            
            # Check if revoked
            fingerprint = hashlib.sha256(certificate_pem).hexdigest()
            if fingerprint in self.revoked_certificates:
                return CertificateStatus.REVOKED
            
            # Check expiry
            if datetime.utcnow() > cert.not_valid_after:
                return CertificateStatus.EXPIRED
            
            # Check subject
            subject_cn = None
            for attribute in cert.subject:
                if attribute.oid == NameOID.COMMON_NAME:
                    subject_cn = attribute.value
                    break
            
            if subject_cn != peer_id:
                logger.warning(f"Certificate subject mismatch: expected {peer_id}, got {subject_cn}")
                return CertificateStatus.INVALID
            
            # Additional validation could include:
            # - Chain of trust verification
            # - CRL checking
            # - OCSP validation
            
            return CertificateStatus.VALID
            
        except Exception as e:
            logger.error(f"Certificate validation failed: {e}")
            return CertificateStatus.INVALID
    
    async def authenticate_peer(self, peer_id: str, 
                              certificate_pem: bytes) -> PeerIdentity:
        """
        Authenticate a peer using their certificate.
        
        Args:
            peer_id: Peer identifier
            certificate_pem: Peer certificate in PEM format
            
        Returns:
            Authenticated peer identity
            
        Raises:
            PeerAuthenticationError: If authentication fails
        """
        # Validate certificate
        status = await self.validate_certificate(certificate_pem, peer_id)
        if status != CertificateStatus.VALID:
            raise PeerAuthenticationError(f"Certificate validation failed: {status.value}")
        
        # Create or update peer identity
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            peer.last_seen = datetime.utcnow()
            peer.successful_auths += 1
        else:
            # Extract public key
            public_key_pem = b""
            if CRYPTO_AVAILABLE:
                try:
                    cert = x509.load_pem_x509_certificate(certificate_pem)
                    public_key_pem = cert.public_key().public_bytes(
                        Encoding.PEM,
                        serialization.PublicFormat.SubjectPublicKeyInfo
                    )
                except Exception as e:
                    logger.error(f"Failed to extract public key: {e}")
            
            # Create certificate object
            peer_cert = PeerCertificate(
                peer_id=peer_id,
                certificate_pem=certificate_pem,
                public_key_pem=public_key_pem
            )
            
            # Create peer identity
            peer = PeerIdentity(
                peer_id=peer_id,
                public_key=public_key_pem,
                certificate=peer_cert,
                trust_level=TrustLevel.BASIC,
                successful_auths=1
            )
            
            self.peers[peer_id] = peer
        
        # Update reputation
        await self._update_peer_reputation(peer_id, success=True)
        
        self._save_data()
        
        logger.info(f"Peer authenticated successfully: {peer_id}")
        return peer
    
    async def create_challenge(self, peer_id: str) -> AuthenticationChallenge:
        """
        Create an authentication challenge for a peer.
        
        Args:
            peer_id: Peer identifier
            
        Returns:
            Authentication challenge
        """
        challenge_id = secrets.token_urlsafe(32)
        challenge_data = secrets.token_bytes(32)
        
        # For now, expected response is just the challenge data
        # In production, this would involve cryptographic operations
        expected_response = challenge_data
        
        challenge = AuthenticationChallenge(
            challenge_id=challenge_id,
            peer_id=peer_id,
            challenge_data=challenge_data,
            expected_response=expected_response
        )
        
        self.active_challenges[challenge_id] = challenge
        
        logger.info(f"Created challenge for peer: {peer_id}")
        return challenge
    
    async def verify_challenge_response(self, challenge_id: str, 
                                      response: bytes) -> bool:
        """
        Verify a challenge response from a peer.
        
        Args:
            challenge_id: Challenge identifier
            response: Peer's response to the challenge
            
        Returns:
            True if response is valid
            
        Raises:
            ChallengeError: If challenge verification fails
        """
        if challenge_id not in self.active_challenges:
            raise ChallengeError("Challenge not found")
        
        challenge = self.active_challenges[challenge_id]
        
        if challenge.is_expired:
            del self.active_challenges[challenge_id]
            raise ChallengeError("Challenge expired")
        
        # Verify response
        is_valid = response == challenge.expected_response
        
        # Clean up challenge
        del self.active_challenges[challenge_id]
        
        # Update peer reputation
        await self._update_peer_reputation(challenge.peer_id, success=is_valid)
        
        if is_valid:
            logger.info(f"Challenge response verified for peer: {challenge.peer_id}")
        else:
            logger.warning(f"Challenge response failed for peer: {challenge.peer_id}")
        
        return is_valid
    
    async def establish_trust(self, peer_id: str, trust_level: TrustLevel,
                            verified_by: Optional[List[str]] = None) -> TrustRelationship:
        """
        Establish a trust relationship with a peer.
        
        Args:
            peer_id: Peer identifier
            trust_level: Level of trust to establish
            verified_by: List of peers who verified this relationship
            
        Returns:
            Trust relationship
        """
        relationship_key = (self.peer_id, peer_id)
        
        if relationship_key in self.trust_relationships:
            # Update existing relationship
            relationship = self.trust_relationships[relationship_key]
            relationship.trust_level = trust_level
            relationship.last_interaction = datetime.utcnow()
            relationship.interaction_count += 1
            if verified_by:
                relationship.verified_by.extend(verified_by)
        else:
            # Create new relationship
            relationship = TrustRelationship(
                peer_a=self.peer_id,
                peer_b=peer_id,
                trust_level=trust_level,
                verified_by=verified_by or []
            )
            self.trust_relationships[relationship_key] = relationship
        
        # Update peer trust level
        if peer_id in self.peers:
            self.peers[peer_id].trust_level = trust_level
        
        self._save_data()
        
        logger.info(f"Established {trust_level.value} trust with peer: {peer_id}")
        return relationship
    
    async def revoke_certificate(self, peer_id: str, reason: str = "") -> bool:
        """
        Revoke a peer certificate.
        
        Args:
            peer_id: Peer identifier
            reason: Reason for revocation
            
        Returns:
            True if certificate was revoked
        """
        if peer_id not in self.certificates:
            return False
        
        cert = self.certificates[peer_id]
        self.revoked_certificates.add(cert.fingerprint)
        
        # Update peer trust level
        if peer_id in self.peers:
            self.peers[peer_id].trust_level = TrustLevel.UNTRUSTED
        
        self._save_data()
        
        logger.warning(f"Revoked certificate for peer: {peer_id}, reason: {reason}")
        return True
    
    async def get_peer_reputation(self, peer_id: str) -> float:
        """
        Get reputation score for a peer.
        
        Args:
            peer_id: Peer identifier
            
        Returns:
            Reputation score (0.0 to 1.0)
        """
        if peer_id not in self.peers:
            return 0.0
        
        peer = self.peers[peer_id]
        return peer.reputation_score
    
    async def create_ssl_context(self, server_side: bool = False) -> Optional[Any]:
        """
        Create SSL context for secure peer connections.
        
        Args:
            server_side: Whether this is for server-side connections
            
        Returns:
            SSL context or None if crypto not available
        """
        if not CRYPTO_AVAILABLE or not self.own_certificate:
            return None
        
        try:
            # Create SSL context
            if server_side:
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_REQUIRED
            
            # Load own certificate and key
            cert_file = self.data_dir / f"{self.peer_id}.crt"
            key_file = self.data_dir / f"{self.peer_id}.key"
            
            # Write certificate and key to temporary files
            cert_file.write_bytes(self.own_certificate.certificate_pem)
            if self.own_certificate.private_key_pem:
                key_file.write_bytes(self.own_certificate.private_key_pem)
            
            context.load_cert_chain(str(cert_file), str(key_file))
            
            return context
            
        except Exception as e:
            logger.error(f"Failed to create SSL context: {e}")
            return None
    
    async def _update_peer_reputation(self, peer_id: str, success: bool):
        """Update peer reputation based on interaction outcome."""
        if peer_id not in self.peers:
            return
        
        peer = self.peers[peer_id]
        
        # Simple reputation algorithm
        if success:
            peer.reputation_score = min(1.0, peer.reputation_score + 0.1)
        else:
            peer.reputation_score = max(0.0, peer.reputation_score - 0.2)
            peer.failed_auths += 1
        
        # Factor in success rate
        success_rate = peer.success_rate
        peer.reputation_score = (peer.reputation_score * 0.7) + (success_rate * 0.3)
    
    def _load_data(self):
        """Load authentication data from storage."""
        try:
            # Load certificates
            if self.certificates_file.exists():
                data = json.loads(self.certificates_file.read_text())
                for peer_id, cert_data in data.items():
                    cert_data['issued_at'] = datetime.fromisoformat(cert_data['issued_at'])
                    cert_data['expires_at'] = datetime.fromisoformat(cert_data['expires_at'])
                    cert_data['certificate_pem'] = cert_data['certificate_pem'].encode()
                    if cert_data.get('private_key_pem'):
                        cert_data['private_key_pem'] = cert_data['private_key_pem'].encode()
                    if cert_data.get('public_key_pem'):
                        cert_data['public_key_pem'] = cert_data['public_key_pem'].encode()
                    
                    cert = PeerCertificate(**cert_data)
                    self.certificates[peer_id] = cert
                    
                    # Set own certificate
                    if peer_id == self.peer_id:
                        self.own_certificate = cert
            
            # Load peers
            if self.peers_file.exists():
                data = json.loads(self.peers_file.read_text())
                for peer_id, peer_data in data.items():
                    peer_data['last_seen'] = datetime.fromisoformat(peer_data['last_seen'])
                    peer_data['trust_level'] = TrustLevel(peer_data['trust_level'])
                    peer_data['public_key'] = peer_data['public_key'].encode()
                    
                    # Reconstruct certificate
                    cert_data = peer_data.pop('certificate')
                    cert_data['issued_at'] = datetime.fromisoformat(cert_data['issued_at'])
                    cert_data['expires_at'] = datetime.fromisoformat(cert_data['expires_at'])
                    cert_data['certificate_pem'] = cert_data['certificate_pem'].encode()
                    if cert_data.get('public_key_pem'):
                        cert_data['public_key_pem'] = cert_data['public_key_pem'].encode()
                    
                    peer_data['certificate'] = PeerCertificate(**cert_data)
                    
                    self.peers[peer_id] = PeerIdentity(**peer_data)
            
            # Load trust relationships
            if self.trust_file.exists():
                data = json.loads(self.trust_file.read_text())
                for key_str, trust_data in data.items():
                    key = tuple(key_str.split(','))
                    trust_data['established_at'] = datetime.fromisoformat(trust_data['established_at'])
                    trust_data['last_interaction'] = datetime.fromisoformat(trust_data['last_interaction'])
                    trust_data['trust_level'] = TrustLevel(trust_data['trust_level'])
                    
                    self.trust_relationships[key] = TrustRelationship(**trust_data)
            
            # Load revoked certificates
            if self.revoked_file.exists():
                data = json.loads(self.revoked_file.read_text())
                self.revoked_certificates = set(data)
                
        except Exception as e:
            logger.error(f"Failed to load authentication data: {e}")
    
    def _save_data(self):
        """Save authentication data to storage."""
        try:
            # Save certificates
            cert_data = {}
            for peer_id, cert in self.certificates.items():
                cert_data[peer_id] = {
                    'peer_id': cert.peer_id,
                    'certificate_pem': cert.certificate_pem.decode(),
                    'private_key_pem': cert.private_key_pem.decode() if cert.private_key_pem else None,
                    'public_key_pem': cert.public_key_pem.decode() if cert.public_key_pem else "",
                    'issued_at': cert.issued_at.isoformat(),
                    'expires_at': cert.expires_at.isoformat(),
                    'issuer': cert.issuer,
                    'subject': cert.subject,
                    'serial_number': cert.serial_number,
                    'fingerprint': cert.fingerprint
                }
            self.certificates_file.write_text(json.dumps(cert_data, indent=2))
            
            # Save peers
            peer_data = {}
            for peer_id, peer in self.peers.items():
                peer_data[peer_id] = {
                    'peer_id': peer.peer_id,
                    'public_key': peer.public_key.decode(),
                    'certificate': {
                        'peer_id': peer.certificate.peer_id,
                        'certificate_pem': peer.certificate.certificate_pem.decode(),
                        'public_key_pem': peer.certificate.public_key_pem.decode() if peer.certificate.public_key_pem else "",
                        'issued_at': peer.certificate.issued_at.isoformat(),
                        'expires_at': peer.certificate.expires_at.isoformat(),
                        'issuer': peer.certificate.issuer,
                        'subject': peer.certificate.subject,
                        'serial_number': peer.certificate.serial_number,
                        'fingerprint': peer.certificate.fingerprint
                    },
                    'trust_level': peer.trust_level.value,
                    'capabilities': peer.capabilities,
                    'last_seen': peer.last_seen.isoformat(),
                    'reputation_score': peer.reputation_score,
                    'connection_count': peer.connection_count,
                    'successful_auths': peer.successful_auths,
                    'failed_auths': peer.failed_auths,
                    'metadata': peer.metadata
                }
            self.peers_file.write_text(json.dumps(peer_data, indent=2))
            
            # Save trust relationships
            trust_data = {}
            for key, trust in self.trust_relationships.items():
                key_str = f"{key[0]},{key[1]}"
                trust_data[key_str] = {
                    'peer_a': trust.peer_a,
                    'peer_b': trust.peer_b,
                    'trust_level': trust.trust_level.value,
                    'established_at': trust.established_at.isoformat(),
                    'verified_by': trust.verified_by,
                    'reputation_score': trust.reputation_score,
                    'last_interaction': trust.last_interaction.isoformat(),
                    'interaction_count': trust.interaction_count,
                    'notes': trust.notes
                }
            self.trust_file.write_text(json.dumps(trust_data, indent=2))
            
            # Save revoked certificates
            self.revoked_file.write_text(json.dumps(list(self.revoked_certificates)))
            
        except Exception as e:
            logger.error(f"Failed to save authentication data: {e}")
    
    async def _periodic_cleanup(self):
        """Periodically clean up expired challenges and certificates."""
        while True:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes
                
                # Clean up expired challenges
                expired_challenges = [
                    challenge_id for challenge_id, challenge in self.active_challenges.items()
                    if challenge.is_expired
                ]
                
                for challenge_id in expired_challenges:
                    del self.active_challenges[challenge_id]
                
                if expired_challenges:
                    logger.info(f"Cleaned up {len(expired_challenges)} expired challenges")
                
                # Check for expiring certificates
                expiring_soon = []
                for peer_id, cert in self.certificates.items():
                    if cert.days_until_expiry <= 30:  # Warn 30 days before expiry
                        expiring_soon.append((peer_id, cert.days_until_expiry))
                
                for peer_id, days in expiring_soon:
                    logger.warning(f"Certificate for {peer_id} expires in {days} days")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")