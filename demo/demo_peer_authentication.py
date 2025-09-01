#!/usr/bin/env python3
"""
Demo script for DittoFS Peer Authentication Service.

This script demonstrates the peer-to-peer authentication features including:
- Peer certificate generation and management
- Mutual authentication between peers
- Certificate validation and revocation
- Trust relationship management
- Challenge-response authentication
- Reputation scoring system
"""

import asyncio
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from src.dittofs.peer_authentication import (
    PeerAuthenticationService,
    PeerCertificate,
    TrustLevel,
    CertificateStatus,
    PeerAuthenticationError,
    CertificateError
)


async def demo_certificate_management():
    """Demonstrate certificate generation and management."""
    print("=== Certificate Management Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create peer authentication service
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Creating peer authentication service for 'alice'...")
            print(f"Peer ID: {peer_service.peer_id}")
            print(f"Data directory: {peer_service.data_dir}")
            
            # Try to generate certificate
            print("\nAttempting to generate peer certificate...")
            try:
                cert = await peer_service.generate_peer_certificate("alice")
                print(f"✓ Certificate generated successfully!")
                print(f"  Peer ID: {cert.peer_id}")
                print(f"  Subject: {cert.subject}")
                print(f"  Issued at: {cert.issued_at}")
                print(f"  Expires at: {cert.expires_at}")
                print(f"  Days until expiry: {cert.days_until_expiry}")
                print(f"  Fingerprint: {cert.fingerprint[:16]}...")
                
                # Validate the certificate
                print("\nValidating certificate...")
                status = await peer_service.validate_certificate(
                    cert.certificate_pem, 
                    "alice"
                )
                print(f"✓ Certificate status: {status.value}")
                
            except CertificateError as e:
                print(f"⚠ Certificate generation failed: {e}")
                print("  This is expected if cryptography libraries are not available")
            
        finally:
            await peer_service.stop()


async def demo_peer_authentication():
    """Demonstrate peer-to-peer authentication."""
    print("\n=== Peer Authentication Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create two peer services
        alice_service = PeerAuthenticationService(Path(temp_dir) / "alice", "alice")
        bob_service = PeerAuthenticationService(Path(temp_dir) / "bob", "bob")
        
        await alice_service.start()
        await bob_service.start()
        
        try:
            print("Created peer services for 'alice' and 'bob'")
            
            # Create mock certificate for demonstration
            mock_cert_pem = b"""-----BEGIN CERTIFICATE-----
MIICXjCCAUYCAQAwDQYJKoZIhvcNAQELBQAwEjEQMA4GA1UEAwwHYm9iX3BlZXIw
HhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAwMDAwWjASMRAwDgYDVQQDDAdib2Jf
cGVlcjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL5Q...
-----END CERTIFICATE-----"""
            
            print("\nAttempting peer authentication...")
            
            # Mock certificate validation for demo
            with tempfile.NamedTemporaryFile() as mock_validate:
                try:
                    # Try to authenticate bob with alice
                    peer_identity = await alice_service.authenticate_peer("bob", mock_cert_pem)
                    print(f"✓ Peer authenticated successfully!")
                    print(f"  Peer ID: {peer_identity.peer_id}")
                    print(f"  Trust level: {peer_identity.trust_level.value}")
                    print(f"  Successful auths: {peer_identity.successful_auths}")
                    print(f"  Reputation score: {peer_identity.reputation_score}")
                    
                except PeerAuthenticationError as e:
                    print(f"⚠ Peer authentication failed: {e}")
                    print("  This is expected without valid certificates")
            
        finally:
            await alice_service.stop()
            await bob_service.stop()


async def demo_challenge_response():
    """Demonstrate challenge-response authentication."""
    print("\n=== Challenge-Response Authentication Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Creating authentication challenge...")
            
            # Create challenge for peer
            challenge = await peer_service.create_challenge("bob")
            print(f"✓ Challenge created!")
            print(f"  Challenge ID: {challenge.challenge_id[:16]}...")
            print(f"  Peer ID: {challenge.peer_id}")
            print(f"  Challenge data length: {len(challenge.challenge_data)} bytes")
            print(f"  Expires at: {challenge.expires_at}")
            print(f"  Is expired: {challenge.is_expired}")
            
            # Verify correct response
            print("\nVerifying correct challenge response...")
            result = await peer_service.verify_challenge_response(
                challenge.challenge_id,
                challenge.expected_response
            )
            print(f"✓ Challenge response result: {result}")
            
            # Try to verify again (should fail - challenge consumed)
            print("\nTrying to verify challenge again...")
            try:
                await peer_service.verify_challenge_response(
                    challenge.challenge_id,
                    challenge.expected_response
                )
                print("✗ Should have failed")
            except Exception as e:
                print(f"✓ Challenge verification failed as expected: {e}")
            
            # Create new challenge and test wrong response
            print("\nTesting incorrect challenge response...")
            challenge2 = await peer_service.create_challenge("charlie")
            result = await peer_service.verify_challenge_response(
                challenge2.challenge_id,
                b"wrong_response"
            )
            print(f"✓ Incorrect response result: {result}")
            
        finally:
            await peer_service.stop()


async def demo_trust_management():
    """Demonstrate trust relationship management."""
    print("\n=== Trust Management Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Managing trust relationships...")
            
            # Establish trust with different peers
            peers_and_trust = [
                ("bob", TrustLevel.BASIC, ["charlie"]),
                ("charlie", TrustLevel.TRUSTED, ["bob", "diana"]),
                ("diana", TrustLevel.VERIFIED, ["alice", "bob", "charlie"])
            ]
            
            for peer_id, trust_level, verified_by in peers_and_trust:
                relationship = await peer_service.establish_trust(
                    peer_id, trust_level, verified_by
                )
                print(f"✓ Established {trust_level.value} trust with {peer_id}")
                print(f"  Verified by: {', '.join(verified_by)}")
                print(f"  Established at: {relationship.established_at}")
            
            # Display trust relationships
            print(f"\nTotal trust relationships: {len(peer_service.trust_relationships)}")
            for key, relationship in peer_service.trust_relationships.items():
                print(f"  {key[0]} -> {key[1]}: {relationship.trust_level.value}")
            
            # Update existing trust
            print(f"\nUpdating trust level for bob...")
            updated = await peer_service.establish_trust("bob", TrustLevel.VERIFIED)
            print(f"✓ Updated trust level to: {updated.trust_level.value}")
            print(f"  Interaction count: {updated.interaction_count}")
            
        finally:
            await peer_service.stop()


async def demo_certificate_revocation():
    """Demonstrate certificate revocation."""
    print("\n=== Certificate Revocation Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Demonstrating certificate revocation...")
            
            # Create mock certificate
            mock_cert = PeerCertificate(
                peer_id="malicious_peer",
                certificate_pem=b"mock_certificate_data",
                fingerprint="mock_fingerprint_12345"
            )
            peer_service.certificates["malicious_peer"] = mock_cert
            
            print(f"Created certificate for: {mock_cert.peer_id}")
            print(f"Certificate fingerprint: {mock_cert.fingerprint}")
            
            # Revoke certificate
            print(f"\nRevoking certificate...")
            result = await peer_service.revoke_certificate(
                "malicious_peer", 
                "Suspected malicious activity"
            )
            print(f"✓ Certificate revoked: {result}")
            
            # Check revocation status
            print(f"Revoked certificates count: {len(peer_service.revoked_certificates)}")
            print(f"Certificate in revoked list: {mock_cert.fingerprint in peer_service.revoked_certificates}")
            
            # Try to validate revoked certificate
            print(f"\nValidating revoked certificate...")
            status = await peer_service.validate_certificate(
                mock_cert.certificate_pem,
                "malicious_peer"
            )
            print(f"✓ Revoked certificate status: {status.value}")
            
        finally:
            await peer_service.stop()


async def demo_reputation_system():
    """Demonstrate peer reputation scoring."""
    print("\n=== Reputation System Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Demonstrating reputation scoring...")
            
            # Create mock peer with authentication history
            from src.dittofs.peer_authentication import PeerIdentity
            
            peer = PeerIdentity(
                peer_id="bob",
                public_key=b"mock_public_key",
                certificate=PeerCertificate(peer_id="bob", certificate_pem=b"cert"),
                successful_auths=15,
                failed_auths=3,
                reputation_score=0.7
            )
            peer_service.peers["bob"] = peer
            
            print(f"Created peer: {peer.peer_id}")
            print(f"  Successful auths: {peer.successful_auths}")
            print(f"  Failed auths: {peer.failed_auths}")
            print(f"  Success rate: {peer.success_rate:.2%}")
            print(f"  Initial reputation: {peer.reputation_score}")
            
            # Get reputation
            reputation = await peer_service.get_peer_reputation("bob")
            print(f"✓ Current reputation score: {reputation}")
            
            # Simulate successful interaction
            print(f"\nSimulating successful authentication...")
            await peer_service._update_peer_reputation("bob", success=True)
            new_reputation = await peer_service.get_peer_reputation("bob")
            print(f"✓ Updated reputation after success: {new_reputation}")
            
            # Simulate failed interaction
            print(f"\nSimulating failed authentication...")
            await peer_service._update_peer_reputation("bob", success=False)
            final_reputation = await peer_service.get_peer_reputation("bob")
            print(f"✓ Updated reputation after failure: {final_reputation}")
            
            # Test non-existent peer
            unknown_reputation = await peer_service.get_peer_reputation("unknown")
            print(f"✓ Unknown peer reputation: {unknown_reputation}")
            
        finally:
            await peer_service.stop()


async def demo_persistence():
    """Demonstrate data persistence across service restarts."""
    print("\n=== Persistence Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir)
        
        # First service instance
        print("Creating first service instance...")
        service1 = PeerAuthenticationService(data_dir, "alice")
        await service1.start()
        
        # Create test data
        mock_cert = PeerCertificate(
            peer_id="bob",
            certificate_pem=b"mock_certificate_data"
        )
        service1.certificates["bob"] = mock_cert
        
        await service1.establish_trust("bob", TrustLevel.TRUSTED, ["charlie"])
        service1.revoked_certificates.add("revoked_fingerprint")
        
        print(f"Created test data in first instance:")
        print(f"  Certificates: {len(service1.certificates)}")
        print(f"  Trust relationships: {len(service1.trust_relationships)}")
        print(f"  Revoked certificates: {len(service1.revoked_certificates)}")
        
        await service1.stop()
        
        # Second service instance
        print(f"\nCreating second service instance...")
        service2 = PeerAuthenticationService(data_dir, "alice")
        await service2.start()
        
        try:
            # Verify data persisted
            print(f"Loaded data in second instance:")
            print(f"  Certificates: {len(service2.certificates)}")
            print(f"  Trust relationships: {len(service2.trust_relationships)}")
            print(f"  Revoked certificates: {len(service2.revoked_certificates)}")
            
            if "bob" in service2.certificates:
                print(f"✓ Certificate for 'bob' persisted")
            
            if ("alice", "bob") in service2.trust_relationships:
                relationship = service2.trust_relationships[("alice", "bob")]
                print(f"✓ Trust relationship persisted: {relationship.trust_level.value}")
            
            if "revoked_fingerprint" in service2.revoked_certificates:
                print(f"✓ Revoked certificate list persisted")
                
        finally:
            await service2.stop()


async def demo_ssl_context():
    """Demonstrate SSL context creation."""
    print("\n=== SSL Context Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        peer_service = PeerAuthenticationService(Path(temp_dir), "alice")
        await peer_service.start()
        
        try:
            print("Creating SSL context for secure connections...")
            
            # Try to create SSL context
            server_context = await peer_service.create_ssl_context(server_side=True)
            client_context = await peer_service.create_ssl_context(server_side=False)
            
            if server_context:
                print(f"✓ Server SSL context created")
                print(f"  Context type: {type(server_context)}")
            else:
                print(f"⚠ SSL context creation failed")
                print(f"  This is expected without cryptography libraries or certificates")
            
            if client_context:
                print(f"✓ Client SSL context created")
                print(f"  Context type: {type(client_context)}")
            else:
                print(f"⚠ Client SSL context creation failed")
                print(f"  This is expected without cryptography libraries or certificates")
                
        finally:
            await peer_service.stop()


async def main():
    """Run all peer authentication demos."""
    print("DittoFS Peer Authentication Service Demo")
    print("=" * 50)
    
    demos = [
        demo_certificate_management,
        demo_peer_authentication,
        demo_challenge_response,
        demo_trust_management,
        demo_certificate_revocation,
        demo_reputation_system,
        demo_persistence,
        demo_ssl_context
    ]
    
    for demo in demos:
        try:
            await demo()
        except Exception as e:
            print(f"Demo failed: {e}")
        print()  # Add spacing between demos
    
    print("Peer authentication demo completed!")


if __name__ == "__main__":
    asyncio.run(main())