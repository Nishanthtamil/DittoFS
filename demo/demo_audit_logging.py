#!/usr/bin/env python3
"""
Demo script for DittoFS Audit Logging System.

This script demonstrates the comprehensive audit logging features including:
- Detailed logging of all security-relevant events
- Tamper-evident audit logs with cryptographic signatures
- Audit log analysis and reporting tools
- Audit log retention and archival policies
"""

import asyncio
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from src.dittofs.audit_logging import (
    AuditLoggingService,
    EventType,
    Severity,
    AuditQuery,
    RetentionPolicy
)


async def demo_basic_logging():
    """Demonstrate basic audit logging."""
    print("=== Basic Audit Logging Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Logging various audit events...")
            
            # Authentication events
            await audit_service.log_event(
                EventType.LOGIN_SUCCESS,
                Severity.MEDIUM,
                user_id="alice",
                peer_id="desktop_001",
                resource="authentication_service",
                action="login",
                result="success",
                details={"method": "password", "ip": "192.168.1.100"},
                source_ip="192.168.1.100",
                session_id="sess_12345"
            )
            print("✓ Logged successful login")
            
            # File operations
            await audit_service.log_event(
                EventType.FILE_CREATED,
                Severity.LOW,
                user_id="alice",
                peer_id="desktop_001",
                resource="/documents/report.pdf",
                action="create",
                result="success",
                details={"size": 1024000, "mime_type": "application/pdf"}
            )
            print("✓ Logged file creation")
            
            # Security events
            await audit_service.log_event(
                EventType.ACCESS_DENIED,
                Severity.HIGH,
                user_id="bob",
                peer_id="mobile_002",
                resource="/admin/config.json",
                action="read",
                result="denied",
                details={"reason": "insufficient_permissions"}
            )
            print("✓ Logged access denial")
            
            # System events
            await audit_service.log_event(
                EventType.CONFIGURATION_CHANGED,
                Severity.MEDIUM,
                user_id="admin",
                peer_id="server_001",
                resource="security_settings",
                action="update",
                result="success",
                details={"setting": "password_policy", "old_value": "weak", "new_value": "strong"}
            )
            print("✓ Logged configuration change")
            
            print(f"\nAudit log file: {audit_service.current_log_file}")
            print(f"Current sequence number: {audit_service.sequence_number}")
            
        finally:
            await audit_service.stop()


async def demo_event_querying():
    """Demonstrate audit event querying."""
    print("\n=== Event Querying Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Creating test audit events...")
            
            # Create various events for querying
            events_to_create = [
                (EventType.LOGIN_SUCCESS, Severity.MEDIUM, "alice", "success"),
                (EventType.LOGIN_SUCCESS, Severity.MEDIUM, "bob", "success"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "charlie", "failure"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "charlie", "failure"),
                (EventType.FILE_CREATED, Severity.LOW, "alice", "success"),
                (EventType.FILE_MODIFIED, Severity.LOW, "bob", "success"),
                (EventType.FILE_DELETED, Severity.MEDIUM, "alice", "success"),
                (EventType.ACCESS_DENIED, Severity.HIGH, "charlie", "denied"),
                (EventType.SECURITY_VIOLATION, Severity.CRITICAL, "system", "detected"),
            ]
            
            for event_type, severity, user_id, result in events_to_create:
                await audit_service.log_event(
                    event_type, severity, user_id, "peer_001", 
                    "test_resource", "test_action", result
                )
            
            print(f"Created {len(events_to_create)} test events")
            
            # Query all events
            print("\nQuerying all events...")
            query = AuditQuery(limit=100)
            all_events = await audit_service.query_events(query)
            print(f"Total events found: {len(all_events)}")
            
            # Query by event type
            print("\nQuerying login events...")
            query = AuditQuery(event_types=[EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILURE])
            login_events = await audit_service.query_events(query)
            print(f"Login events found: {len(login_events)}")
            for event in login_events:
                print(f"  {event.timestamp.strftime('%H:%M:%S')} - {event.event_type.value} - {event.user_id} - {event.result}")
            
            # Query by severity
            print("\nQuerying high severity events...")
            query = AuditQuery(severities=[Severity.HIGH, Severity.CRITICAL])
            high_severity_events = await audit_service.query_events(query)
            print(f"High severity events found: {len(high_severity_events)}")
            for event in high_severity_events:
                print(f"  {event.timestamp.strftime('%H:%M:%S')} - {event.event_type.value} - {event.severity.value} - {event.user_id}")
            
            # Query by user
            print("\nQuerying events for user 'alice'...")
            query = AuditQuery(user_ids=["alice"])
            alice_events = await audit_service.query_events(query)
            print(f"Alice's events found: {len(alice_events)}")
            for event in alice_events:
                print(f"  {event.timestamp.strftime('%H:%M:%S')} - {event.event_type.value} - {event.action}")
            
            # Query by time range
            print("\nQuerying recent events (last 1 minute)...")
            start_time = datetime.utcnow() - timedelta(minutes=1)
            query = AuditQuery(start_time=start_time)
            recent_events = await audit_service.query_events(query)
            print(f"Recent events found: {len(recent_events)}")
            
        finally:
            await audit_service.stop()


async def demo_audit_reporting():
    """Demonstrate audit report generation."""
    print("\n=== Audit Reporting Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Creating events for reporting...")
            
            # Create events that will trigger security findings
            suspicious_events = [
                # Multiple failed logins
                (EventType.LOGIN_FAILURE, Severity.HIGH, "suspicious_user", "failure"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "suspicious_user", "failure"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "suspicious_user", "failure"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "suspicious_user", "failure"),
                (EventType.LOGIN_FAILURE, Severity.HIGH, "suspicious_user", "failure"),
                
                # Multiple access denials
                (EventType.ACCESS_DENIED, Severity.HIGH, "unauthorized_user", "denied"),
                (EventType.ACCESS_DENIED, Severity.HIGH, "unauthorized_user", "denied"),
                (EventType.ACCESS_DENIED, Severity.HIGH, "unauthorized_user", "denied"),
                
                # Critical security events
                (EventType.SECURITY_VIOLATION, Severity.CRITICAL, "system", "detected"),
                (EventType.INTRUSION_DETECTED, Severity.CRITICAL, "system", "detected"),
                
                # Normal user activity
                (EventType.LOGIN_SUCCESS, Severity.MEDIUM, "normal_user", "success"),
                (EventType.FILE_CREATED, Severity.LOW, "normal_user", "success"),
                (EventType.FILE_READ, Severity.LOW, "normal_user", "success"),
                (EventType.LOGOUT, Severity.LOW, "normal_user", "success"),
                
                # High activity user
                *[(EventType.FILE_READ, Severity.LOW, "power_user", "success") for _ in range(20)],
            ]
            
            for event_type, severity, user_id, result in suspicious_events:
                await audit_service.log_event(
                    event_type, severity, user_id, "peer_001",
                    "test_resource", "test_action", result
                )
            
            print(f"Created {len(suspicious_events)} events for analysis")
            
            # Generate comprehensive report
            print("\nGenerating audit report...")
            query = AuditQuery(limit=1000)
            report = await audit_service.generate_report(query)
            
            print(f"✓ Generated report: {report.report_id}")
            print(f"  Report generated at: {report.generated_at}")
            print(f"  Total events analyzed: {report.total_events}")
            
            # Display statistics
            print(f"\nReport Statistics:")
            stats = report.statistics
            print(f"  Time range: {stats.get('time_range', {}).get('start', 'N/A')} to {stats.get('time_range', {}).get('end', 'N/A')}")
            
            print(f"  Event types:")
            for event_type, count in stats.get('event_types', {}).items():
                print(f"    {event_type}: {count}")
            
            print(f"  Severities:")
            for severity, count in stats.get('severities', {}).items():
                print(f"    {severity}: {count}")
            
            print(f"  Top users:")
            for user, count in sorted(stats.get('users', {}).items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    {user}: {count} events")
            
            # Display security findings
            print(f"\nSecurity Findings ({len(report.findings)}):")
            for i, finding in enumerate(report.findings, 1):
                print(f"  {i}. {finding}")
            
            # Display recommendations
            print(f"\nSecurity Recommendations ({len(report.recommendations)}):")
            for i, recommendation in enumerate(report.recommendations, 1):
                print(f"  {i}. {recommendation}")
            
        finally:
            await audit_service.stop()


async def demo_event_handlers():
    """Demonstrate event handlers for real-time monitoring."""
    print("\n=== Event Handlers Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Setting up event handlers...")
            
            # Handler for security violations
            security_alerts = []
            def security_handler(event):
                security_alerts.append(event)
                print(f"🚨 SECURITY ALERT: {event.event_type.value} - {event.user_id} - {event.details}")
            
            # Handler for failed logins
            failed_logins = []
            def login_failure_handler(event):
                failed_logins.append(event)
                print(f"⚠️  LOGIN FAILURE: {event.user_id} from {event.source_ip}")
            
            # Handler for file operations
            file_operations = []
            def file_handler(event):
                file_operations.append(event)
                print(f"📁 FILE OPERATION: {event.action} on {event.resource} by {event.user_id}")
            
            # Register handlers
            audit_service.add_event_handler(EventType.SECURITY_VIOLATION, security_handler)
            audit_service.add_event_handler(EventType.LOGIN_FAILURE, login_failure_handler)
            audit_service.add_event_handler(EventType.FILE_CREATED, file_handler)
            audit_service.add_event_handler(EventType.FILE_DELETED, file_handler)
            
            print("Event handlers registered")
            
            # Generate events that will trigger handlers
            print("\nGenerating events to trigger handlers...")
            
            await audit_service.log_event(
                EventType.LOGIN_FAILURE, Severity.HIGH,
                "attacker", "peer_001", "auth", "login", "failure",
                source_ip="192.168.1.999"
            )
            
            await audit_service.log_event(
                EventType.FILE_CREATED, Severity.LOW,
                "user1", "peer_001", "/documents/secret.txt", "create", "success"
            )
            
            await audit_service.log_event(
                EventType.SECURITY_VIOLATION, Severity.CRITICAL,
                "system", "peer_001", "intrusion_detection", "detect", "violation",
                details={"type": "brute_force", "source": "192.168.1.999"}
            )
            
            await audit_service.log_event(
                EventType.FILE_DELETED, Severity.MEDIUM,
                "user1", "peer_001", "/documents/old_file.txt", "delete", "success"
            )
            
            # Wait a moment for handlers to process
            await asyncio.sleep(0.1)
            
            print(f"\nHandler results:")
            print(f"  Security alerts triggered: {len(security_alerts)}")
            print(f"  Failed login alerts: {len(failed_logins)}")
            print(f"  File operation alerts: {len(file_operations)}")
            
        finally:
            await audit_service.stop()


async def demo_security_monitoring():
    """Demonstrate automatic security monitoring."""
    print("\n=== Security Monitoring Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Simulating suspicious activity...")
            
            # Simulate brute force attack (multiple failed logins)
            print("Simulating brute force attack...")
            for i in range(6):  # Threshold is 5
                await audit_service.log_event(
                    EventType.LOGIN_FAILURE, Severity.MEDIUM,
                    "target_user", "attacker_peer", "auth", "login", "failure",
                    source_ip="192.168.1.999",
                    details={"attempt": i+1, "method": "password"}
                )
                await asyncio.sleep(0.01)  # Small delay between attempts
            
            print("✓ Brute force simulation complete")
            
            # Check for automatically generated security violations
            print("\nChecking for security violations...")
            query = AuditQuery(event_types=[EventType.SECURITY_VIOLATION])
            violations = await audit_service.query_events(query)
            
            print(f"Security violations detected: {len(violations)}")
            for violation in violations:
                print(f"  {violation.timestamp.strftime('%H:%M:%S')} - {violation.action} - {violation.user_id}")
                print(f"    Details: {violation.details}")
            
            # Simulate privilege escalation attempt
            print("\nSimulating privilege escalation attempt...")
            await audit_service.log_event(
                EventType.ACCESS_DENIED, Severity.HIGH,
                "regular_user", "peer_001", "/admin/system_config", "read", "denied",
                details={"required_permission": "admin", "user_permission": "user"}
            )
            
            # Check for new violations
            await asyncio.sleep(0.1)
            new_violations = await audit_service.query_events(query)
            
            if len(new_violations) > len(violations):
                print("✓ Additional security violation detected for privilege escalation")
                latest_violation = new_violations[-1]
                print(f"  Action: {latest_violation.action}")
                print(f"  Resource: {latest_violation.resource}")
            
        finally:
            await audit_service.stop()


async def demo_retention_policy():
    """Demonstrate audit log retention policies."""
    print("\n=== Retention Policy Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create custom retention policy
        policy = RetentionPolicy(
            max_age_days=30,
            max_size_mb=10,
            compress_after_days=7,
            archive_after_days=14,
            auto_delete=False,
            backup_before_delete=True
        )
        
        audit_service = AuditLoggingService(Path(temp_dir), policy)
        await audit_service.start()
        
        try:
            print("Retention policy configuration:")
            print(f"  Maximum age: {policy.max_age_days} days")
            print(f"  Maximum size: {policy.max_size_mb} MB")
            print(f"  Compress after: {policy.compress_after_days} days")
            print(f"  Archive after: {policy.archive_after_days} days")
            print(f"  Auto delete: {policy.auto_delete}")
            print(f"  Backup before delete: {policy.backup_before_delete}")
            
            # Log some events
            print(f"\nLogging events...")
            for i in range(10):
                await audit_service.log_event(
                    EventType.FILE_READ, Severity.LOW,
                    f"user_{i}", "peer_001", f"file_{i}.txt", "read", "success"
                )
            
            print(f"✓ Logged 10 events")
            print(f"Current log file size: {audit_service.current_log_file.stat().st_size} bytes")
            
            # Apply retention policy manually (normally done periodically)
            print(f"\nApplying retention policy...")
            await audit_service._apply_retention_policy()
            print(f"✓ Retention policy applied")
            
            # Check archive directory
            archive_files = list(audit_service.archive_dir.glob("*"))
            print(f"Archive files: {len(archive_files)}")
            for archive_file in archive_files:
                print(f"  {archive_file.name}")
            
        finally:
            await audit_service.stop()


async def demo_log_integrity():
    """Demonstrate log integrity verification."""
    print("\n=== Log Integrity Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        audit_service = AuditLoggingService(Path(temp_dir))
        await audit_service.start()
        
        try:
            print("Demonstrating log integrity features...")
            
            # Log some events
            await audit_service.log_event(
                EventType.LOGIN_SUCCESS, Severity.MEDIUM,
                "user1", "peer_001", "auth", "login", "success"
            )
            
            await audit_service.log_event(
                EventType.FILE_CREATED, Severity.LOW,
                "user1", "peer_001", "document.txt", "create", "success"
            )
            
            print("✓ Logged events with integrity protection")
            
            # Check signing keys
            if audit_service.signing_key:
                print("✓ Cryptographic signing keys available")
                print(f"  Private key file: {audit_service.private_key_file}")
                print(f"  Public key file: {audit_service.public_key_file}")
            else:
                print("⚠️  Cryptographic signing not available (requires cryptography library)")
            
            # Verify log integrity
            print("\nVerifying log integrity...")
            try:
                integrity_ok = await audit_service.verify_log_integrity()
                if integrity_ok:
                    print("✓ Log integrity verification passed")
                else:
                    print("✗ Log integrity verification failed")
            except Exception as e:
                print(f"⚠️  Integrity verification error: {e}")
            
            # Show hash chain information
            print(f"\nHash chain information:")
            print(f"  Current sequence number: {audit_service.sequence_number}")
            print(f"  Last hash: {audit_service.last_hash[:16]}..." if audit_service.last_hash else "  Last hash: None")
            
        finally:
            await audit_service.stop()


async def demo_persistence():
    """Demonstrate audit log persistence."""
    print("\n=== Persistence Demo ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir)
        
        # First service instance
        print("Creating first audit service instance...")
        service1 = AuditLoggingService(data_dir)
        await service1.start()
        
        # Log some events
        await service1.log_event(
            EventType.LOGIN_SUCCESS, Severity.MEDIUM,
            "persistent_user", "peer_001", "auth", "login", "success"
        )
        
        await service1.log_event(
            EventType.FILE_CREATED, Severity.LOW,
            "persistent_user", "peer_001", "persistent_file.txt", "create", "success"
        )
        
        original_sequence = service1.sequence_number
        print(f"First instance sequence number: {original_sequence}")
        
        await service1.stop()
        
        # Second service instance
        print("\nCreating second audit service instance...")
        service2 = AuditLoggingService(data_dir)
        await service2.start()
        
        try:
            print(f"Second instance sequence number: {service2.sequence_number}")
            print(f"✓ Sequence number preserved: {service2.sequence_number >= original_sequence}")
            
            # Query events from first instance
            query = AuditQuery(user_ids=["persistent_user"])
            events = await service2.query_events(query)
            
            persistent_events = [e for e in events if e.user_id == "persistent_user"]
            print(f"✓ Found {len(persistent_events)} events from first instance")
            
            for event in persistent_events:
                print(f"  {event.timestamp.strftime('%H:%M:%S')} - {event.event_type.value} - {event.resource}")
            
            # Log new event in second instance
            await service2.log_event(
                EventType.LOGOUT, Severity.LOW,
                "persistent_user", "peer_001", "auth", "logout", "success"
            )
            
            print(f"✓ Logged new event in second instance")
            print(f"Final sequence number: {service2.sequence_number}")
            
        finally:
            await service2.stop()


async def main():
    """Run all audit logging demos."""
    print("DittoFS Audit Logging System Demo")
    print("=" * 50)
    
    demos = [
        demo_basic_logging,
        demo_event_querying,
        demo_audit_reporting,
        demo_event_handlers,
        demo_security_monitoring,
        demo_retention_policy,
        demo_log_integrity,
        demo_persistence
    ]
    
    for demo in demos:
        try:
            await demo()
        except Exception as e:
            print(f"Demo failed: {e}")
        print()  # Add spacing between demos
    
    print("Audit logging demo completed!")


if __name__ == "__main__":
    asyncio.run(main())