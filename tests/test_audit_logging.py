"""
Tests for the audit logging system.
"""

import asyncio
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.dittofs.audit_logging import (
    AuditEvent,
    AuditLogEntry,
    AuditLogError,
    AuditLoggingService,
    AuditQuery,
    AuditReport,
    EventType,
    RetentionPolicy,
    Severity,
    SignatureVerificationError,
    TamperDetectedError,
)


@pytest.fixture
async def audit_service():
    """Create audit logging service for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AuditLoggingService(Path(temp_dir))
        await service.start()
        yield service
        await service.stop()


@pytest.fixture
async def audit_service_with_policy():
    """Create audit logging service with custom retention policy."""
    with tempfile.TemporaryDirectory() as temp_dir:
        policy = RetentionPolicy(
            max_age_days=30, max_size_mb=10, compress_after_days=7, auto_delete=True
        )
        service = AuditLoggingService(Path(temp_dir), policy)
        await service.start()
        yield service
        await service.stop()


class TestAuditLoggingService:
    """Test audit logging service functionality."""

    async def test_service_initialization(self, audit_service):
        """Test service initialization."""
        assert audit_service.data_dir.exists()
        assert audit_service.sequence_number >= 0
        assert isinstance(audit_service.retention_policy, RetentionPolicy)

    async def test_log_event(self, audit_service):
        """Test logging audit events."""
        event_id = await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            user_id="test_user",
            peer_id="test_peer",
            resource="authentication",
            action="login",
            result="success",
            details={"method": "password"},
            source_ip="192.168.1.100",
            session_id="session_123",
        )

        assert len(event_id) > 0
        assert audit_service.current_log_file.exists()

        # Verify log file contains the event
        log_content = audit_service.current_log_file.read_text()
        assert "LOGIN_SUCCESS" in log_content
        assert "test_user" in log_content
        assert "session_123" in log_content

    async def test_log_multiple_events(self, audit_service):
        """Test logging multiple events."""
        events = [
            (EventType.LOGIN_SUCCESS, "user1", "login", "success"),
            (EventType.FILE_CREATED, "user1", "create", "success"),
            (EventType.FILE_READ, "user2", "read", "success"),
            (EventType.LOGIN_FAILURE, "user3", "login", "failure"),
            (EventType.ACCESS_DENIED, "user3", "access", "denied"),
        ]

        event_ids = []
        for event_type, user_id, action, result in events:
            event_id = await audit_service.log_event(
                event_type,
                Severity.MEDIUM,
                user_id=user_id,
                peer_id="test_peer",
                resource="test_resource",
                action=action,
                result=result,
            )
            event_ids.append(event_id)

        assert len(event_ids) == len(events)
        assert len(set(event_ids)) == len(events)  # All unique

        # Check sequence numbers are incrementing
        assert (
            audit_service.sequence_number == len(events) + 1
        )  # +1 for service start event

    async def test_query_events_basic(self, audit_service):
        """Test basic event querying."""
        # Log some test events
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "auth",
            "login",
            "success",
        )
        await audit_service.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )
        await audit_service.log_event(
            EventType.LOGIN_FAILURE,
            Severity.HIGH,
            "user2",
            "peer2",
            "auth",
            "login",
            "failure",
        )

        # Query all events
        query = AuditQuery(limit=100)
        events = await audit_service.query_events(query)

        # Should include service start event + our 3 events
        assert len(events) >= 3

        # Check event types
        event_types = [e.event_type for e in events]
        assert EventType.LOGIN_SUCCESS in event_types
        assert EventType.FILE_CREATED in event_types
        assert EventType.LOGIN_FAILURE in event_types

    async def test_query_events_filtered(self, audit_service):
        """Test filtered event querying."""
        # Log test events
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "auth",
            "login",
            "success",
        )
        await audit_service.log_event(
            EventType.LOGIN_FAILURE,
            Severity.HIGH,
            "user2",
            "peer1",
            "auth",
            "login",
            "failure",
        )
        await audit_service.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )

        # Query by event type
        query = AuditQuery(
            event_types=[EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILURE]
        )
        events = await audit_service.query_events(query)

        login_events = [
            e
            for e in events
            if e.event_type in [EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILURE]
        ]
        assert len(login_events) == 2

        # Query by user
        query = AuditQuery(user_ids=["user1"])
        events = await audit_service.query_events(query)

        user1_events = [e for e in events if e.user_id == "user1"]
        assert len(user1_events) >= 2  # LOGIN_SUCCESS and FILE_CREATED

        # Query by severity
        query = AuditQuery(severities=[Severity.HIGH])
        events = await audit_service.query_events(query)

        high_severity_events = [e for e in events if e.severity == Severity.HIGH]
        assert len(high_severity_events) >= 1  # LOGIN_FAILURE

    async def test_query_events_time_range(self, audit_service):
        """Test time range querying."""
        start_time = datetime.utcnow()

        # Log an event
        await audit_service.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )

        # Wait a bit
        await asyncio.sleep(0.1)

        end_time = datetime.utcnow()

        # Log another event
        await audit_service.log_event(
            EventType.FILE_DELETED,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "file.txt",
            "delete",
            "success",
        )

        # Query events in time range
        query = AuditQuery(start_time=start_time, end_time=end_time)
        events = await audit_service.query_events(query)

        # Should include the FILE_CREATED event but not FILE_DELETED
        file_created_events = [
            e for e in events if e.event_type == EventType.FILE_CREATED
        ]
        file_deleted_events = [
            e for e in events if e.event_type == EventType.FILE_DELETED
        ]

        assert len(file_created_events) >= 1
        assert len(file_deleted_events) == 0

    async def test_generate_report(self, audit_service):
        """Test audit report generation."""
        # Log various events
        events_to_log = [
            (EventType.LOGIN_SUCCESS, Severity.MEDIUM, "user1", "success"),
            (EventType.LOGIN_SUCCESS, Severity.MEDIUM, "user2", "success"),
            (EventType.LOGIN_FAILURE, Severity.HIGH, "user3", "failure"),
            (EventType.ACCESS_DENIED, Severity.HIGH, "user3", "denied"),
            (EventType.FILE_CREATED, Severity.LOW, "user1", "success"),
            (EventType.FILE_DELETED, Severity.MEDIUM, "user2", "success"),
        ]

        for event_type, severity, user_id, result in events_to_log:
            await audit_service.log_event(
                event_type, severity, user_id, "peer1", "resource", "action", result
            )

        # Generate report
        query = AuditQuery(limit=100)
        report = await audit_service.generate_report(query)

        assert len(report.report_id) > 0
        assert report.total_events >= len(events_to_log)
        assert isinstance(report.statistics, dict)
        assert isinstance(report.findings, list)
        assert isinstance(report.recommendations, list)

        # Check statistics
        stats = report.statistics
        assert "total_events" in stats
        assert "event_types" in stats
        assert "severities" in stats
        assert "users" in stats

        # Should have findings due to failed logins and access denials
        assert len(report.findings) > 0
        assert len(report.recommendations) > 0

    async def test_event_handlers(self, audit_service):
        """Test event handlers."""
        handled_events = []

        def test_handler(event: AuditEvent):
            handled_events.append(event)

        # Add handler for login events
        audit_service.add_event_handler(EventType.LOGIN_SUCCESS, test_handler)

        # Log events
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "auth",
            "login",
            "success",
        )
        await audit_service.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )

        # Only LOGIN_SUCCESS should trigger handler
        assert len(handled_events) == 1
        assert handled_events[0].event_type == EventType.LOGIN_SUCCESS
        assert handled_events[0].user_id == "user1"

        # Remove handler
        audit_service.remove_event_handler(EventType.LOGIN_SUCCESS, test_handler)

        # Log another login event
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user2",
            "peer1",
            "auth",
            "login",
            "success",
        )

        # Handler should not be called
        assert len(handled_events) == 1

    async def test_security_violation_detection(self, audit_service):
        """Test automatic security violation detection."""
        # Log multiple failed logins for same user
        for i in range(6):  # Threshold is 5
            await audit_service.log_event(
                EventType.LOGIN_FAILURE,
                Severity.MEDIUM,
                "suspicious_user",
                "peer1",
                "auth",
                "login",
                "failure",
            )

        # Query for security violations
        query = AuditQuery(event_types=[EventType.SECURITY_VIOLATION])
        events = await audit_service.query_events(query)

        # Should have detected security violation
        violation_events = [
            e for e in events if e.event_type == EventType.SECURITY_VIOLATION
        ]
        assert len(violation_events) >= 1

        violation_event = violation_events[0]
        assert violation_event.user_id == "suspicious_user"
        assert violation_event.action == "repeated_failures"

    async def test_retention_policy(self, audit_service_with_policy):
        """Test retention policy application."""
        policy = audit_service_with_policy.retention_policy

        assert policy.max_age_days == 30
        assert policy.max_size_mb == 10
        assert policy.compress_after_days == 7
        assert policy.auto_delete is True

        # Log an event
        await audit_service_with_policy.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )

        # Apply retention policy (normally done periodically)
        await audit_service_with_policy._apply_retention_policy()

        # Should not affect recent logs
        assert audit_service_with_policy.current_log_file.exists()

    async def test_log_integrity_verification(self, audit_service):
        """Test log integrity verification."""
        # Log some events
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "auth",
            "login",
            "success",
        )
        await audit_service.log_event(
            EventType.FILE_CREATED,
            Severity.LOW,
            "user1",
            "peer1",
            "file.txt",
            "create",
            "success",
        )

        # Verify integrity
        result = await audit_service.verify_log_integrity()
        assert result is True  # Should pass (or return True if crypto not available)

    async def test_persistence(self, audit_service):
        """Test data persistence across service restarts."""
        # Log events
        await audit_service.log_event(
            EventType.LOGIN_SUCCESS,
            Severity.MEDIUM,
            "user1",
            "peer1",
            "auth",
            "login",
            "success",
        )

        original_sequence = audit_service.sequence_number

        # Stop and restart service
        data_dir = audit_service.data_dir
        await audit_service.stop()

        new_service = AuditLoggingService(data_dir)
        await new_service.start()

        try:
            # Should resume from where it left off
            assert new_service.sequence_number >= original_sequence

            # Should be able to query old events
            query = AuditQuery(user_ids=["user1"])
            events = await new_service.query_events(query)

            user1_events = [e for e in events if e.user_id == "user1"]
            assert len(user1_events) >= 1

        finally:
            await new_service.stop()


class TestAuditEvent:
    """Test AuditEvent class functionality."""

    def test_event_creation(self):
        """Test audit event creation."""
        event = AuditEvent(
            event_id="test_id",
            timestamp=datetime.utcnow(),
            event_type=EventType.LOGIN_SUCCESS,
            severity=Severity.MEDIUM,
            user_id="test_user",
            peer_id="test_peer",
            resource="test_resource",
            action="test_action",
            result="success",
            details={"key": "value"},
            source_ip="192.168.1.1",
            session_id="session_123",
        )

        assert event.event_id == "test_id"
        assert event.event_type == EventType.LOGIN_SUCCESS
        assert event.severity == Severity.MEDIUM
        assert event.user_id == "test_user"
        assert event.details["key"] == "value"

    def test_event_serialization(self):
        """Test event serialization."""
        event = AuditEvent(
            event_id="test_id",
            timestamp=datetime.utcnow(),
            event_type=EventType.FILE_CREATED,
            severity=Severity.LOW,
            user_id="test_user",
            peer_id="test_peer",
            resource="file.txt",
            action="create",
            result="success",
        )

        # Test to_dict
        event_dict = event.to_dict()
        assert event_dict["event_id"] == "test_id"
        assert event_dict["event_type"] == "file_created"
        assert event_dict["severity"] == "low"
        assert event_dict["user_id"] == "test_user"

        # Test from_dict
        restored_event = AuditEvent.from_dict(event_dict)
        assert restored_event.event_id == event.event_id
        assert restored_event.event_type == event.event_type
        assert restored_event.severity == event.severity
        assert restored_event.user_id == event.user_id

    def test_signature_data(self):
        """Test signature data generation."""
        event = AuditEvent(
            event_id="test_id",
            timestamp=datetime.utcnow(),
            event_type=EventType.LOGIN_SUCCESS,
            severity=Severity.MEDIUM,
            user_id="test_user",
            peer_id="test_peer",
            resource="auth",
            action="login",
            result="success",
        )

        sig_data = event.get_signature_data()
        assert isinstance(sig_data, str)
        assert "test_id" in sig_data
        assert "test_user" in sig_data
        assert "login" in sig_data


class TestAuditQuery:
    """Test AuditQuery class functionality."""

    def test_query_creation(self):
        """Test audit query creation."""
        start_time = datetime.utcnow() - timedelta(hours=1)
        end_time = datetime.utcnow()

        query = AuditQuery(
            start_time=start_time,
            end_time=end_time,
            event_types=[EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILURE],
            severities=[Severity.HIGH],
            user_ids=["user1", "user2"],
            limit=50,
            offset=10,
        )

        assert query.start_time == start_time
        assert query.end_time == end_time
        assert EventType.LOGIN_SUCCESS in query.event_types
        assert Severity.HIGH in query.severities
        assert "user1" in query.user_ids
        assert query.limit == 50
        assert query.offset == 10


class TestRetentionPolicy:
    """Test RetentionPolicy class functionality."""

    def test_policy_creation(self):
        """Test retention policy creation."""
        policy = RetentionPolicy(
            max_age_days=90,
            max_size_mb=500,
            compress_after_days=14,
            archive_after_days=30,
            auto_delete=True,
            backup_before_delete=False,
        )

        assert policy.max_age_days == 90
        assert policy.max_size_mb == 500
        assert policy.compress_after_days == 14
        assert policy.archive_after_days == 30
        assert policy.auto_delete is True
        assert policy.backup_before_delete is False

    def test_default_policy(self):
        """Test default retention policy."""
        policy = RetentionPolicy()

        assert policy.max_age_days == 365
        assert policy.max_size_mb == 1000
        assert policy.compress_after_days == 30
        assert policy.archive_after_days == 90
        assert policy.auto_delete is False
        assert policy.backup_before_delete is True


class TestAuditLogEntry:
    """Test AuditLogEntry class functionality."""

    def test_log_entry_creation(self):
        """Test audit log entry creation."""
        event = AuditEvent(
            event_id="test_id",
            timestamp=datetime.utcnow(),
            event_type=EventType.FILE_CREATED,
            severity=Severity.LOW,
            user_id="test_user",
            peer_id="test_peer",
            resource="file.txt",
            action="create",
            result="success",
        )

        log_entry = AuditLogEntry(
            event=event,
            signature="test_signature",
            hash_chain="test_hash",
            sequence_number=123,
        )

        assert log_entry.event == event
        assert log_entry.signature == "test_signature"
        assert log_entry.hash_chain == "test_hash"
        assert log_entry.sequence_number == 123

    def test_log_entry_serialization(self):
        """Test log entry serialization."""
        event = AuditEvent(
            event_id="test_id",
            timestamp=datetime.utcnow(),
            event_type=EventType.FILE_CREATED,
            severity=Severity.LOW,
            user_id="test_user",
            peer_id="test_peer",
            resource="file.txt",
            action="create",
            result="success",
        )

        log_entry = AuditLogEntry(
            event=event,
            signature="test_signature",
            hash_chain="test_hash",
            sequence_number=123,
        )

        # Test to_dict
        entry_dict = log_entry.to_dict()
        assert "event" in entry_dict
        assert entry_dict["signature"] == "test_signature"
        assert entry_dict["sequence_number"] == 123

        # Test from_dict
        restored_entry = AuditLogEntry.from_dict(entry_dict)
        assert restored_entry.signature == log_entry.signature
        assert restored_entry.hash_chain == log_entry.hash_chain
        assert restored_entry.sequence_number == log_entry.sequence_number
        assert restored_entry.event.event_id == event.event_id


if __name__ == "__main__":
    pytest.main([__file__])
