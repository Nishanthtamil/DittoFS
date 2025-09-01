"""
Comprehensive audit logging system for DittoFS.

This module provides comprehensive audit logging including:
- Detailed logging of all security-relevant events
- Tamper-evident audit logs with cryptographic signatures
- Audit log analysis and reporting tools
- Audit log retention and archival policies
"""

import asyncio
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable
import logging
import gzip
import shutil

try:
    import cryptography
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
    CRYPTO_AVAILABLE = True
except ImportError:
    cryptography = None
    rsa = None
    padding = None
    hashes = None
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of audit events."""
    # Authentication events
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    TOKEN_CREATED = "token_created"
    TOKEN_REFRESHED = "token_refreshed"
    TOKEN_REVOKED = "token_revoked"
    
    # Authorization events
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    PERMISSION_CHANGED = "permission_changed"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REVOKED = "role_revoked"
    
    # File operations
    FILE_CREATED = "file_created"
    FILE_READ = "file_read"
    FILE_MODIFIED = "file_modified"
    FILE_DELETED = "file_deleted"
    FILE_MOVED = "file_moved"
    FILE_SHARED = "file_shared"
    
    # System events
    SERVICE_STARTED = "service_started"
    SERVICE_STOPPED = "service_stopped"
    CONFIGURATION_CHANGED = "configuration_changed"
    BACKUP_CREATED = "backup_created"
    BACKUP_RESTORED = "backup_restored"
    
    # Security events
    CERTIFICATE_CREATED = "certificate_created"
    CERTIFICATE_REVOKED = "certificate_revoked"
    ENCRYPTION_KEY_GENERATED = "encryption_key_generated"
    SECURITY_VIOLATION = "security_violation"
    INTRUSION_DETECTED = "intrusion_detected"
    
    # Peer events
    PEER_CONNECTED = "peer_connected"
    PEER_DISCONNECTED = "peer_disconnected"
    PEER_AUTHENTICATED = "peer_authenticated"
    PEER_TRUST_CHANGED = "peer_trust_changed"
    
    # Data integrity events
    CORRUPTION_DETECTED = "corruption_detected"
    INTEGRITY_CHECK_PASSED = "integrity_check_passed"
    INTEGRITY_CHECK_FAILED = "integrity_check_failed"
    DATA_RECOVERED = "data_recovered"


class Severity(Enum):
    """Event severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Individual audit event."""
    event_id: str
    timestamp: datetime
    event_type: EventType
    severity: Severity
    user_id: str
    peer_id: str
    resource: str
    action: str
    result: str
    details: Dict[str, Any] = field(default_factory=dict)
    source_ip: str = ""
    user_agent: str = ""
    session_id: str = ""
    correlation_id: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['event_type'] = self.event_type.value
        data['severity'] = self.severity.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuditEvent':
        """Create from dictionary."""
        data = data.copy()
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['event_type'] = EventType(data['event_type'])
        data['severity'] = Severity(data['severity'])
        return cls(**data)
    
    def get_signature_data(self) -> str:
        """Get data for cryptographic signature."""
        # Create deterministic string for signing
        sig_data = {
            'event_id': self.event_id,
            'timestamp': self.timestamp.isoformat(),
            'event_type': self.event_type.value,
            'user_id': self.user_id,
            'resource': self.resource,
            'action': self.action,
            'result': self.result
        }
        return json.dumps(sig_data, sort_keys=True)


@dataclass
class AuditLogEntry:
    """Signed audit log entry."""
    event: AuditEvent
    signature: str = ""
    hash_chain: str = ""
    sequence_number: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'event': self.event.to_dict(),
            'signature': self.signature,
            'hash_chain': self.hash_chain,
            'sequence_number': self.sequence_number
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuditLogEntry':
        """Create from dictionary."""
        return cls(
            event=AuditEvent.from_dict(data['event']),
            signature=data.get('signature', ''),
            hash_chain=data.get('hash_chain', ''),
            sequence_number=data.get('sequence_number', 0)
        )


@dataclass
class RetentionPolicy:
    """Audit log retention policy."""
    max_age_days: int = 365
    max_size_mb: int = 1000
    compress_after_days: int = 30
    archive_after_days: int = 90
    auto_delete: bool = False
    backup_before_delete: bool = True


@dataclass
class AuditQuery:
    """Query parameters for audit log search."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    event_types: Optional[List[EventType]] = None
    severities: Optional[List[Severity]] = None
    user_ids: Optional[List[str]] = None
    peer_ids: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    actions: Optional[List[str]] = None
    results: Optional[List[str]] = None
    limit: int = 1000
    offset: int = 0


@dataclass
class AuditReport:
    """Audit report with statistics and findings."""
    report_id: str
    generated_at: datetime
    query: AuditQuery
    total_events: int
    events: List[AuditEvent]
    statistics: Dict[str, Any] = field(default_factory=dict)
    findings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class AuditLogError(Exception):
    """Base exception for audit logging errors."""
    pass


class SignatureVerificationError(AuditLogError):
    """Raised when signature verification fails."""
    pass


class TamperDetectedError(AuditLogError):
    """Raised when log tampering is detected."""
    pass


class AuditLoggingService:
    """
    Comprehensive audit logging service.
    
    Provides tamper-evident logging with cryptographic signatures and analysis.
    """
    
    def __init__(self, data_dir: Path, retention_policy: Optional[RetentionPolicy] = None):
        """
        Initialize audit logging service.
        
        Args:
            data_dir: Directory for storing audit logs
            retention_policy: Log retention policy
        """
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.retention_policy = retention_policy or RetentionPolicy()
        
        # Storage files
        self.current_log_file = data_dir / "audit.log"
        self.archive_dir = data_dir / "archive"
        self.archive_dir.mkdir(exist_ok=True)
        
        # Signing key files
        self.private_key_file = data_dir / "audit_signing.key"
        self.public_key_file = data_dir / "audit_signing.pub"
        
        # In-memory state
        self.signing_key = None
        self.verification_key = None
        self.sequence_number = 0
        self.last_hash = ""
        
        # Event handlers
        self.event_handlers: Dict[EventType, List[Callable]] = {}
        
        # Initialize signing keys
        self._initialize_signing_keys()
        
        # Load current state
        self._load_current_state()
        
        # Cleanup task
        self._cleanup_task = None
    
    async def start(self):
        """Start the audit logging service."""
        # Log service start
        await self.log_event(
            EventType.SERVICE_STARTED,
            Severity.MEDIUM,
            user_id="system",
            peer_id="local",
            resource="audit_service",
            action="start",
            result="success",
            details={"version": "1.0", "crypto_available": CRYPTO_AVAILABLE}
        )
        
        # Start periodic cleanup
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        
        logger.info("Audit logging service started")
    
    async def stop(self):
        """Stop the audit logging service."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Log service stop
        await self.log_event(
            EventType.SERVICE_STOPPED,
            Severity.MEDIUM,
            user_id="system",
            peer_id="local",
            resource="audit_service",
            action="stop",
            result="success"
        )
        
        logger.info("Audit logging service stopped")
    
    async def log_event(self, event_type: EventType, severity: Severity,
                       user_id: str, peer_id: str, resource: str,
                       action: str, result: str,
                       details: Optional[Dict[str, Any]] = None,
                       source_ip: str = "", user_agent: str = "",
                       session_id: str = "", correlation_id: str = "") -> str:
        """
        Log an audit event.
        
        Args:
            event_type: Type of event
            severity: Event severity
            user_id: User identifier
            peer_id: Peer identifier
            resource: Resource being accessed
            action: Action being performed
            result: Result of the action
            details: Additional event details
            source_ip: Source IP address
            user_agent: User agent string
            session_id: Session identifier
            correlation_id: Correlation identifier
            
        Returns:
            Event ID
        """
        # Create event
        event_id = secrets.token_urlsafe(16)
        event = AuditEvent(
            event_id=event_id,
            timestamp=datetime.utcnow(),
            event_type=event_type,
            severity=severity,
            user_id=user_id,
            peer_id=peer_id,
            resource=resource,
            action=action,
            result=result,
            details=details or {},
            source_ip=source_ip,
            user_agent=user_agent,
            session_id=session_id,
            correlation_id=correlation_id
        )
        
        # Create signed log entry
        log_entry = await self._create_signed_entry(event)
        
        # Write to log file
        await self._write_log_entry(log_entry)
        
        # Trigger event handlers
        await self._trigger_event_handlers(event)
        
        # Check for security violations
        await self._check_security_violations(event)
        
        logger.debug(f"Logged audit event: {event_type.value} for {user_id}")
        return event_id
    
    async def query_events(self, query: AuditQuery) -> List[AuditEvent]:
        """
        Query audit events.
        
        Args:
            query: Query parameters
            
        Returns:
            List of matching events
        """
        events = []
        
        # Read from current log file
        if self.current_log_file.exists():
            events.extend(await self._read_log_file(self.current_log_file, query))
        
        # Read from archived log files if needed
        if query.start_time:
            archive_files = self._get_archive_files_for_period(
                query.start_time, query.end_time or datetime.utcnow()
            )
            for archive_file in archive_files:
                events.extend(await self._read_log_file(archive_file, query))
        
        # Apply filters and sorting
        filtered_events = self._filter_events(events, query)
        filtered_events.sort(key=lambda e: e.timestamp, reverse=True)
        
        # Apply limit and offset
        start_idx = query.offset
        end_idx = start_idx + query.limit
        
        return filtered_events[start_idx:end_idx]
    
    async def generate_report(self, query: AuditQuery) -> AuditReport:
        """
        Generate audit report.
        
        Args:
            query: Query parameters
            
        Returns:
            Audit report with statistics and analysis
        """
        events = await self.query_events(query)
        
        report = AuditReport(
            report_id=secrets.token_urlsafe(16),
            generated_at=datetime.utcnow(),
            query=query,
            total_events=len(events),
            events=events
        )
        
        # Generate statistics
        report.statistics = self._generate_statistics(events)
        
        # Generate findings and recommendations
        report.findings = self._analyze_security_findings(events)
        report.recommendations = self._generate_recommendations(events, report.findings)
        
        return report
    
    async def verify_log_integrity(self, start_time: Optional[datetime] = None,
                                 end_time: Optional[datetime] = None) -> bool:
        """
        Verify audit log integrity.
        
        Args:
            start_time: Start time for verification
            end_time: End time for verification
            
        Returns:
            True if logs are intact
            
        Raises:
            TamperDetectedError: If tampering is detected
        """
        if not CRYPTO_AVAILABLE:
            logger.warning("Cannot verify log integrity without cryptography libraries")
            return True
        
        # Get log files to verify
        log_files = [self.current_log_file]
        if start_time:
            archive_files = self._get_archive_files_for_period(
                start_time, end_time or datetime.utcnow()
            )
            log_files.extend(archive_files)
        
        # Verify each log file
        for log_file in log_files:
            if log_file.exists():
                await self._verify_log_file_integrity(log_file)
        
        logger.info("Audit log integrity verification completed successfully")
        return True
    
    def add_event_handler(self, event_type: EventType, handler: Callable[[AuditEvent], None]):
        """
        Add event handler for specific event type.
        
        Args:
            event_type: Event type to handle
            handler: Handler function
        """
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
    
    def remove_event_handler(self, event_type: EventType, handler: Callable[[AuditEvent], None]):
        """
        Remove event handler.
        
        Args:
            event_type: Event type
            handler: Handler function to remove
        """
        if event_type in self.event_handlers:
            try:
                self.event_handlers[event_type].remove(handler)
            except ValueError:
                pass
    
    async def _create_signed_entry(self, event: AuditEvent) -> AuditLogEntry:
        """Create signed audit log entry."""
        self.sequence_number += 1
        
        # Create hash chain
        event_data = event.get_signature_data()
        current_hash = hashlib.sha256(
            (self.last_hash + event_data).encode()
        ).hexdigest()
        
        # Create signature
        signature = ""
        if CRYPTO_AVAILABLE and self.signing_key:
            try:
                signature_bytes = self.signing_key.sign(
                    event_data.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                signature = signature_bytes.hex()
            except Exception as e:
                logger.error(f"Failed to sign audit event: {e}")
        
        log_entry = AuditLogEntry(
            event=event,
            signature=signature,
            hash_chain=current_hash,
            sequence_number=self.sequence_number
        )
        
        self.last_hash = current_hash
        return log_entry
    
    async def _write_log_entry(self, log_entry: AuditLogEntry):
        """Write log entry to file."""
        try:
            log_line = json.dumps(log_entry.to_dict()) + "\n"
            
            # Append to current log file
            with open(self.current_log_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
            
            # Check if log rotation is needed
            await self._check_log_rotation()
            
        except Exception as e:
            logger.error(f"Failed to write audit log entry: {e}")
            raise AuditLogError(f"Failed to write audit log: {e}")
    
    async def _read_log_file(self, log_file: Path, query: AuditQuery) -> List[AuditEvent]:
        """Read events from log file."""
        events = []
        
        try:
            # Handle compressed files
            if log_file.suffix == '.gz':
                with gzip.open(log_file, 'rt', encoding='utf-8') as f:
                    lines = f.readlines()
            else:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    entry_data = json.loads(line)
                    log_entry = AuditLogEntry.from_dict(entry_data)
                    
                    # Basic time filtering
                    if query.start_time and log_entry.event.timestamp < query.start_time:
                        continue
                    if query.end_time and log_entry.event.timestamp > query.end_time:
                        continue
                    
                    events.append(log_entry.event)
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in audit log: {line[:100]}...")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to read audit log file {log_file}: {e}")
        
        return events
    
    def _filter_events(self, events: List[AuditEvent], query: AuditQuery) -> List[AuditEvent]:
        """Filter events based on query parameters."""
        filtered = events
        
        if query.event_types:
            filtered = [e for e in filtered if e.event_type in query.event_types]
        
        if query.severities:
            filtered = [e for e in filtered if e.severity in query.severities]
        
        if query.user_ids:
            filtered = [e for e in filtered if e.user_id in query.user_ids]
        
        if query.peer_ids:
            filtered = [e for e in filtered if e.peer_id in query.peer_ids]
        
        if query.resources:
            filtered = [e for e in filtered if any(r in e.resource for r in query.resources)]
        
        if query.actions:
            filtered = [e for e in filtered if e.action in query.actions]
        
        if query.results:
            filtered = [e for e in filtered if e.result in query.results]
        
        return filtered
    
    def _generate_statistics(self, events: List[AuditEvent]) -> Dict[str, Any]:
        """Generate statistics from events."""
        if not events:
            return {}
        
        stats = {
            'total_events': len(events),
            'time_range': {
                'start': min(e.timestamp for e in events).isoformat(),
                'end': max(e.timestamp for e in events).isoformat()
            },
            'event_types': {},
            'severities': {},
            'users': {},
            'peers': {},
            'results': {},
            'hourly_distribution': {}
        }
        
        # Count by categories
        for event in events:
            # Event types
            event_type = event.event_type.value
            stats['event_types'][event_type] = stats['event_types'].get(event_type, 0) + 1
            
            # Severities
            severity = event.severity.value
            stats['severities'][severity] = stats['severities'].get(severity, 0) + 1
            
            # Users
            stats['users'][event.user_id] = stats['users'].get(event.user_id, 0) + 1
            
            # Peers
            stats['peers'][event.peer_id] = stats['peers'].get(event.peer_id, 0) + 1
            
            # Results
            stats['results'][event.result] = stats['results'].get(event.result, 0) + 1
            
            # Hourly distribution
            hour = event.timestamp.strftime('%Y-%m-%d %H:00')
            stats['hourly_distribution'][hour] = stats['hourly_distribution'].get(hour, 0) + 1
        
        return stats
    
    def _analyze_security_findings(self, events: List[AuditEvent]) -> List[str]:
        """Analyze events for security findings."""
        findings = []
        
        # Count failed login attempts
        failed_logins = [e for e in events if e.event_type == EventType.LOGIN_FAILURE]
        if len(failed_logins) > 10:
            findings.append(f"High number of failed login attempts: {len(failed_logins)}")
        
        # Check for access denials
        access_denied = [e for e in events if e.event_type == EventType.ACCESS_DENIED]
        if len(access_denied) > 50:
            findings.append(f"High number of access denials: {len(access_denied)}")
        
        # Check for critical events
        critical_events = [e for e in events if e.severity == Severity.CRITICAL]
        if critical_events:
            findings.append(f"Critical security events detected: {len(critical_events)}")
        
        # Check for unusual activity patterns
        user_activity = {}
        for event in events:
            user_activity[event.user_id] = user_activity.get(event.user_id, 0) + 1
        
        avg_activity = sum(user_activity.values()) / len(user_activity) if user_activity else 0
        for user, count in user_activity.items():
            if count > avg_activity * 3:  # 3x average activity
                findings.append(f"Unusual activity level for user {user}: {count} events")
        
        return findings
    
    def _generate_recommendations(self, events: List[AuditEvent], findings: List[str]) -> List[str]:
        """Generate security recommendations."""
        recommendations = []
        
        if any("failed login" in f for f in findings):
            recommendations.append("Consider implementing account lockout policies")
            recommendations.append("Review and strengthen password policies")
        
        if any("access denied" in f for f in findings):
            recommendations.append("Review access control policies and user permissions")
            recommendations.append("Provide additional user training on proper access procedures")
        
        if any("critical" in f for f in findings):
            recommendations.append("Investigate critical security events immediately")
            recommendations.append("Review and update incident response procedures")
        
        if any("unusual activity" in f for f in findings):
            recommendations.append("Investigate unusual user activity patterns")
            recommendations.append("Consider implementing behavioral analysis tools")
        
        # General recommendations
        recommendations.append("Regularly review audit logs for security anomalies")
        recommendations.append("Ensure audit log retention policies meet compliance requirements")
        
        return recommendations
    
    async def _trigger_event_handlers(self, event: AuditEvent):
        """Trigger registered event handlers."""
        handlers = self.event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Event handler failed: {e}")
    
    async def _check_security_violations(self, event: AuditEvent):
        """Check for immediate security violations."""
        # Check for repeated failed logins
        if event.event_type == EventType.LOGIN_FAILURE:
            recent_failures = await self._count_recent_events(
                EventType.LOGIN_FAILURE,
                user_id=event.user_id,
                minutes=15
            )
            if recent_failures >= 5:
                await self.log_event(
                    EventType.SECURITY_VIOLATION,
                    Severity.HIGH,
                    user_id=event.user_id,
                    peer_id=event.peer_id,
                    resource="authentication",
                    action="repeated_failures",
                    result="violation_detected",
                    details={"failure_count": recent_failures}
                )
        
        # Check for privilege escalation attempts
        if event.event_type == EventType.ACCESS_DENIED and event.severity == Severity.HIGH:
            await self.log_event(
                EventType.SECURITY_VIOLATION,
                Severity.MEDIUM,
                user_id=event.user_id,
                peer_id=event.peer_id,
                resource=event.resource,
                action="privilege_escalation_attempt",
                result="blocked",
                details={"original_event": event.event_id}
            )
    
    async def _count_recent_events(self, event_type: EventType, user_id: str = "",
                                 peer_id: str = "", minutes: int = 60) -> int:
        """Count recent events of specific type."""
        start_time = datetime.utcnow() - timedelta(minutes=minutes)
        query = AuditQuery(
            start_time=start_time,
            event_types=[event_type],
            user_ids=[user_id] if user_id else None,
            peer_ids=[peer_id] if peer_id else None,
            limit=1000
        )
        
        events = await self.query_events(query)
        return len(events)
    
    def _initialize_signing_keys(self):
        """Initialize cryptographic signing keys."""
        if not CRYPTO_AVAILABLE:
            logger.warning("Cryptography not available - audit logs will not be signed")
            return
        
        try:
            # Load existing keys
            if self.private_key_file.exists() and self.public_key_file.exists():
                with open(self.private_key_file, 'rb') as f:
                    private_key_data = f.read()
                    self.signing_key = serialization.load_pem_private_key(
                        private_key_data, password=None
                    )
                
                with open(self.public_key_file, 'rb') as f:
                    public_key_data = f.read()
                    self.verification_key = serialization.load_pem_public_key(public_key_data)
                
                logger.info("Loaded existing audit signing keys")
            else:
                # Generate new keys
                self.signing_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048
                )
                self.verification_key = self.signing_key.public_key()
                
                # Save keys
                private_key_data = self.signing_key.private_bytes(
                    Encoding.PEM,
                    PrivateFormat.PKCS8,
                    NoEncryption()
                )
                public_key_data = self.verification_key.public_bytes(
                    Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo
                )
                
                self.private_key_file.write_bytes(private_key_data)
                self.public_key_file.write_bytes(public_key_data)
                
                # Set restrictive permissions
                self.private_key_file.chmod(0o600)
                self.public_key_file.chmod(0o644)
                
                logger.info("Generated new audit signing keys")
                
        except Exception as e:
            logger.error(f"Failed to initialize signing keys: {e}")
            self.signing_key = None
            self.verification_key = None
    
    def _load_current_state(self):
        """Load current audit log state."""
        if not self.current_log_file.exists():
            return
        
        try:
            # Read last few entries to get sequence number and hash
            with open(self.current_log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            if lines:
                last_line = lines[-1].strip()
                if last_line:
                    entry_data = json.loads(last_line)
                    self.sequence_number = entry_data.get('sequence_number', 0)
                    self.last_hash = entry_data.get('hash_chain', '')
                    
        except Exception as e:
            logger.error(f"Failed to load current audit state: {e}")
            self.sequence_number = 0
            self.last_hash = ""
    
    async def _verify_log_file_integrity(self, log_file: Path):
        """Verify integrity of a log file."""
        if not CRYPTO_AVAILABLE or not self.verification_key:
            return
        
        try:
            events = await self._read_log_file(log_file, AuditQuery(limit=10000))
            
            # Verify signatures and hash chain
            previous_hash = ""
            for i, event in enumerate(events):
                # This is a simplified verification - in practice you'd need
                # to read the full log entries with signatures
                pass
                
        except Exception as e:
            raise TamperDetectedError(f"Log integrity verification failed: {e}")
    
    async def _check_log_rotation(self):
        """Check if log rotation is needed."""
        if not self.current_log_file.exists():
            return
        
        file_size_mb = self.current_log_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb > 100:  # Rotate at 100MB
            await self._rotate_log_file()
    
    async def _rotate_log_file(self):
        """Rotate current log file."""
        if not self.current_log_file.exists():
            return
        
        # Create archive filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        archive_file = self.archive_dir / f"audit_{timestamp}.log"
        
        # Move current log to archive
        shutil.move(str(self.current_log_file), str(archive_file))
        
        # Reset state for new log file
        self.sequence_number = 0
        self.last_hash = ""
        
        logger.info(f"Rotated audit log to: {archive_file}")
    
    def _get_archive_files_for_period(self, start_time: datetime, end_time: datetime) -> List[Path]:
        """Get archive files that might contain events in the given period."""
        archive_files = []
        
        for file_path in self.archive_dir.glob("audit_*.log*"):
            # Extract timestamp from filename
            try:
                timestamp_str = file_path.stem.split('_', 1)[1]
                if timestamp_str.endswith('.log'):
                    timestamp_str = timestamp_str[:-4]
                
                file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                
                # Include file if it might contain relevant events
                if file_time >= start_time - timedelta(days=1):
                    archive_files.append(file_path)
                    
            except (ValueError, IndexError):
                # Skip files with invalid names
                continue
        
        return sorted(archive_files)
    
    async def _periodic_cleanup(self):
        """Periodically clean up old audit logs."""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                await self._apply_retention_policy()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Audit cleanup error: {e}")
    
    async def _apply_retention_policy(self):
        """Apply retention policy to audit logs."""
        policy = self.retention_policy
        cutoff_date = datetime.utcnow() - timedelta(days=policy.max_age_days)
        compress_date = datetime.utcnow() - timedelta(days=policy.compress_after_days)
        
        for file_path in self.archive_dir.glob("audit_*.log*"):
            try:
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                # Delete old files if policy allows
                if file_time < cutoff_date and policy.auto_delete:
                    if policy.backup_before_delete:
                        # In practice, you'd backup to external storage
                        pass
                    file_path.unlink()
                    logger.info(f"Deleted old audit log: {file_path}")
                
                # Compress old files
                elif file_time < compress_date and not file_path.suffix == '.gz':
                    compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
                    with open(file_path, 'rb') as f_in:
                        with gzip.open(compressed_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    file_path.unlink()
                    logger.info(f"Compressed audit log: {compressed_path}")
                    
            except Exception as e:
                logger.error(f"Failed to process audit log {file_path}: {e}")