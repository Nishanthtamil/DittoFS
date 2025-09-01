"""
Role-based access control system for DittoFS.

This module provides comprehensive access control including:
- User roles and groups with hierarchical permissions
- Granular permission system for files and folders (read, write, admin)
- Permission inheritance and override mechanisms
- Access control list (ACL) support for fine-grained control
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Union, Any
import logging
import fnmatch

logger = logging.getLogger(__name__)


class Permission(Enum):
    """Basic permission types."""
    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    DELETE = "delete"
    ADMIN = "admin"
    SHARE = "share"
    MODIFY_PERMISSIONS = "modify_permissions"


class AccessType(Enum):
    """Access control entry types."""
    ALLOW = "allow"
    DENY = "deny"


@dataclass
class AccessControlEntry:
    """Individual access control entry."""
    principal: str  # User or group name
    principal_type: str  # "user" or "group"
    permissions: Set[Permission]
    access_type: AccessType = AccessType.ALLOW
    conditions: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if this ACE grants a specific permission."""
        return permission in self.permissions
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'principal': self.principal,
            'principal_type': self.principal_type,
            'permissions': [p.value for p in self.permissions],
            'access_type': self.access_type.value,
            'conditions': self.conditions,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccessControlEntry':
        """Create from dictionary."""
        return cls(
            principal=data['principal'],
            principal_type=data['principal_type'],
            permissions={Permission(p) for p in data['permissions']},
            access_type=AccessType(data['access_type']),
            conditions=data.get('conditions', {}),
            created_at=datetime.fromisoformat(data['created_at']),
            created_by=data.get('created_by', '')
        )


@dataclass
class AccessControlList:
    """Access control list for a resource."""
    resource_path: str
    entries: List[AccessControlEntry] = field(default_factory=list)
    owner: str = ""
    group: str = ""
    inherit_parent: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    modified_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_entry(self, entry: AccessControlEntry) -> None:
        """Add an access control entry."""
        self.entries.append(entry)
        self.modified_at = datetime.utcnow()
    
    def remove_entry(self, principal: str, principal_type: str) -> bool:
        """Remove access control entries for a principal."""
        original_count = len(self.entries)
        self.entries = [
            entry for entry in self.entries
            if not (entry.principal == principal and entry.principal_type == principal_type)
        ]
        
        if len(self.entries) < original_count:
            self.modified_at = datetime.utcnow()
            return True
        return False
    
    def get_entries_for_principal(self, principal: str, principal_type: str) -> List[AccessControlEntry]:
        """Get all entries for a specific principal."""
        return [
            entry for entry in self.entries
            if entry.principal == principal and entry.principal_type == principal_type
        ]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'resource_path': self.resource_path,
            'entries': [entry.to_dict() for entry in self.entries],
            'owner': self.owner,
            'group': self.group,
            'inherit_parent': self.inherit_parent,
            'created_at': self.created_at.isoformat(),
            'modified_at': self.modified_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccessControlList':
        """Create from dictionary."""
        return cls(
            resource_path=data['resource_path'],
            entries=[AccessControlEntry.from_dict(entry) for entry in data['entries']],
            owner=data.get('owner', ''),
            group=data.get('group', ''),
            inherit_parent=data.get('inherit_parent', True),
            created_at=datetime.fromisoformat(data['created_at']),
            modified_at=datetime.fromisoformat(data['modified_at'])
        )


@dataclass
class Role:
    """User role definition."""
    name: str
    description: str
    permissions: Set[Permission] = field(default_factory=set)
    parent_roles: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if role has a specific permission."""
        return permission in self.permissions
    
    def add_permission(self, permission: Permission) -> None:
        """Add permission to role."""
        self.permissions.add(permission)
    
    def remove_permission(self, permission: Permission) -> None:
        """Remove permission from role."""
        self.permissions.discard(permission)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'name': self.name,
            'description': self.description,
            'permissions': [p.value for p in self.permissions],
            'parent_roles': self.parent_roles,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Role':
        """Create from dictionary."""
        return cls(
            name=data['name'],
            description=data['description'],
            permissions={Permission(p) for p in data['permissions']},
            parent_roles=data.get('parent_roles', []),
            created_at=datetime.fromisoformat(data['created_at']),
            created_by=data.get('created_by', '')
        )


@dataclass
class Group:
    """User group definition."""
    name: str
    description: str
    members: Set[str] = field(default_factory=set)
    roles: Set[str] = field(default_factory=set)
    parent_groups: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""
    
    def add_member(self, username: str) -> None:
        """Add member to group."""
        self.members.add(username)
    
    def remove_member(self, username: str) -> None:
        """Remove member from group."""
        self.members.discard(username)
    
    def has_member(self, username: str) -> bool:
        """Check if user is a member of this group."""
        return username in self.members
    
    def add_role(self, role_name: str) -> None:
        """Add role to group."""
        self.roles.add(role_name)
    
    def remove_role(self, role_name: str) -> None:
        """Remove role from group."""
        self.roles.discard(role_name)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'name': self.name,
            'description': self.description,
            'members': list(self.members),
            'roles': list(self.roles),
            'parent_groups': self.parent_groups,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Group':
        """Create from dictionary."""
        return cls(
            name=data['name'],
            description=data['description'],
            members=set(data.get('members', [])),
            roles=set(data.get('roles', [])),
            parent_groups=data.get('parent_groups', []),
            created_at=datetime.fromisoformat(data['created_at']),
            created_by=data.get('created_by', '')
        )


@dataclass
class UserPermissions:
    """Computed permissions for a user."""
    username: str
    direct_permissions: Set[Permission] = field(default_factory=set)
    role_permissions: Set[Permission] = field(default_factory=set)
    group_permissions: Set[Permission] = field(default_factory=set)
    effective_permissions: Set[Permission] = field(default_factory=set)
    roles: Set[str] = field(default_factory=set)
    groups: Set[str] = field(default_factory=set)
    
    def compute_effective_permissions(self) -> None:
        """Compute effective permissions from all sources."""
        self.effective_permissions = (
            self.direct_permissions | 
            self.role_permissions | 
            self.group_permissions
        )
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has effective permission."""
        return permission in self.effective_permissions


class AccessDeniedError(Exception):
    """Raised when access is denied."""
    pass


class RoleNotFoundError(Exception):
    """Raised when role is not found."""
    pass


class GroupNotFoundError(Exception):
    """Raised when group is not found."""
    pass


class AccessControlService:
    """
    Comprehensive role-based access control service.
    
    Provides user roles, groups, and fine-grained permissions management.
    """
    
    def __init__(self, data_dir: Path):
        """
        Initialize access control service.
        
        Args:
            data_dir: Directory for storing access control data
        """
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Storage files
        self.roles_file = data_dir / "roles.json"
        self.groups_file = data_dir / "groups.json"
        self.acls_file = data_dir / "acls.json"
        self.user_roles_file = data_dir / "user_roles.json"
        self.user_groups_file = data_dir / "user_groups.json"
        
        # In-memory storage
        self.roles: Dict[str, Role] = {}
        self.groups: Dict[str, Group] = {}
        self.acls: Dict[str, AccessControlList] = {}
        self.user_roles: Dict[str, Set[str]] = {}  # username -> role names
        self.user_groups: Dict[str, Set[str]] = {}  # username -> group names
        
        # Permission cache
        self.permission_cache: Dict[str, UserPermissions] = {}
        self.cache_timeout = 300  # 5 minutes
        self.last_cache_clear = datetime.utcnow()
        
        # Load existing data
        self._load_data()
        
        # Create default roles if none exist
        if not self.roles:
            self._create_default_roles()
    
    async def start(self):
        """Start the access control service."""
        logger.info("Access control service started")
    
    async def stop(self):
        """Stop the access control service."""
        self._save_data()
        logger.info("Access control service stopped")
    
    # Role Management
    
    async def create_role(self, name: str, description: str, 
                         permissions: Optional[Set[Permission]] = None,
                         parent_roles: Optional[List[str]] = None,
                         created_by: str = "") -> Role:
        """
        Create a new role.
        
        Args:
            name: Role name
            description: Role description
            permissions: Set of permissions for the role
            parent_roles: List of parent role names
            created_by: Username of creator
            
        Returns:
            Created role
            
        Raises:
            ValueError: If role already exists
        """
        if name in self.roles:
            raise ValueError(f"Role '{name}' already exists")
        
        role = Role(
            name=name,
            description=description,
            permissions=permissions or set(),
            parent_roles=parent_roles or [],
            created_by=created_by
        )
        
        self.roles[name] = role
        self._clear_permission_cache()
        self._save_data()
        
        logger.info(f"Created role: {name}")
        return role
    
    async def get_role(self, name: str) -> Role:
        """
        Get role by name.
        
        Args:
            name: Role name
            
        Returns:
            Role object
            
        Raises:
            RoleNotFoundError: If role doesn't exist
        """
        if name not in self.roles:
            raise RoleNotFoundError(f"Role '{name}' not found")
        return self.roles[name]
    
    async def update_role(self, name: str, **kwargs) -> Role:
        """
        Update role properties.
        
        Args:
            name: Role name
            **kwargs: Properties to update
            
        Returns:
            Updated role
            
        Raises:
            RoleNotFoundError: If role doesn't exist
        """
        role = await self.get_role(name)
        
        for key, value in kwargs.items():
            if hasattr(role, key):
                setattr(role, key, value)
        
        self._clear_permission_cache()
        self._save_data()
        
        logger.info(f"Updated role: {name}")
        return role
    
    async def delete_role(self, name: str) -> bool:
        """
        Delete a role.
        
        Args:
            name: Role name
            
        Returns:
            True if role was deleted
        """
        if name not in self.roles:
            return False
        
        # Remove role from all users
        for username, roles in self.user_roles.items():
            roles.discard(name)
        
        # Remove role from all groups
        for group in self.groups.values():
            group.remove_role(name)
        
        del self.roles[name]
        self._clear_permission_cache()
        self._save_data()
        
        logger.info(f"Deleted role: {name}")
        return True
    
    async def get_role_permissions(self, role_name: str) -> Set[Permission]:
        """
        Get effective permissions for a role (including inherited).
        
        Args:
            role_name: Role name
            
        Returns:
            Set of effective permissions
        """
        if role_name not in self.roles:
            return set()
        
        role = self.roles[role_name]
        permissions = role.permissions.copy()
        
        # Add permissions from parent roles
        for parent_name in role.parent_roles:
            if parent_name in self.roles:
                parent_permissions = await self.get_role_permissions(parent_name)
                permissions.update(parent_permissions)
        
        return permissions
    
    # Group Management
    
    async def create_group(self, name: str, description: str,
                          members: Optional[Set[str]] = None,
                          roles: Optional[Set[str]] = None,
                          parent_groups: Optional[List[str]] = None,
                          created_by: str = "") -> Group:
        """
        Create a new group.
        
        Args:
            name: Group name
            description: Group description
            members: Set of member usernames
            roles: Set of role names
            parent_groups: List of parent group names
            created_by: Username of creator
            
        Returns:
            Created group
            
        Raises:
            ValueError: If group already exists
        """
        if name in self.groups:
            raise ValueError(f"Group '{name}' already exists")
        
        group = Group(
            name=name,
            description=description,
            members=members or set(),
            roles=roles or set(),
            parent_groups=parent_groups or [],
            created_by=created_by
        )
        
        self.groups[name] = group
        
        # Update user group memberships
        for member in group.members:
            if member not in self.user_groups:
                self.user_groups[member] = set()
            self.user_groups[member].add(name)
        
        self._clear_permission_cache()
        self._save_data()
        
        logger.info(f"Created group: {name}")
        return group
    
    async def get_group(self, name: str) -> Group:
        """
        Get group by name.
        
        Args:
            name: Group name
            
        Returns:
            Group object
            
        Raises:
            GroupNotFoundError: If group doesn't exist
        """
        if name not in self.groups:
            raise GroupNotFoundError(f"Group '{name}' not found")
        return self.groups[name]
    
    async def add_user_to_group(self, username: str, group_name: str) -> bool:
        """
        Add user to group.
        
        Args:
            username: Username
            group_name: Group name
            
        Returns:
            True if user was added
            
        Raises:
            GroupNotFoundError: If group doesn't exist
        """
        group = await self.get_group(group_name)
        
        if username not in group.members:
            group.add_member(username)
            
            if username not in self.user_groups:
                self.user_groups[username] = set()
            self.user_groups[username].add(group_name)
            
            self._clear_permission_cache()
            self._save_data()
            
            logger.info(f"Added user {username} to group {group_name}")
            return True
        
        return False
    
    async def remove_user_from_group(self, username: str, group_name: str) -> bool:
        """
        Remove user from group.
        
        Args:
            username: Username
            group_name: Group name
            
        Returns:
            True if user was removed
        """
        if group_name not in self.groups:
            return False
        
        group = self.groups[group_name]
        
        if username in group.members:
            group.remove_member(username)
            
            if username in self.user_groups:
                self.user_groups[username].discard(group_name)
            
            self._clear_permission_cache()
            self._save_data()
            
            logger.info(f"Removed user {username} from group {group_name}")
            return True
        
        return False
    
    # User Role Assignment
    
    async def assign_role_to_user(self, username: str, role_name: str) -> bool:
        """
        Assign role to user.
        
        Args:
            username: Username
            role_name: Role name
            
        Returns:
            True if role was assigned
            
        Raises:
            RoleNotFoundError: If role doesn't exist
        """
        await self.get_role(role_name)  # Validate role exists
        
        if username not in self.user_roles:
            self.user_roles[username] = set()
        
        if role_name not in self.user_roles[username]:
            self.user_roles[username].add(role_name)
            self._clear_permission_cache()
            self._save_data()
            
            logger.info(f"Assigned role {role_name} to user {username}")
            return True
        
        return False
    
    async def revoke_role_from_user(self, username: str, role_name: str) -> bool:
        """
        Revoke role from user.
        
        Args:
            username: Username
            role_name: Role name
            
        Returns:
            True if role was revoked
        """
        if username not in self.user_roles:
            return False
        
        if role_name in self.user_roles[username]:
            self.user_roles[username].discard(role_name)
            self._clear_permission_cache()
            self._save_data()
            
            logger.info(f"Revoked role {role_name} from user {username}")
            return True
        
        return False
    
    # Permission Checking
    
    async def get_user_permissions(self, username: str) -> UserPermissions:
        """
        Get effective permissions for a user.
        
        Args:
            username: Username
            
        Returns:
            User permissions object
        """
        # Check cache first
        if username in self.permission_cache:
            cached = self.permission_cache[username]
            # Simple cache timeout check
            if (datetime.utcnow() - self.last_cache_clear).total_seconds() < self.cache_timeout:
                return cached
        
        user_perms = UserPermissions(username=username)
        
        # Get direct role assignments
        user_roles = self.user_roles.get(username, set())
        user_perms.roles = user_roles.copy()
        
        # Get role permissions
        for role_name in user_roles:
            role_permissions = await self.get_role_permissions(role_name)
            user_perms.role_permissions.update(role_permissions)
        
        # Get group memberships
        user_groups = self.user_groups.get(username, set())
        user_perms.groups = user_groups.copy()
        
        # Get group permissions (including inherited groups)
        all_groups = await self._get_all_user_groups(username)
        for group_name in all_groups:
            if group_name in self.groups:
                group = self.groups[group_name]
                for role_name in group.roles:
                    role_permissions = await self.get_role_permissions(role_name)
                    user_perms.group_permissions.update(role_permissions)
        
        # Compute effective permissions
        user_perms.compute_effective_permissions()
        
        # Cache result
        self.permission_cache[username] = user_perms
        
        return user_perms
    
    async def check_permission(self, username: str, permission: Permission,
                             resource_path: Optional[str] = None) -> bool:
        """
        Check if user has a specific permission.
        
        Args:
            username: Username
            permission: Permission to check
            resource_path: Optional resource path for ACL checking
            
        Returns:
            True if user has permission
        """
        # Get user permissions
        user_perms = await self.get_user_permissions(username)
        
        # Check global permissions first
        if user_perms.has_permission(permission):
            # If checking resource-specific permission, also check ACL
            if resource_path:
                return await self._check_acl_permission(username, permission, resource_path)
            return True
        
        # If no global permission, check ACL only
        if resource_path:
            return await self._check_acl_permission(username, permission, resource_path)
        
        return False
    
    async def require_permission(self, username: str, permission: Permission,
                               resource_path: Optional[str] = None) -> None:
        """
        Require user to have permission, raise exception if not.
        
        Args:
            username: Username
            permission: Required permission
            resource_path: Optional resource path
            
        Raises:
            AccessDeniedError: If user doesn't have permission
        """
        if not await self.check_permission(username, permission, resource_path):
            resource_info = f" for {resource_path}" if resource_path else ""
            raise AccessDeniedError(
                f"User {username} does not have {permission.value} permission{resource_info}"
            )
    
    # Access Control Lists (ACLs)
    
    async def create_acl(self, resource_path: str, owner: str = "",
                        group: str = "", inherit_parent: bool = True) -> AccessControlList:
        """
        Create access control list for a resource.
        
        Args:
            resource_path: Path to the resource
            owner: Resource owner
            group: Resource group
            inherit_parent: Whether to inherit parent permissions
            
        Returns:
            Created ACL
        """
        acl = AccessControlList(
            resource_path=resource_path,
            owner=owner,
            group=group,
            inherit_parent=inherit_parent
        )
        
        self.acls[resource_path] = acl
        self._save_data()
        
        logger.info(f"Created ACL for resource: {resource_path}")
        return acl
    
    async def get_acl(self, resource_path: str) -> Optional[AccessControlList]:
        """
        Get ACL for a resource.
        
        Args:
            resource_path: Resource path
            
        Returns:
            ACL or None if not found
        """
        return self.acls.get(resource_path)
    
    async def add_acl_entry(self, resource_path: str, principal: str,
                           principal_type: str, permissions: Set[Permission],
                           access_type: AccessType = AccessType.ALLOW,
                           created_by: str = "") -> bool:
        """
        Add entry to resource ACL.
        
        Args:
            resource_path: Resource path
            principal: User or group name
            principal_type: "user" or "group"
            permissions: Set of permissions
            access_type: Allow or deny
            created_by: Username of creator
            
        Returns:
            True if entry was added
        """
        acl = self.acls.get(resource_path)
        if not acl:
            acl = await self.create_acl(resource_path)
        
        entry = AccessControlEntry(
            principal=principal,
            principal_type=principal_type,
            permissions=permissions,
            access_type=access_type,
            created_by=created_by
        )
        
        acl.add_entry(entry)
        self._save_data()
        
        logger.info(f"Added ACL entry for {principal} on {resource_path}")
        return True
    
    async def remove_acl_entry(self, resource_path: str, principal: str,
                              principal_type: str) -> bool:
        """
        Remove ACL entry for a principal.
        
        Args:
            resource_path: Resource path
            principal: User or group name
            principal_type: "user" or "group"
            
        Returns:
            True if entry was removed
        """
        acl = self.acls.get(resource_path)
        if not acl:
            return False
        
        result = acl.remove_entry(principal, principal_type)
        if result:
            self._save_data()
            logger.info(f"Removed ACL entry for {principal} on {resource_path}")
        
        return result
    
    async def _check_acl_permission(self, username: str, permission: Permission,
                                  resource_path: str) -> bool:
        """Check permission against ACL for a resource."""
        # Get all possible ACLs (resource and parent paths)
        acls_to_check = []
        
        # Check direct ACL
        if resource_path in self.acls:
            acls_to_check.append(self.acls[resource_path])
        
        # Check parent ACLs if inheritance is enabled
        current_path = resource_path
        while current_path and '/' in current_path:
            parent_path = str(Path(current_path).parent)
            if parent_path in self.acls:
                parent_acl = self.acls[parent_path]
                if parent_acl.inherit_parent:
                    acls_to_check.append(parent_acl)
            current_path = parent_path if parent_path != '/' else None
        
        # Check each ACL
        for acl in acls_to_check:
            # Check owner permissions
            if acl.owner == username:
                # Owner typically has all permissions
                return True
            
            # Check user-specific entries
            user_entries = acl.get_entries_for_principal(username, "user")
            for entry in user_entries:
                if entry.has_permission(permission):
                    return entry.access_type == AccessType.ALLOW
            
            # Check group entries
            user_groups = await self._get_all_user_groups(username)
            for group_name in user_groups:
                group_entries = acl.get_entries_for_principal(group_name, "group")
                for entry in group_entries:
                    if entry.has_permission(permission):
                        return entry.access_type == AccessType.ALLOW
        
        return False
    
    async def _get_all_user_groups(self, username: str) -> Set[str]:
        """Get all groups for a user (including inherited)."""
        all_groups = set()
        direct_groups = self.user_groups.get(username, set())
        
        # Add direct groups
        all_groups.update(direct_groups)
        
        # Add inherited groups
        for group_name in direct_groups:
            if group_name in self.groups:
                group = self.groups[group_name]
                all_groups.update(group.parent_groups)
        
        return all_groups
    
    def _create_default_roles(self):
        """Create default system roles."""
        default_roles = [
            {
                'name': 'admin',
                'description': 'System administrator with full access',
                'permissions': {Permission.READ, Permission.WRITE, Permission.DELETE, 
                              Permission.ADMIN, Permission.SHARE, Permission.MODIFY_PERMISSIONS}
            },
            {
                'name': 'user',
                'description': 'Regular user with basic access',
                'permissions': {Permission.READ, Permission.WRITE}
            },
            {
                'name': 'readonly',
                'description': 'Read-only access',
                'permissions': {Permission.READ}
            },
            {
                'name': 'guest',
                'description': 'Guest user with minimal access',
                'permissions': set()
            }
        ]
        
        for role_data in default_roles:
            role = Role(
                name=role_data['name'],
                description=role_data['description'],
                permissions=role_data['permissions'],
                created_by='system'
            )
            self.roles[role.name] = role
        
        logger.info("Created default roles")
    
    def _clear_permission_cache(self):
        """Clear permission cache."""
        self.permission_cache.clear()
        self.last_cache_clear = datetime.utcnow()
    
    def _load_data(self):
        """Load access control data from storage."""
        try:
            # Load roles
            if self.roles_file.exists():
                data = json.loads(self.roles_file.read_text())
                for role_name, role_data in data.items():
                    self.roles[role_name] = Role.from_dict(role_data)
            
            # Load groups
            if self.groups_file.exists():
                data = json.loads(self.groups_file.read_text())
                for group_name, group_data in data.items():
                    self.groups[group_name] = Group.from_dict(group_data)
            
            # Load ACLs
            if self.acls_file.exists():
                data = json.loads(self.acls_file.read_text())
                for resource_path, acl_data in data.items():
                    self.acls[resource_path] = AccessControlList.from_dict(acl_data)
            
            # Load user roles
            if self.user_roles_file.exists():
                data = json.loads(self.user_roles_file.read_text())
                self.user_roles = {
                    username: set(roles) for username, roles in data.items()
                }
            
            # Load user groups
            if self.user_groups_file.exists():
                data = json.loads(self.user_groups_file.read_text())
                self.user_groups = {
                    username: set(groups) for username, groups in data.items()
                }
                
        except Exception as e:
            logger.error(f"Failed to load access control data: {e}")
    
    def _save_data(self):
        """Save access control data to storage."""
        try:
            # Save roles
            roles_data = {
                name: role.to_dict() for name, role in self.roles.items()
            }
            self.roles_file.write_text(json.dumps(roles_data, indent=2))
            
            # Save groups
            groups_data = {
                name: group.to_dict() for name, group in self.groups.items()
            }
            self.groups_file.write_text(json.dumps(groups_data, indent=2))
            
            # Save ACLs
            acls_data = {
                path: acl.to_dict() for path, acl in self.acls.items()
            }
            self.acls_file.write_text(json.dumps(acls_data, indent=2))
            
            # Save user roles
            user_roles_data = {
                username: list(roles) for username, roles in self.user_roles.items()
            }
            self.user_roles_file.write_text(json.dumps(user_roles_data, indent=2))
            
            # Save user groups
            user_groups_data = {
                username: list(groups) for username, groups in self.user_groups.items()
            }
            self.user_groups_file.write_text(json.dumps(user_groups_data, indent=2))
            
        except Exception as e:
            logger.error(f"Failed to save access control data: {e}")