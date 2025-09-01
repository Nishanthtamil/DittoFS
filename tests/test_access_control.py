"""
Tests for the access control system.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from src.dittofs.access_control import (
    AccessControlEntry,
    AccessControlList,
    AccessControlService,
    AccessDeniedError,
    AccessType,
    Group,
    GroupNotFoundError,
    Permission,
    Role,
    RoleNotFoundError,
    UserPermissions,
)


@pytest.fixture
async def access_control_service():
    """Create access control service for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AccessControlService(Path(temp_dir))
        await service.start()
        yield service
        await service.stop()


class TestAccessControlService:
    """Test access control service functionality."""

    async def test_service_initialization(self, access_control_service):
        """Test service initialization with default roles."""
        assert len(access_control_service.roles) > 0
        assert "admin" in access_control_service.roles
        assert "user" in access_control_service.roles
        assert "readonly" in access_control_service.roles
        assert "guest" in access_control_service.roles

    async def test_create_role(self, access_control_service):
        """Test role creation."""
        permissions = {Permission.READ, Permission.WRITE}
        role = await access_control_service.create_role(
            "test_role", "Test role description", permissions, created_by="admin"
        )

        assert role.name == "test_role"
        assert role.description == "Test role description"
        assert role.permissions == permissions
        assert role.created_by == "admin"
        assert "test_role" in access_control_service.roles

    async def test_create_duplicate_role(self, access_control_service):
        """Test creating duplicate role fails."""
        await access_control_service.create_role("test_role", "Description")

        with pytest.raises(ValueError, match="already exists"):
            await access_control_service.create_role("test_role", "Another description")

    async def test_get_role(self, access_control_service):
        """Test getting role by name."""
        # Test existing role
        role = await access_control_service.get_role("admin")
        assert role.name == "admin"

        # Test non-existent role
        with pytest.raises(RoleNotFoundError):
            await access_control_service.get_role("nonexistent")

    async def test_update_role(self, access_control_service):
        """Test updating role properties."""
        # Create role
        await access_control_service.create_role("test_role", "Original description")

        # Update role
        updated_role = await access_control_service.update_role(
            "test_role",
            description="Updated description",
            permissions={Permission.READ},
        )

        assert updated_role.description == "Updated description"
        assert updated_role.permissions == {Permission.READ}

    async def test_delete_role(self, access_control_service):
        """Test role deletion."""
        # Create role
        await access_control_service.create_role("test_role", "Description")
        assert "test_role" in access_control_service.roles

        # Delete role
        result = await access_control_service.delete_role("test_role")
        assert result is True
        assert "test_role" not in access_control_service.roles

        # Delete non-existent role
        result = await access_control_service.delete_role("nonexistent")
        assert result is False

    async def test_get_role_permissions_with_inheritance(self, access_control_service):
        """Test getting role permissions with inheritance."""
        # Create parent role
        parent_permissions = {Permission.READ, Permission.WRITE}
        await access_control_service.create_role(
            "parent_role", "Parent role", parent_permissions
        )

        # Create child role with inheritance
        child_permissions = {Permission.DELETE}
        await access_control_service.create_role(
            "child_role", "Child role", child_permissions, ["parent_role"]
        )

        # Get effective permissions
        effective_permissions = await access_control_service.get_role_permissions(
            "child_role"
        )
        expected_permissions = parent_permissions | child_permissions

        assert effective_permissions == expected_permissions

    async def test_create_group(self, access_control_service):
        """Test group creation."""
        members = {"user1", "user2"}
        roles = {"user", "readonly"}

        group = await access_control_service.create_group(
            "test_group", "Test group description", members, roles, created_by="admin"
        )

        assert group.name == "test_group"
        assert group.description == "Test group description"
        assert group.members == members
        assert group.roles == roles
        assert group.created_by == "admin"
        assert "test_group" in access_control_service.groups

        # Check user group memberships were updated
        assert "test_group" in access_control_service.user_groups.get("user1", set())
        assert "test_group" in access_control_service.user_groups.get("user2", set())

    async def test_get_group(self, access_control_service):
        """Test getting group by name."""
        # Create group
        await access_control_service.create_group("test_group", "Description")

        # Test existing group
        group = await access_control_service.get_group("test_group")
        assert group.name == "test_group"

        # Test non-existent group
        with pytest.raises(GroupNotFoundError):
            await access_control_service.get_group("nonexistent")

    async def test_add_user_to_group(self, access_control_service):
        """Test adding user to group."""
        # Create group
        await access_control_service.create_group("test_group", "Description")

        # Add user to group
        result = await access_control_service.add_user_to_group("user1", "test_group")
        assert result is True

        group = await access_control_service.get_group("test_group")
        assert "user1" in group.members
        assert "test_group" in access_control_service.user_groups.get("user1", set())

        # Add same user again (should return False)
        result = await access_control_service.add_user_to_group("user1", "test_group")
        assert result is False

    async def test_remove_user_from_group(self, access_control_service):
        """Test removing user from group."""
        # Create group with user
        await access_control_service.create_group(
            "test_group", "Description", {"user1"}
        )

        # Remove user from group
        result = await access_control_service.remove_user_from_group(
            "user1", "test_group"
        )
        assert result is True

        group = await access_control_service.get_group("test_group")
        assert "user1" not in group.members
        assert "test_group" not in access_control_service.user_groups.get(
            "user1", set()
        )

        # Remove user not in group
        result = await access_control_service.remove_user_from_group(
            "user2", "test_group"
        )
        assert result is False

    async def test_assign_role_to_user(self, access_control_service):
        """Test assigning role to user."""
        # Assign existing role
        result = await access_control_service.assign_role_to_user("user1", "admin")
        assert result is True
        assert "admin" in access_control_service.user_roles.get("user1", set())

        # Assign same role again
        result = await access_control_service.assign_role_to_user("user1", "admin")
        assert result is False

        # Assign non-existent role
        with pytest.raises(RoleNotFoundError):
            await access_control_service.assign_role_to_user("user1", "nonexistent")

    async def test_revoke_role_from_user(self, access_control_service):
        """Test revoking role from user."""
        # Assign role first
        await access_control_service.assign_role_to_user("user1", "admin")

        # Revoke role
        result = await access_control_service.revoke_role_from_user("user1", "admin")
        assert result is True
        assert "admin" not in access_control_service.user_roles.get("user1", set())

        # Revoke role not assigned
        result = await access_control_service.revoke_role_from_user("user1", "user")
        assert result is False

    async def test_get_user_permissions(self, access_control_service):
        """Test getting user permissions."""
        # Create custom role
        custom_permissions = {Permission.READ, Permission.WRITE}
        await access_control_service.create_role(
            "custom_role", "Custom", custom_permissions
        )

        # Create group with role
        await access_control_service.create_group("test_group", "Group", roles={"user"})

        # Assign role and group to user
        await access_control_service.assign_role_to_user("user1", "custom_role")
        await access_control_service.add_user_to_group("user1", "test_group")

        # Get user permissions
        user_perms = await access_control_service.get_user_permissions("user1")

        assert user_perms.username == "user1"
        assert "custom_role" in user_perms.roles
        assert "test_group" in user_perms.groups
        assert Permission.READ in user_perms.effective_permissions
        assert Permission.WRITE in user_perms.effective_permissions

    async def test_check_permission(self, access_control_service):
        """Test permission checking."""
        # Assign admin role to user
        await access_control_service.assign_role_to_user("user1", "admin")

        # Check permission
        has_read = await access_control_service.check_permission(
            "user1", Permission.READ
        )
        assert has_read is True

        has_admin = await access_control_service.check_permission(
            "user1", Permission.ADMIN
        )
        assert has_admin is True

        # Check permission for user without role
        has_admin_user2 = await access_control_service.check_permission(
            "user2", Permission.ADMIN
        )
        assert has_admin_user2 is False

    async def test_require_permission(self, access_control_service):
        """Test requiring permission."""
        # Assign role to user
        await access_control_service.assign_role_to_user("user1", "admin")

        # Should not raise exception
        await access_control_service.require_permission("user1", Permission.READ)

        # Should raise exception
        with pytest.raises(AccessDeniedError):
            await access_control_service.require_permission("user2", Permission.ADMIN)

    async def test_create_acl(self, access_control_service):
        """Test ACL creation."""
        acl = await access_control_service.create_acl(
            "/test/file.txt", owner="user1", group="test_group"
        )

        assert acl.resource_path == "/test/file.txt"
        assert acl.owner == "user1"
        assert acl.group == "test_group"
        assert acl.inherit_parent is True
        assert "/test/file.txt" in access_control_service.acls

    async def test_add_acl_entry(self, access_control_service):
        """Test adding ACL entry."""
        resource_path = "/test/file.txt"
        permissions = {Permission.READ, Permission.WRITE}

        # Add ACL entry (creates ACL if not exists)
        result = await access_control_service.add_acl_entry(
            resource_path, "user1", "user", permissions, created_by="admin"
        )

        assert result is True

        acl = await access_control_service.get_acl(resource_path)
        assert acl is not None
        assert len(acl.entries) == 1

        entry = acl.entries[0]
        assert entry.principal == "user1"
        assert entry.principal_type == "user"
        assert entry.permissions == permissions
        assert entry.access_type == AccessType.ALLOW
        assert entry.created_by == "admin"

    async def test_remove_acl_entry(self, access_control_service):
        """Test removing ACL entry."""
        resource_path = "/test/file.txt"

        # Add ACL entry
        await access_control_service.add_acl_entry(
            resource_path, "user1", "user", {Permission.READ}
        )

        # Remove ACL entry
        result = await access_control_service.remove_acl_entry(
            resource_path, "user1", "user"
        )

        assert result is True

        acl = await access_control_service.get_acl(resource_path)
        assert len(acl.entries) == 0

        # Remove non-existent entry
        result = await access_control_service.remove_acl_entry(
            resource_path, "user2", "user"
        )
        assert result is False

    async def test_acl_permission_checking(self, access_control_service):
        """Test permission checking with ACLs."""
        resource_path = "/test/file.txt"

        # Create ACL with specific permissions
        await access_control_service.add_acl_entry(
            resource_path, "user1", "user", {Permission.READ}
        )

        # User should have read permission on resource
        has_read = await access_control_service.check_permission(
            "user1", Permission.READ, resource_path
        )
        assert has_read is True

        # User should not have write permission on resource
        has_write = await access_control_service.check_permission(
            "user1", Permission.WRITE, resource_path
        )
        assert has_write is False

        # Different user should not have access
        has_read_user2 = await access_control_service.check_permission(
            "user2", Permission.READ, resource_path
        )
        assert has_read_user2 is False

    async def test_acl_owner_permissions(self, access_control_service):
        """Test ACL owner permissions."""
        resource_path = "/test/file.txt"

        # Create ACL with owner
        await access_control_service.create_acl(resource_path, owner="user1")

        # Owner should have all permissions
        has_read = await access_control_service.check_permission(
            "user1", Permission.READ, resource_path
        )
        assert has_read is True

        has_write = await access_control_service.check_permission(
            "user1", Permission.WRITE, resource_path
        )
        assert has_write is True

    async def test_acl_group_permissions(self, access_control_service):
        """Test ACL group permissions."""
        resource_path = "/test/file.txt"

        # Create group and add user
        await access_control_service.create_group("test_group", "Test group")
        await access_control_service.add_user_to_group("user1", "test_group")

        # Add group ACL entry
        await access_control_service.add_acl_entry(
            resource_path, "test_group", "group", {Permission.READ}
        )

        # User in group should have permission
        has_read = await access_control_service.check_permission(
            "user1", Permission.READ, resource_path
        )
        assert has_read is True

        # User not in group should not have permission
        has_read_user2 = await access_control_service.check_permission(
            "user2", Permission.READ, resource_path
        )
        assert has_read_user2 is False

    async def test_persistence(self, access_control_service):
        """Test data persistence across service restarts."""
        # Create test data
        await access_control_service.create_role(
            "test_role", "Test role", {Permission.READ}
        )
        await access_control_service.create_group("test_group", "Test group", {"user1"})
        await access_control_service.assign_role_to_user("user1", "test_role")
        await access_control_service.add_acl_entry(
            "/test/file", "user1", "user", {Permission.WRITE}
        )

        # Stop and restart service
        data_dir = access_control_service.data_dir
        await access_control_service.stop()

        new_service = AccessControlService(data_dir)
        await new_service.start()

        try:
            # Verify data persisted
            assert "test_role" in new_service.roles
            assert "test_group" in new_service.groups
            assert "test_role" in new_service.user_roles.get("user1", set())
            assert "test_group" in new_service.user_groups.get("user1", set())
            assert "/test/file" in new_service.acls

            # Verify functionality still works
            has_permission = await new_service.check_permission(
                "user1", Permission.READ
            )
            assert has_permission is True

        finally:
            await new_service.stop()


class TestRole:
    """Test Role class functionality."""

    def test_role_creation(self):
        """Test role creation."""
        permissions = {Permission.READ, Permission.WRITE}
        role = Role(
            name="test_role",
            description="Test role",
            permissions=permissions,
            parent_roles=["parent_role"],
            created_by="admin",
        )

        assert role.name == "test_role"
        assert role.description == "Test role"
        assert role.permissions == permissions
        assert role.parent_roles == ["parent_role"]
        assert role.created_by == "admin"

    def test_role_permissions(self):
        """Test role permission methods."""
        role = Role("test_role", "Test role")

        # Test has_permission
        assert not role.has_permission(Permission.READ)

        # Test add_permission
        role.add_permission(Permission.READ)
        assert role.has_permission(Permission.READ)

        # Test remove_permission
        role.remove_permission(Permission.READ)
        assert not role.has_permission(Permission.READ)

    def test_role_serialization(self):
        """Test role serialization."""
        permissions = {Permission.READ, Permission.WRITE}
        role = Role(
            name="test_role",
            description="Test role",
            permissions=permissions,
            created_by="admin",
        )

        # Test to_dict
        role_dict = role.to_dict()
        assert role_dict["name"] == "test_role"
        assert role_dict["description"] == "Test role"
        assert set(role_dict["permissions"]) == {"read", "write"}
        assert role_dict["created_by"] == "admin"

        # Test from_dict
        restored_role = Role.from_dict(role_dict)
        assert restored_role.name == role.name
        assert restored_role.description == role.description
        assert restored_role.permissions == role.permissions
        assert restored_role.created_by == role.created_by


class TestGroup:
    """Test Group class functionality."""

    def test_group_creation(self):
        """Test group creation."""
        members = {"user1", "user2"}
        roles = {"admin", "user"}

        group = Group(
            name="test_group",
            description="Test group",
            members=members,
            roles=roles,
            created_by="admin",
        )

        assert group.name == "test_group"
        assert group.description == "Test group"
        assert group.members == members
        assert group.roles == roles
        assert group.created_by == "admin"

    def test_group_member_management(self):
        """Test group member management."""
        group = Group("test_group", "Test group")

        # Test add_member
        group.add_member("user1")
        assert group.has_member("user1")

        # Test remove_member
        group.remove_member("user1")
        assert not group.has_member("user1")

    def test_group_role_management(self):
        """Test group role management."""
        group = Group("test_group", "Test group")

        # Test add_role
        group.add_role("admin")
        assert "admin" in group.roles

        # Test remove_role
        group.remove_role("admin")
        assert "admin" not in group.roles


class TestAccessControlEntry:
    """Test AccessControlEntry class functionality."""

    def test_ace_creation(self):
        """Test ACE creation."""
        permissions = {Permission.READ, Permission.WRITE}
        ace = AccessControlEntry(
            principal="user1",
            principal_type="user",
            permissions=permissions,
            access_type=AccessType.ALLOW,
            created_by="admin",
        )

        assert ace.principal == "user1"
        assert ace.principal_type == "user"
        assert ace.permissions == permissions
        assert ace.access_type == AccessType.ALLOW
        assert ace.created_by == "admin"

    def test_ace_has_permission(self):
        """Test ACE permission checking."""
        permissions = {Permission.READ, Permission.WRITE}
        ace = AccessControlEntry("user1", "user", permissions)

        assert ace.has_permission(Permission.READ)
        assert ace.has_permission(Permission.WRITE)
        assert not ace.has_permission(Permission.DELETE)

    def test_ace_serialization(self):
        """Test ACE serialization."""
        permissions = {Permission.READ, Permission.WRITE}
        ace = AccessControlEntry(
            principal="user1",
            principal_type="user",
            permissions=permissions,
            created_by="admin",
        )

        # Test to_dict
        ace_dict = ace.to_dict()
        assert ace_dict["principal"] == "user1"
        assert ace_dict["principal_type"] == "user"
        assert set(ace_dict["permissions"]) == {"read", "write"}
        assert ace_dict["created_by"] == "admin"

        # Test from_dict
        restored_ace = AccessControlEntry.from_dict(ace_dict)
        assert restored_ace.principal == ace.principal
        assert restored_ace.principal_type == ace.principal_type
        assert restored_ace.permissions == ace.permissions
        assert restored_ace.created_by == ace.created_by


class TestAccessControlList:
    """Test AccessControlList class functionality."""

    def test_acl_creation(self):
        """Test ACL creation."""
        acl = AccessControlList(
            resource_path="/test/file.txt", owner="user1", group="test_group"
        )

        assert acl.resource_path == "/test/file.txt"
        assert acl.owner == "user1"
        assert acl.group == "test_group"
        assert acl.inherit_parent is True
        assert len(acl.entries) == 0

    def test_acl_entry_management(self):
        """Test ACL entry management."""
        acl = AccessControlList("/test/file.txt")

        # Add entry
        entry = AccessControlEntry("user1", "user", {Permission.READ})
        acl.add_entry(entry)
        assert len(acl.entries) == 1

        # Get entries for principal
        user_entries = acl.get_entries_for_principal("user1", "user")
        assert len(user_entries) == 1
        assert user_entries[0] == entry

        # Remove entry
        result = acl.remove_entry("user1", "user")
        assert result is True
        assert len(acl.entries) == 0

        # Remove non-existent entry
        result = acl.remove_entry("user2", "user")
        assert result is False


class TestUserPermissions:
    """Test UserPermissions class functionality."""

    def test_user_permissions_creation(self):
        """Test user permissions creation."""
        user_perms = UserPermissions("user1")

        assert user_perms.username == "user1"
        assert len(user_perms.direct_permissions) == 0
        assert len(user_perms.role_permissions) == 0
        assert len(user_perms.group_permissions) == 0
        assert len(user_perms.effective_permissions) == 0

    def test_compute_effective_permissions(self):
        """Test computing effective permissions."""
        user_perms = UserPermissions("user1")
        user_perms.direct_permissions = {Permission.READ}
        user_perms.role_permissions = {Permission.WRITE}
        user_perms.group_permissions = {Permission.DELETE}

        user_perms.compute_effective_permissions()

        expected = {Permission.READ, Permission.WRITE, Permission.DELETE}
        assert user_perms.effective_permissions == expected

    def test_has_permission(self):
        """Test permission checking."""
        user_perms = UserPermissions("user1")
        user_perms.effective_permissions = {Permission.READ, Permission.WRITE}

        assert user_perms.has_permission(Permission.READ)
        assert user_perms.has_permission(Permission.WRITE)
        assert not user_perms.has_permission(Permission.DELETE)


if __name__ == "__main__":
    pytest.main([__file__])
