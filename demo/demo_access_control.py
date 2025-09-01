#!/usr/bin/env python3
"""
Demo script for DittoFS Access Control System.

This script demonstrates the role-based access control features including:
- User roles and groups with hierarchical permissions
- Granular permission system for files and folders
- Permission inheritance and override mechanisms
- Access control list (ACL) support for fine-grained control
"""

import asyncio
import tempfile
from pathlib import Path

from src.dittofs.access_control import (
    AccessControlService,
    AccessDeniedError,
    AccessType,
    GroupNotFoundError,
    Permission,
    RoleNotFoundError,
)


async def demo_default_roles():
    """Demonstrate default system roles."""
    print("=== Default Roles Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Default system roles:")
            for role_name, role in access_service.roles.items():
                permissions = [p.value for p in role.permissions]
                print(f"  {role_name}: {role.description}")
                print(
                    f"    Permissions: {', '.join(permissions) if permissions else 'None'}"
                )
                print(f"    Created by: {role.created_by}")

        finally:
            await access_service.stop()


async def demo_role_management():
    """Demonstrate role creation and management."""
    print("\n=== Role Management Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Creating custom roles...")

            # Create editor role
            editor_permissions = {Permission.READ, Permission.WRITE, Permission.SHARE}
            editor_role = await access_service.create_role(
                "editor",
                "Content editor with read/write access",
                editor_permissions,
                created_by="admin",
            )
            print(f"✓ Created role: {editor_role.name}")
            print(f"  Description: {editor_role.description}")
            print(f"  Permissions: {[p.value for p in editor_role.permissions]}")

            # Create moderator role with inheritance
            moderator_permissions = {Permission.DELETE, Permission.MODIFY_PERMISSIONS}
            moderator_role = await access_service.create_role(
                "moderator",
                "Content moderator with additional privileges",
                moderator_permissions,
                parent_roles=["editor"],
                created_by="admin",
            )
            print(f"✓ Created role: {moderator_role.name}")
            print(f"  Parent roles: {moderator_role.parent_roles}")

            # Get effective permissions (including inherited)
            effective_perms = await access_service.get_role_permissions("moderator")
            print(f"  Effective permissions: {[p.value for p in effective_perms]}")

            # Update role
            await access_service.update_role(
                "editor", description="Updated: Content editor with enhanced access"
            )
            updated_role = await access_service.get_role("editor")
            print(f"✓ Updated role description: {updated_role.description}")

            # List all roles
            print(f"\nTotal roles: {len(access_service.roles)}")
            for name in access_service.roles.keys():
                print(f"  - {name}")

        finally:
            await access_service.stop()


async def demo_group_management():
    """Demonstrate group creation and management."""
    print("\n=== Group Management Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Creating user groups...")

            # Create development team group
            dev_team = await access_service.create_group(
                "dev_team",
                "Development team members",
                members={"alice", "bob", "charlie"},
                roles={"user", "editor"},
                created_by="admin",
            )
            print(f"✓ Created group: {dev_team.name}")
            print(f"  Description: {dev_team.description}")
            print(f"  Members: {list(dev_team.members)}")
            print(f"  Roles: {list(dev_team.roles)}")

            # Create admin group
            admin_group = await access_service.create_group(
                "admins", "System administrators", roles={"admin"}, created_by="system"
            )
            print(f"✓ Created group: {admin_group.name}")

            # Add user to group
            result = await access_service.add_user_to_group("diana", "dev_team")
            print(f"✓ Added user 'diana' to dev_team: {result}")

            # Check group membership
            updated_group = await access_service.get_group("dev_team")
            print(f"  Updated members: {list(updated_group.members)}")

            # Add user to admin group
            await access_service.add_user_to_group("admin_user", "admins")
            print(f"✓ Added 'admin_user' to admins group")

            # Show user group memberships
            print(f"\nUser group memberships:")
            for username, groups in access_service.user_groups.items():
                print(f"  {username}: {list(groups)}")

        finally:
            await access_service.stop()


async def demo_user_role_assignment():
    """Demonstrate user role assignment."""
    print("\n=== User Role Assignment Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Assigning roles to users...")

            # Assign roles to users
            users_and_roles = [
                ("alice", "admin"),
                ("bob", "user"),
                ("charlie", "readonly"),
                ("diana", "guest"),
            ]

            for username, role_name in users_and_roles:
                result = await access_service.assign_role_to_user(username, role_name)
                print(f"✓ Assigned '{role_name}' to '{username}': {result}")

            # Try to assign non-existent role
            try:
                await access_service.assign_role_to_user("eve", "nonexistent")
            except RoleNotFoundError as e:
                print(f"✓ Expected error for non-existent role: {e}")

            # Show user role assignments
            print(f"\nUser role assignments:")
            for username, roles in access_service.user_roles.items():
                print(f"  {username}: {list(roles)}")

            # Revoke role
            result = await access_service.revoke_role_from_user("diana", "guest")
            print(f"✓ Revoked 'guest' from 'diana': {result}")

        finally:
            await access_service.stop()


async def demo_permission_checking():
    """Demonstrate permission checking."""
    print("\n=== Permission Checking Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Setting up users with different permissions...")

            # Assign roles
            await access_service.assign_role_to_user("admin_user", "admin")
            await access_service.assign_role_to_user("regular_user", "user")
            await access_service.assign_role_to_user("readonly_user", "readonly")

            # Create group with specific role
            await access_service.create_group("editors", "Editor group", roles={"user"})
            await access_service.add_user_to_group("group_user", "editors")

            print("Checking permissions for different users...")

            users_to_test = [
                "admin_user",
                "regular_user",
                "readonly_user",
                "group_user",
                "unknown_user",
            ]
            permissions_to_test = [
                Permission.READ,
                Permission.WRITE,
                Permission.DELETE,
                Permission.ADMIN,
            ]

            for username in users_to_test:
                print(f"\n  {username}:")
                user_perms = await access_service.get_user_permissions(username)
                print(f"    Roles: {list(user_perms.roles)}")
                print(f"    Groups: {list(user_perms.groups)}")
                print(
                    f"    Effective permissions: {[p.value for p in user_perms.effective_permissions]}"
                )

                for permission in permissions_to_test:
                    has_perm = await access_service.check_permission(
                        username, permission
                    )
                    status = "✓" if has_perm else "✗"
                    print(f"    {status} {permission.value}")

            # Test require_permission
            print(f"\nTesting permission requirements...")
            try:
                await access_service.require_permission("admin_user", Permission.ADMIN)
                print(f"✓ Admin user has admin permission")
            except AccessDeniedError as e:
                print(f"✗ {e}")

            try:
                await access_service.require_permission(
                    "regular_user", Permission.ADMIN
                )
                print(f"✗ Should have failed")
            except AccessDeniedError as e:
                print(f"✓ Expected access denied: {e}")

        finally:
            await access_service.stop()


async def demo_access_control_lists():
    """Demonstrate Access Control Lists (ACLs)."""
    print("\n=== Access Control Lists Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Creating ACLs for specific resources...")

            # Create ACL for a file
            file_path = "/documents/secret.txt"
            acl = await access_service.create_acl(
                file_path, owner="alice", group="dev_team"
            )
            print(f"✓ Created ACL for: {acl.resource_path}")
            print(f"  Owner: {acl.owner}")
            print(f"  Group: {acl.group}")

            # Add specific user permissions
            await access_service.add_acl_entry(
                file_path,
                "bob",
                "user",
                {Permission.READ, Permission.WRITE},
                created_by="alice",
            )
            print(f"✓ Added ACL entry for user 'bob'")

            # Add group permissions
            await access_service.create_group("reviewers", "Document reviewers")
            await access_service.add_user_to_group("charlie", "reviewers")

            await access_service.add_acl_entry(
                file_path, "reviewers", "group", {Permission.READ}, created_by="alice"
            )
            print(f"✓ Added ACL entry for group 'reviewers'")

            # Add deny entry
            await access_service.add_acl_entry(
                file_path,
                "diana",
                "user",
                {Permission.READ, Permission.WRITE},
                AccessType.DENY,
                created_by="alice",
            )
            print(f"✓ Added DENY ACL entry for user 'diana'")

            # Show ACL entries
            acl = await access_service.get_acl(file_path)
            print(f"\nACL entries for {file_path}:")
            for i, entry in enumerate(acl.entries):
                perms = [p.value for p in entry.permissions]
                print(
                    f"  {i+1}. {entry.access_type.value.upper()} {entry.principal_type} '{entry.principal}': {perms}"
                )

            # Test resource-specific permissions
            print(f"\nTesting resource-specific permissions:")
            test_users = ["alice", "bob", "charlie", "diana", "eve"]

            for username in test_users:
                has_read = await access_service.check_permission(
                    username, Permission.READ, file_path
                )
                has_write = await access_service.check_permission(
                    username, Permission.WRITE, file_path
                )

                read_status = "✓" if has_read else "✗"
                write_status = "✓" if has_write else "✗"

                print(f"  {username}: {read_status} READ, {write_status} WRITE")

        finally:
            await access_service.stop()


async def demo_permission_inheritance():
    """Demonstrate permission inheritance."""
    print("\n=== Permission Inheritance Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Setting up hierarchical permissions...")

            # Create parent directory ACL
            parent_path = "/projects"
            await access_service.create_acl(parent_path, owner="admin")
            await access_service.add_acl_entry(
                parent_path, "developers", "group", {Permission.READ, Permission.WRITE}
            )
            print(f"✓ Created parent ACL for: {parent_path}")

            # Create child directory ACL with inheritance
            child_path = "/projects/secret_project"
            await access_service.create_acl(child_path, inherit_parent=True)
            await access_service.add_acl_entry(
                child_path, "alice", "user", {Permission.ADMIN}
            )
            print(f"✓ Created child ACL for: {child_path} (with inheritance)")

            # Create file ACL with inheritance
            file_path = "/projects/secret_project/config.txt"
            await access_service.create_acl(file_path, inherit_parent=True)
            print(f"✓ Created file ACL for: {file_path} (with inheritance)")

            # Create group and add user
            await access_service.create_group("developers", "Development team")
            await access_service.add_user_to_group("bob", "developers")

            # Test inherited permissions
            print(f"\nTesting permission inheritance:")

            test_resources = [parent_path, child_path, file_path]
            test_users = ["admin", "alice", "bob", "charlie"]

            for resource in test_resources:
                print(f"\n  Resource: {resource}")
                for username in test_users:
                    has_read = await access_service.check_permission(
                        username, Permission.READ, resource
                    )
                    has_write = await access_service.check_permission(
                        username, Permission.WRITE, resource
                    )
                    has_admin = await access_service.check_permission(
                        username, Permission.ADMIN, resource
                    )

                    permissions = []
                    if has_read:
                        permissions.append("READ")
                    if has_write:
                        permissions.append("WRITE")
                    if has_admin:
                        permissions.append("ADMIN")

                    perm_str = ", ".join(permissions) if permissions else "None"
                    print(f"    {username}: {perm_str}")

        finally:
            await access_service.stop()


async def demo_complex_scenario():
    """Demonstrate complex access control scenario."""
    print("\n=== Complex Scenario Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        access_service = AccessControlService(Path(temp_dir))
        await access_service.start()

        try:
            print("Setting up complex organizational structure...")

            # Create custom roles
            await access_service.create_role(
                "project_manager",
                "Project manager with oversight permissions",
                {
                    Permission.READ,
                    Permission.WRITE,
                    Permission.SHARE,
                    Permission.MODIFY_PERMISSIONS,
                },
            )

            await access_service.create_role(
                "senior_developer",
                "Senior developer with elevated access",
                {Permission.READ, Permission.WRITE, Permission.DELETE},
                parent_roles=["user"],
            )

            # Create organizational groups
            await access_service.create_group(
                "management", "Management team", roles={"project_manager", "admin"}
            )

            await access_service.create_group(
                "senior_devs", "Senior developers", roles={"senior_developer"}
            )

            await access_service.create_group(
                "junior_devs", "Junior developers", roles={"user"}
            )

            # Assign users to groups
            await access_service.add_user_to_group("alice", "management")
            await access_service.add_user_to_group("bob", "senior_devs")
            await access_service.add_user_to_group("charlie", "junior_devs")

            # Also assign direct roles
            await access_service.assign_role_to_user("diana", "readonly")

            # Create project structure with ACLs
            project_resources = [
                ("/company/projects", "alice", "management"),
                ("/company/projects/project_a", "bob", "senior_devs"),
                ("/company/projects/project_a/src", "bob", "senior_devs"),
                ("/company/projects/project_a/docs", "alice", "management"),
                ("/company/confidential", "alice", "management"),
            ]

            for resource_path, owner, group in project_resources:
                await access_service.create_acl(resource_path, owner=owner, group=group)
                print(
                    f"✓ Created ACL for {resource_path} (owner: {owner}, group: {group})"
                )

            # Add specific permissions
            await access_service.add_acl_entry(
                "/company/projects/project_a/src",
                "junior_devs",
                "group",
                {Permission.READ},
            )

            await access_service.add_acl_entry(
                "/company/confidential",
                "diana",
                "user",
                {Permission.READ, Permission.WRITE},
                AccessType.DENY,
            )

            print(f"\nOrganizational structure created!")
            print(f"  Roles: {len(access_service.roles)}")
            print(f"  Groups: {len(access_service.groups)}")
            print(f"  ACLs: {len(access_service.acls)}")

            # Test complex permissions
            print(f"\nTesting complex permission scenarios:")

            test_scenarios = [
                ("alice", "/company/projects", "Management access to projects"),
                (
                    "bob",
                    "/company/projects/project_a/src",
                    "Senior dev access to source",
                ),
                (
                    "charlie",
                    "/company/projects/project_a/src",
                    "Junior dev access to source",
                ),
                (
                    "diana",
                    "/company/confidential",
                    "Readonly user denied confidential access",
                ),
                ("alice", "/company/confidential", "Management access to confidential"),
            ]

            for username, resource, description in test_scenarios:
                print(f"\n  Scenario: {description}")
                print(f"    User: {username}, Resource: {resource}")

                user_perms = await access_service.get_user_permissions(username)
                print(f"    User roles: {list(user_perms.roles)}")
                print(f"    User groups: {list(user_perms.groups)}")

                permissions_to_check = [
                    Permission.READ,
                    Permission.WRITE,
                    Permission.DELETE,
                    Permission.ADMIN,
                ]
                results = []

                for perm in permissions_to_check:
                    has_perm = await access_service.check_permission(
                        username, perm, resource
                    )
                    if has_perm:
                        results.append(perm.value.upper())

                result_str = ", ".join(results) if results else "None"
                print(f"    Effective permissions: {result_str}")

        finally:
            await access_service.stop()


async def demo_persistence():
    """Demonstrate data persistence."""
    print("\n=== Persistence Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir)

        # First service instance
        print("Creating first service instance...")
        service1 = AccessControlService(data_dir)
        await service1.start()

        # Create test data
        await service1.create_role("test_role", "Test role", {Permission.READ})
        await service1.create_group("test_group", "Test group", {"user1"})
        await service1.assign_role_to_user("user1", "test_role")
        await service1.add_acl_entry("/test/file", "user1", "user", {Permission.WRITE})

        print(f"Created test data:")
        print(f"  Roles: {len(service1.roles)}")
        print(f"  Groups: {len(service1.groups)}")
        print(f"  User roles: {len(service1.user_roles)}")
        print(f"  User groups: {len(service1.user_groups)}")
        print(f"  ACLs: {len(service1.acls)}")

        await service1.stop()

        # Second service instance
        print(f"\nCreating second service instance...")
        service2 = AccessControlService(data_dir)
        await service2.start()

        try:
            print(f"Loaded data:")
            print(f"  Roles: {len(service2.roles)}")
            print(f"  Groups: {len(service2.groups)}")
            print(f"  User roles: {len(service2.user_roles)}")
            print(f"  User groups: {len(service2.user_groups)}")
            print(f"  ACLs: {len(service2.acls)}")

            # Verify functionality
            has_permission = await service2.check_permission("user1", Permission.READ)
            print(f"✓ User1 has read permission: {has_permission}")

            has_write_on_file = await service2.check_permission(
                "user1", Permission.WRITE, "/test/file"
            )
            print(f"✓ User1 has write permission on /test/file: {has_write_on_file}")

        finally:
            await service2.stop()


async def main():
    """Run all access control demos."""
    print("DittoFS Access Control System Demo")
    print("=" * 50)

    demos = [
        demo_default_roles,
        demo_role_management,
        demo_group_management,
        demo_user_role_assignment,
        demo_permission_checking,
        demo_access_control_lists,
        demo_permission_inheritance,
        demo_complex_scenario,
        demo_persistence,
    ]

    for demo in demos:
        try:
            await demo()
        except Exception as e:
            print(f"Demo failed: {e}")
        print()  # Add spacing between demos

    print("Access control demo completed!")


if __name__ == "__main__":
    asyncio.run(main())
