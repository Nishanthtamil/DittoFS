#!/usr/bin/env python3
"""
Test script to debug pycrdt issues
Run this to verify pycrdt is working correctly
"""

import sys
import traceback


def test_pycrdt_basic():
    """Test basic pycrdt functionality"""
    print("Testing basic pycrdt functionality...")

    try:
        import pycrdt as Y

        print(f"✓ pycrdt imported successfully")
        print(f"  Available functions: {[x for x in dir(Y) if not x.startswith('_')]}")

        # Test document creation
        doc = Y.Doc()
        print("✓ Created Y.Doc()")

        # Test map creation
        root_map = Y.Map()
        doc["root"] = root_map
        print("✓ Created and assigned root map")

        # Test nested maps
        files_map = Y.Map()
        chunks_map = Y.Map()
        root_map["files"] = files_map
        root_map["chunks"] = chunks_map
        print("✓ Created nested maps")

        # Test basic operations
        files_map["test_file"] = {"path": "/test", "size": 100}
        chunks_map["test_chunk"] = ["file1", "file2"]
        print("✓ Basic map operations work")

        # Test serialization
        try:
            update_data = doc.get_update()
            print(f"✓ doc.get_update() works, size: {len(update_data)} bytes")
        except Exception as e:
            print(f"✗ doc.get_update() failed: {e}")
            return False

        # Test state vector
        try:
            state = doc.get_state()
            print(f"✓ doc.get_state() works, size: {len(state)} bytes")
        except Exception as e:
            print(f"✗ doc.get_state() failed: {e}")
            return False

        # Test apply_update (which works with your version)
        try:
            new_doc = Y.Doc()
            new_doc.apply_update(update_data)
            print("✓ doc.apply_update() works")
        except Exception as e:
            print(f"✗ doc.apply_update() failed: {e}")
            traceback.print_exc()
            return False

        print("✓ All basic pycrdt tests passed!")
        return True

    except ImportError as e:
        print(f"✗ Failed to import pycrdt: {e}")
        print("Try: pip install pycrdt")
        return False
    except Exception as e:
        print(f"✗ pycrdt test failed: {e}")
        traceback.print_exc()
        return False


def test_crdt_store():
    """Test the CRDTStore class"""
    print("\nTesting CRDTStore class...")

    try:
        import pathlib
        import tempfile

        from .crdt_store import CRDTStore

        # Create temporary store
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = pathlib.Path(tmpdir) / "test_store.yrs"

            # Test store creation
            store = CRDTStore(store_path)
            print("✓ CRDTStore created successfully")

            # Test save/load cycle
            store.save()
            print("✓ Store saved successfully")

            # Create new store instance with same path
            store2 = CRDTStore(store_path)
            print("✓ Store loaded successfully")

            print("✓ CRDTStore tests passed!")
            return True

    except Exception as e:
        print(f"✗ CRDTStore test failed: {e}")
        traceback.print_exc()
        return False


def test_file_operations():
    """Test file operations with a real file"""
    print("\nTesting file operations...")

    try:
        import pathlib
        import tempfile

        from src.dittofs.crdt_store import CRDTStore

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = pathlib.Path(tmpdir)
            store_path = tmpdir_path / "test_store.yrs"

            # Create test file
            test_file = tmpdir_path / "test.txt"
            test_file.write_text("Hello, DittoFS!")

            # Create store and add file
            store = CRDTStore(store_path)

            # Test adding file (with mock hashes)
            test_hashes = ["hash1", "hash2", "hash3"]
            success = store.add_file(test_file, test_hashes)

            if success:
                print("✓ File added to store successfully")

                # Test retrieving file
                file_record = store.get_file(test_file)
                if file_record:
                    print(f"✓ File retrieved: {file_record.path}")
                    print(f"  Size: {file_record.size}")
                    print(f"  Hashes: {len(file_record.hashes)}")
                else:
                    print("✗ Failed to retrieve file")
                    return False

                # Test listing files
                files = store.list_files()
                if files:
                    print(f"✓ Listed {len(files)} files")
                else:
                    print("✗ No files listed")
                    return False

            else:
                print("✗ Failed to add file to store")
                return False

            print("✓ File operations tests passed!")
            return True

    except Exception as e:
        print(f"✗ File operations test failed: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("DittoFS pycrdt Debug Tests")
    print("=" * 40)

    tests = [test_pycrdt_basic, test_crdt_store, test_file_operations]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                break  # Stop on first failure
        except Exception as e:
            print(f"✗ Test {test.__name__} crashed: {e}")
            traceback.print_exc()
            break

    print("\n" + "=" * 40)
    print(f"Tests passed: {passed}/{len(tests)}")

    if passed == len(tests):
        print("✓ All tests passed! Your pycrdt setup should work.")
        return 0
    else:
        print("✗ Some tests failed. Check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
