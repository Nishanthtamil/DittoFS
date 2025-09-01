#!/usr/bin/env python3
"""
Simple test to understand your pycrdt version
"""

import sys
import traceback

def test_doc_operations():
    """Test basic document operations without merge_updates"""
    print("Testing basic document operations...")
    
    try:
        import pycrdt as Y
        
        # Create document
        doc = Y.Doc()
        print("✓ Created document")
        
        # Add some data
        root = Y.Map()
        doc["root"] = root
        print("✓ Added root map")
        
        files_map = Y.Map()
        root["files"] = files_map
        print("✓ Added files map")
        
        # Add some test data
        files_map["test"] = {"path": "/test", "size": 100}
        print("✓ Added test data")
        
        # Try to get update
        try:
            update1 = doc.get_update()
            print(f"✓ Got update: {len(update1)} bytes")
            
            # Try to get state
            state1 = doc.get_state()
            print(f"✓ Got state: {len(state1)} bytes")
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to get update/state: {e}")
            return False
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False

def test_alternative_sync():
    """Test alternative ways to sync documents"""
    print("\nTesting alternative sync methods...")
    
    try:
        import pycrdt as Y
        
        # Create two documents
        doc1 = Y.Doc()
        doc2 = Y.Doc()
        
        # Add data to doc1
        root1 = Y.Map()
        doc1["root"] = root1
        root1["test"] = "hello"
        
        print("✓ Created doc1 with data")
        
        # Try to create update message
        try:
            # Look for alternative functions
            if hasattr(Y, 'create_update_message'):
                update_msg = Y.create_update_message(doc1)
                print(f"✓ create_update_message works: {len(update_msg)} bytes")
                
                # Try to handle the update message
                if hasattr(Y, 'handle_sync_message'):
                    # This might be the right way to sync
                    result = Y.handle_sync_message(update_msg, doc2)
                    print("✓ handle_sync_message works")
                    return True
                
        except Exception as e:
            print(f"✗ create_update_message failed: {e}")
        
        # Try get_update with state parameter
        try:
            state2 = doc2.get_state()
            update_for_doc2 = doc1.get_update(state2)
            print(f"✓ get_update(state) works: {len(update_for_doc2)} bytes")
            
            # Now try applying this update differently
            # Maybe there's a doc method to apply updates?
            if hasattr(doc2, 'apply_update'):
                doc2.apply_update(update_for_doc2)
                print("✓ doc.apply_update() works")
                return True
            elif hasattr(doc2, 'merge_update'):
                doc2.merge_update(update_for_doc2)
                print("✓ doc.merge_update() works")
                return True
                
        except Exception as e:
            print(f"✗ Alternative sync failed: {e}")
            
        return False
        
    except Exception as e:
        print(f"✗ Alternative sync test failed: {e}")
        traceback.print_exc()
        return False

def inspect_pycrdt_api():
    """Inspect the pycrdt API to understand available methods"""
    print("\nInspecting pycrdt API...")
    
    try:
        import pycrdt as Y
        
        # Create a document to inspect its methods
        doc = Y.Doc()
        print("Doc methods:", [m for m in dir(doc) if not m.startswith('_')])
        
        # Create a map to inspect its methods
        map_obj = Y.Map()
        print("Map methods:", [m for m in dir(map_obj) if not m.startswith('_')])
        
        # Look at module-level functions related to updates
        update_functions = [f for f in dir(Y) if 'update' in f.lower() or 'merge' in f.lower() or 'sync' in f.lower()]
        print("Update/sync functions:", update_functions)
        
        return True
        
    except Exception as e:
        print(f"✗ API inspection failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Simple pycrdt Test")
    print("=" * 30)
    
    tests = [
        test_doc_operations,
        inspect_pycrdt_api,
        test_alternative_sync
    ]
    
    for test in tests:
        try:
            if not test():
                print(f"Test {test.__name__} failed")
            print("-" * 30)
        except Exception as e:
            print(f"Test {test.__name__} crashed: {e}")
            traceback.print_exc()
            print("-" * 30)
    
    print("Done!")

if __name__ == "__main__":
    main()