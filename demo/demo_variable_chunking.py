#!/usr/bin/env python3
"""
Demonstration of variable-size chunking algorithm improvements in DittoFS.

This script shows the differences between the old fixed-size chunking
and the new content-defined variable-size chunking.
"""

import pathlib
import tempfile

from src.dittofs.chunker import analyze_deduplication_ratio, get_chunk_stats, split


def create_test_files():
    """Create various test files to demonstrate chunking behavior."""
    files = {}

    # Text file with repetitive content
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
        content = []
        for i in range(100):
            content.append(f"This is line {i:03d} with some repetitive content.\n")
            if i % 10 == 0:
                content.append("=" * 50 + "\n")  # Section dividers
        f.write("".join(content).encode("utf-8"))
        files["text"] = pathlib.Path(f.name)

    # Binary file with patterns
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        # Create binary content with some patterns
        content = bytearray()
        for i in range(1000):
            content.extend(bytes([i % 256] * 100))  # Patterns every 100 bytes
        f.write(content)
        files["binary"] = pathlib.Path(f.name)

    # Large uniform file (like the old test)
    with tempfile.NamedTemporaryFile(suffix=".uniform", delete=False) as f:
        f.write(b"A" * (1024 * 1024))  # 1MB of 'A's
        files["uniform"] = pathlib.Path(f.name)

    # Mixed content file
    with tempfile.NamedTemporaryFile(suffix=".mixed", delete=False) as f:
        # Mix of text and binary
        f.write(b"Text section: " + b"Hello world! " * 100)
        f.write(b"\x00" * 1000)  # Binary section
        f.write(b"More text: " + b"Goodbye world! " * 100)
        files["mixed"] = pathlib.Path(f.name)

    return files


def demonstrate_chunking():
    """Demonstrate the variable-size chunking algorithm."""
    print("DittoFS Variable-Size Chunking Demonstration")
    print("=" * 50)

    files = create_test_files()

    try:
        for file_type, file_path in files.items():
            print(f"\n{file_type.upper()} FILE: {file_path.name}")
            print(f"File size: {file_path.stat().st_size:,} bytes")

            # Chunk the file
            hashes = split(file_path)
            stats = get_chunk_stats(hashes)

            print(f"Chunks created: {stats['count']}")
            print(f"Average chunk size: {stats['avg_size']:,} bytes")
            print(f"Min chunk size: {stats['min_size']:,} bytes")
            print(f"Max chunk size: {stats['max_size']:,} bytes")
            print("Size distribution:")
            for size_range, count in stats["size_distribution"].items():
                if count > 0:
                    print(f"  {size_range}: {count} chunks")

        # Demonstrate deduplication
        print(f"\nDEDUPLICATION ANALYSIS")
        print("-" * 30)

        # Create files with shared content
        shared_files = []
        shared_content = b"Shared content block " * 500  # 10KB shared

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(shared_content + b"Unique content A " * 500)
            shared_files.append(pathlib.Path(f.name))

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(shared_content + b"Unique content B " * 500)
            shared_files.append(pathlib.Path(f.name))

        dedup_stats = analyze_deduplication_ratio(shared_files)

        print(f"Total chunks across files: {dedup_stats['total_chunks']}")
        print(f"Unique chunks: {dedup_stats['unique_chunks']}")
        print(f"Duplicate chunks: {dedup_stats['duplicate_chunks']}")
        print(f"Original total size: {dedup_stats['original_size']:,} bytes")
        print(f"Deduplicated size: {dedup_stats['deduplicated_size']:,} bytes")
        print(f"Space saved: {dedup_stats['space_saved']:,} bytes")
        print(f"Deduplication ratio: {dedup_stats['deduplication_ratio']:.2%}")

        # Cleanup shared files
        for f in shared_files:
            f.unlink()

    finally:
        # Cleanup test files
        for file_path in files.values():
            file_path.unlink()


if __name__ == "__main__":
    demonstrate_chunking()
