#!/usr/bin/env python3
"""
Simple WebSocket test client for Voltra Phase 1.

Sends increment reducer calls and verifies responses.

Usage:
    python3 test_client.py

Requirements:
    pip install websocket-client msgpack
"""

import websocket
import msgpack
import json
import sys
import time
from typing import Dict, Any

class VoltraClient:
    def __init__(self, url: str = "ws://localhost:8000"):
        self.url = url
        self.ws = None
        self.call_id = 0
    
    def connect(self):
        """Connect to the Voltra server."""
        try:
            self.ws = websocket.create_connection(self.url)
            print(f"✓ Connected to {self.url}")
        except Exception as e:
            print(f"✗ Failed to connect: {e}")
            sys.exit(1)
    
    def disconnect(self):
        """Disconnect from the server."""
        if self.ws:
            self.ws.close()
            print("✓ Disconnected")
    
    def increment(self, name: str, delta: int) -> Dict[str, Any]:
        """
        Call the increment reducer.
        
        Args:
            name: Counter name
            delta: Amount to increment by
        
        Returns:
            Response dict with result
        """
        self.call_id += 1
        
        # Serialize arguments as MessagePack
        args = msgpack.packb({"name": name, "delta": delta})
        
        # Prepare reducer call
        call = {
            "call_id": self.call_id,
            "reducer_name": "increment",
            "args": args
        }
        
        # Send binary message
        binary_data = msgpack.packb(call)
        self.ws.send_binary(binary_data)
        
        # Receive and parse response
        raw_response = self.ws.recv()
        
        if isinstance(raw_response, bytes):
            response = msgpack.unpackb(raw_response, raw=False)
        else:
            response = json.loads(raw_response)
        
        return response
    
    def handle_response(self, response: Dict[str, Any]) -> bool:
        """
        Process and display a reducer response.
        
        Args:
            response: Response dict from server
        
        Returns:
            True if successful, False otherwise
        """
        call_id = response.get("call_id")
        success = response.get("success")
        
        if not success:
            error = response.get("error", "Unknown error")
            print(f"✗ Call #{call_id} failed: {error}")
            return False
        
        # Decode result
        result_bytes = response.get("result", b'')
        if result_bytes:
            try:
                if isinstance(result_bytes, list):
                    result_bytes = bytes(result_bytes)
                result = msgpack.unpackb(result_bytes, raw=False)
            except Exception as e:
                print(f"✗ Failed to decode result: {e}")
                return False
        else:
            result = {}
        
        print(f"✓ Call #{call_id} succeeded:")
        print(f"  new_value: {result.get('new_value')}")
        print(f"  timestamp: {result.get('timestamp')}")
        
        return True

def test_basic_increment():
    """Test basic increment operations."""
    print("\n=== Test 1: Basic Increment ===")
    client = VoltraClient()
    client.connect()
    
    try:
        # Increment a counter 5 times
        for i in range(5):
            response = client.increment("counter_1", 1)
            client.handle_response(response)
            time.sleep(0.1)
        
        print("\n✓ Test 1 passed!")
        return True
    except Exception as e:
        print(f"\n✗ Test 1 failed: {e}")
        return False
    finally:
        client.disconnect()

def test_multiple_counters():
    """Test incrementing multiple counters."""
    print("\n=== Test 2: Multiple Counters ===")
    client = VoltraClient()
    client.connect()
    
    try:
        # Increment different counters
        counters = ["score", "health", "mana"]
        for counter in counters:
            response = client.increment(counter, 10)
            client.handle_response(response)
            time.sleep(0.1)
        
        print("\n✓ Test 2 passed!")
        return True
    except Exception as e:
        print(f"\n✗ Test 2 failed: {e}")
        return False
    finally:
        client.disconnect()

def test_concurrent_calls():
    """Test multiple calls in sequence."""
    print("\n=== Test 3: Sequential Calls (100x) ===")
    client = VoltraClient()
    client.connect()
    
    try:
        success_count = 0
        for i in range(100):
            response = client.increment("stress_test", 1)
            if client.handle_response(response):
                success_count += 1
            
            if (i + 1) % 20 == 0:
                print(f"  ... {i + 1}/100 calls completed")
        
        print(f"\n✓ Test 3 passed! ({success_count}/100 successful)")
        return success_count == 100
    except Exception as e:
        print(f"\n✗ Test 3 failed: {e}")
        return False
    finally:
        client.disconnect()

def main():
    """Run all tests."""
    print("Voltra Phase 1 Test Client")
    print("===========================")
    
    results = []
    
    try:
        results.append(("Basic Increment", test_basic_increment()))
        results.append(("Multiple Counters", test_multiple_counters()))
        results.append(("Sequential Calls", test_concurrent_calls()))
    except KeyboardInterrupt:
        print("\n\n✗ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)
    
    # Print summary
    print("\n===========================")
    print("Test Summary:")
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {test_name}")
    
    all_passed = all(result for _, result in results)
    print("===========================")
    
    if all_passed:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
