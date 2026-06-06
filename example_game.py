#!/usr/bin/env python3
"""Example NeonDB game demo using live subscriptions."""

import time
from client_sdk import NeonDBClient


def main():
    client = NeonDBClient(url="ws://localhost:3000", api_key=None)
    client.connect()
    print("Connected to NeonDB")

    print("Subscribing to player1 counter updates...")
    ack = client.subscribe("player1_score", "counters where row_key == player1")
    print("Subscription ack:", ack)

    print("Sending increment actions...")
    for i in range(3):
        response = client.increment("player1", 1)
        print(f"Increment response #{i+1}: {response}")
        diff = client.receive_diff()
        print(f"Subscription diff #{i+1}: {diff}")
        time.sleep(0.5)

    print("Unsubscribing...")
    unsub = client.unsubscribe("player1_score")
    print("Unsubscribe ack:", unsub)
    client.disconnect()
    print("Disconnected")


if __name__ == "__main__":
    main()
