#!/usr/bin/env python3
"""Test storage channel access and create if needed."""

import asyncio
import sys
from telethon import TelegramClient, functions
from telethon.errors import ChannelPrivateError, PeerIdInvalidError
from config import Config

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    """Test storage channel access."""
    print("=== Storage Channel Test ===\n")
    
    client = TelegramClient(Config.SESSION_NAME, Config.API_ID, Config.API_HASH)
    
    try:
        await client.start()
        me = await client.get_me()
        print(f"Connected as: {me.first_name} (@{me.username})\n")
        
        # Test current storage channel
        try:
            channel = await client.get_entity(Config.STORAGE_CHANNEL_ID)
            print(f"✓ Storage channel found: {channel.title}")
            print(f"  - ID: {channel.id}")
            print(f"  - Access hash: {channel.access_hash}")
            
            # Try to send a test message
            try:
                test_msg = await client.send_message(channel, "Test message from bot")
                print("✓ Can send messages to channel")
                await client.delete_messages(channel, test_msg)
                print("✓ Can delete messages from channel")
                
                await client.disconnect()
                return True
                
            except Exception as e:
                print(f"✗ Cannot send messages to channel: {e}")
                print("Make sure the userbot account is admin in the channel")
                
        except (ChannelPrivateError, PeerIdInvalidError, ValueError) as e:
            print(f"✗ Cannot access storage channel {Config.STORAGE_CHANNEL_ID}: {e}")
            print("\nLet's create a new storage channel...")
            
            # Create new private channel
            result = await client(
                functions.channels.CreateChannelRequest(
                    title="Video Storage Bot",
                    about="Private channel for storing downloaded videos",
                    megagroup=False  # Create a channel, not a supergroup
                )
            )
            
            channel = result.chats[0]
            channel_id = f"-100{channel.id}"
            
            print(f"✓ Created new storage channel: {channel.title}")
            print(f"  - ID: {channel_id}")
            print(f"  - Access hash: {channel.access_hash}")
            print(f"\nPlease update STORAGE_CHANNEL_ID in .env file to: {channel_id}")
            
        await client.disconnect()
        return False
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n[ERROR] Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Script error: {e}")
        sys.exit(1)
