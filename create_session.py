#!/usr/bin/env python3
"""Temporary session creator to fix authentication issues."""

import asyncio
import sys
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from config import Config

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    """Create new session file."""
    print("=== Session Creator ===\n")
    
    if not Config.API_ID or not Config.API_HASH:
        print("[ERROR] API_ID and API_HASH are required in .env file")
        return
    
    # Remove any existing session files
    session_file = f"{Config.SESSION_NAME}.session"
    if os.path.exists(session_file):
        os.remove(session_file)
        print(f"Removed existing session file: {session_file}")
    
    # Create fresh client
    client = TelegramClient(Config.SESSION_NAME, Config.API_ID, Config.API_HASH)
    
    try:
        await client.start()
        
        if not await client.is_user_authorized():
            phone = input("Enter your phone number (with country code, e.g., +998901234567): ")
            await client.send_code_request(phone)
            
            code = input("Enter the verification code you received: ")
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                password = input("Enter your 2FA password: ")
                await client.sign_in(password=password)
        
        me = await client.get_me()
        print(f"\n[SUCCESS] Authenticated as {me.first_name} (@{me.username})")
        print(f"Session file created: {session_file}")
        
        await client.disconnect()
        return True
        
    except Exception as e:
        print(f"[ERROR] Authentication failed: {e}")
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
