#!/usr/bin/env python3
"""Fix session file database locks by enabling WAL mode."""

import sqlite3
import os
from config import Config

def fix_session_file():
    """Fix session file by enabling WAL mode and optimizing SQLite settings."""
    session_path = f"{Config.SESSION_NAME}.session"
    
    if not os.path.exists(session_path):
        print(f"Session file {session_path} not found.")
        return
    
    try:
        # Open the session file with SQLite
        conn = sqlite3.connect(session_path, timeout=30.0)
        
        # Enable WAL mode and optimize settings
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL") 
        conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=10000")
        
        conn.commit()
        conn.close()
        
        print(f"Successfully optimized session file: {session_path}")
        print("- Enabled WAL mode")
        print("- Set 30-second busy timeout")
        print("- Optimized synchronous mode")
        
    except Exception as e:
        print(f"Error fixing session file: {e}")

if __name__ == "__main__":
    fix_session_file()
