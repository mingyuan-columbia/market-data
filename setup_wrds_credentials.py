#!/usr/bin/env python3
"""Helper script to securely set up WRDS credentials in .pgpass file."""

import getpass
import os
import stat
from pathlib import Path

# Standard WRDS connection details
WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = "9737"
WRDS_DATABASE = "wrds"


def setup_pgpass():
    """Set up .pgpass file for WRDS credentials."""
    pgpass_path = Path.home() / ".pgpass"
    
    print("=" * 60)
    print("WRDS Credentials Setup")
    print("=" * 60)
    print()
    print("This script will help you securely store your WRDS credentials")
    print("in a .pgpass file. This file will be used automatically by the")
    print("WRDS Python library, so you won't need to enter credentials each time.")
    print()
    
    # Get username
    username = input("Enter your WRDS username: ").strip()
    if not username:
        print("Error: Username cannot be empty")
        return False
    
    # Get password (hidden input)
    password = getpass.getpass("Enter your WRDS password: ")
    if not password:
        print("Error: Password cannot be empty")
        return False
    
    # Create .pgpass entry
    # Format: hostname:port:database:username:password
    pgpass_entry = f"{WRDS_HOST}:{WRDS_PORT}:{WRDS_DATABASE}:{username}:{password}\n"
    
    # Check if .pgpass already exists
    if pgpass_path.exists():
        print(f"\nFound existing .pgpass file at {pgpass_path}")
        with open(pgpass_path, "r") as f:
            existing_content = f.read()
        
        # Check if WRDS entry already exists
        if WRDS_HOST in existing_content:
            print("\nWarning: An entry for WRDS already exists in .pgpass")
            response = input("Do you want to replace it? (y/n): ").strip().lower()
            if response != 'y':
                print("Cancelled. No changes made.")
                return False
            
            # Remove old WRDS entries
            lines = existing_content.strip().split("\n")
            new_lines = [line for line in lines if WRDS_HOST not in line]
            new_content = "\n".join(new_lines) + "\n" + pgpass_entry
        else:
            # Append to existing file
            new_content = existing_content.rstrip() + "\n" + pgpass_entry
    else:
        # Create new file
        new_content = pgpass_entry
    
    # Write .pgpass file
    try:
        with open(pgpass_path, "w") as f:
            f.write(new_content)
        
        # Set restrictive permissions (read/write for owner only)
        os.chmod(pgpass_path, stat.S_IRUSR | stat.S_IWUSR)
        
        print(f"\n✓ Successfully created/updated .pgpass file at {pgpass_path}")
        print(f"✓ File permissions set to 600 (read/write for owner only)")
        print()
        print("Your WRDS credentials are now stored securely.")
        print("You can now use the pipeline without entering credentials each time.")
        print()
        print("Note: Make sure to add your WRDS username to config.yaml:")
        print(f"  wrds_username: {username}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error writing .pgpass file: {e}")
        return False


if __name__ == "__main__":
    success = setup_pgpass()
    exit(0 if success else 1)

