#!/usr/bin/env python3
"""
Data Discovery System - UI Startup Script
Quick start script for your senior to launch the complete system
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    print("🔍 Checking dependencies...")
    
    try:
        import fastapi
        import uvicorn
        print("✅ FastAPI and Uvicorn are installed")
    except ImportError:
        print("❌ Missing dependencies. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn"])
        print("✅ Dependencies installed")

def create_directories():
    """Create required directories"""
    print("📁 Creating directories...")
    
    directories = [
        "ui",
        "ui/static",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    print("✅ Directories created")

def check_config():
    """Check if config file exists"""
    print("⚙️  Checking configuration...")
    
    if not Path("config.yaml").exists():
        print("❌ config.yaml not found!")
        return False
    
    print("✅ Configuration file found")
    return True

def start_system():
    """Start the Data Discovery System"""
    print("\n" + "="*60)
    print("🚀 STARTING DATA DISCOVERY SYSTEM")
    print("="*60)
    
    print("📊 Dashboard: http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("🔧 Interactive API: http://localhost:8000/redoc")
    print("="*60)
    
    # Import and run the web API
    try:
        from web_api import app
        import uvicorn
        
        uvicorn.run(
            "web_api:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\n🛑 System stopped by user")
    except Exception as e:
        print(f"❌ Error starting system: {e}")
        return False
    
    return True

def main():
    """Main startup function"""
    print("🔍 Data Discovery System - Enterprise UI")
    print("Initializing system...")
    
    # Check dependencies
    check_dependencies()
    
    # Create directories
    create_directories()
    
    # Check configuration
    if not check_config():
        print("\n⚠️  Please ensure config.yaml exists and is properly configured")
        print("Example: Update the GCP section with your BigQuery project ID")
        return
    
    # Start the system
    print("\n🎯 Starting web interface...")
    start_system()

if __name__ == "__main__":
    main()
