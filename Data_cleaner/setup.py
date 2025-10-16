#!/usr/bin/env python3
"""
Setup script for Data Cleaner dependencies
"""
import subprocess
import sys

def install_requirements():
    """Install all required packages"""
    requirements = [
        'pandas>=1.5.0',
        'sqlalchemy>=1.4.0', 
        'scikit-learn>=1.2.0',
        'numpy>=1.21.0',
        'scipy>=1.7.0'
    ]
    
    print("📦 Installing dependencies...")
    for package in requirements:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError:
            print(f"❌ Failed to install {package}")
    
    print("🎉 All dependencies installed!")

if __name__ == "__main__":
    install_requirements()