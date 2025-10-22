"""
Quick Setup and Test Script
Install dependencies and run a test of the pipeline
"""

import subprocess
import sys
import os

def main():
    print("="*60)
    print("PRODUCT PRICE TRACKER - SETUP")
    print("="*60)
    
    # Check Python version
    print(f"\n✓ Python version: {sys.version}")
    
    # Install dependencies
    print("\n Installing dependencies...")
    print("-" * 60)
    
    try:
        subprocess.check_call([
            sys.executable, 
            "-m", 
            "pip", 
            "install", 
            "-r", 
            "requirements.txt",
            "--quiet"
        ])
        print("✓ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False
    
    # Create data directory
    print("\n Creating data directory...")
    os.makedirs("data", exist_ok=True)
    print("✓ Data directory ready")
    
    # Run pipeline test
    print("\n Running pipeline test...")
    print("=" * 60)
    
    try:
        from run_pipeline import run_pipeline
        success = run_pipeline()
        
        if success:
            print("\n" + "="*60)
            print("✓ SETUP COMPLETE!")
            print("="*60)
            print("\n📚 Next steps:")
            print("  1. Check the data/ folder for generated files")
            print("  2. Run 'python run_pipeline.py' anytime to update prices")
            print("  3. Read README.md for more information")
            print("  4. Optional: Set up Airflow for automated scheduling")
            return True
        else:
            print("\n Pipeline test failed")
            return False
            
    except Exception as e:
        print(f"\n Error running pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
