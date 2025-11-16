#!/usr/bin/env python3
"""
Deploy Glue ETL scripts and packages to S3
This script:
1. Deletes old main_etl.py from S3
2. Uploads new main_etl.py
3. Deletes old glue_etl_package.zip from S3
4. Creates new zip from lib/ folder
5. Uploads new glue_etl_package.zip
"""

import boto3
import os
import zipfile
from pathlib import Path

# S3 Configuration
BUCKET_NAME = "intel-business-intelligence"
SCRIPT_PREFIX = "bi-database/scripts/job/"
PACKAGE_PREFIX = "bi-database/scripts/"
REGION = "sa-east-1"

# Local paths
SCRIPT_DIR = Path(__file__).parent
print(f"🔍 Script directory: {SCRIPT_DIR}")
MAIN_ETL_PATH = SCRIPT_DIR / "glue_etl" /"job" / "main_etl.py"
LIB_DIR = SCRIPT_DIR / "glue_etl" / "lib"
ZIP_OUTPUT = SCRIPT_DIR / "glue_etl"/ "glue_etl_package.zip"

def create_zip_from_lib():
    """Create a zip file containing all files from lib/ directory"""
    print(f"📦 Creating zip package from {LIB_DIR}...")
    
    with zipfile.ZipFile(ZIP_OUTPUT, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through lib directory
        for root, dirs, files in os.walk(LIB_DIR):
            for file in files:
                file_path = Path(root) / file
                # Calculate the archive name (relative to lib/)
                arcname = file_path.relative_to(LIB_DIR.parent)
                zipf.write(file_path, arcname)
                print(f"  ✓ Added: {arcname}")
    
    print(f"✅ Created {ZIP_OUTPUT} ({ZIP_OUTPUT.stat().st_size / 1024:.2f} KB)")
    return ZIP_OUTPUT

def deploy_to_s3():
    """Deploy files to S3 bucket"""
    print(f"\n🚀 Deploying to S3 bucket: {BUCKET_NAME}")
    
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=REGION)
    
    # 1. Delete old main_etl.py
    old_script_key = f"{SCRIPT_PREFIX}main_etl_bi_database.py"
    print(f"\n🗑️  Deleting old script: s3://{BUCKET_NAME}/{old_script_key}")
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_script_key)
        print(f"✅ Deleted old main_etl.py")
    except Exception as e:
        print(f"⚠️  Warning: Could not delete old script (may not exist): {e}")
    
    # 2. Upload new main_etl.py
    print(f"\n📤 Uploading new script: {MAIN_ETL_PATH}")
    try:
        s3_client.upload_file(
            str(MAIN_ETL_PATH),
            BUCKET_NAME,
            old_script_key
        )
        print(f"✅ Uploaded main_etl.py to s3://{BUCKET_NAME}/{old_script_key}")
    except Exception as e:
        print(f"❌ Error uploading script: {e}")
        raise
    
    # 3. Delete old glue_etl_package.zip
    old_package_key = f"{PACKAGE_PREFIX}glue_etl_package_bi_database.zip"
    print(f"\n🗑️  Deleting old package: s3://{BUCKET_NAME}/{old_package_key}")
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_package_key)
        print(f"✅ Deleted old glue_etl_package.zip")
    except Exception as e:
        print(f"⚠️  Warning: Could not delete old package (may not exist): {e}")
    
    # 4. Create new zip package
    zip_file = create_zip_from_lib()
    
    # 5. Upload new glue_etl_package.zip
    print(f"\n📤 Uploading new package: {zip_file}")
    try:
        s3_client.upload_file(
            str(zip_file),
            BUCKET_NAME,
            old_package_key
        )
        print(f"✅ Uploaded glue_etl_package.zip to s3://{BUCKET_NAME}/{old_package_key}")
    except Exception as e:
        print(f"❌ Error uploading package: {e}")
        raise
    
    print("\n" + "="*60)
    print("🎉 Deployment completed successfully!")
    print("="*60)
    print(f"\nDeployed files:")
    print(f"  • Script: s3://{BUCKET_NAME}/{old_script_key}")
    print(f"  • Package: s3://{BUCKET_NAME}/{old_package_key}")
    print(f"\nYou can verify in the AWS Console:")
    print(f"  https://sa-east-1.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}?prefix={SCRIPT_PREFIX}")

def main():
    """Main deployment function"""
    print("="*60)
    print("AWS Glue ETL Deployment Script")
    print("="*60)
    
    # Verify files exist
    if not MAIN_ETL_PATH.exists():
        print(f"❌ Error: {MAIN_ETL_PATH} not found!")
        return 1
    
    if not LIB_DIR.exists():
        print(f"❌ Error: {LIB_DIR} not found!")
        return 1
    
    print(f"✓ Found main_etl.py: {MAIN_ETL_PATH}")
    print(f"✓ Found lib directory: {LIB_DIR}")
    
    try:
        deploy_to_s3()
        return 0
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())