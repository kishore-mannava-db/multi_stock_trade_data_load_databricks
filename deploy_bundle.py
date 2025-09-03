#!/usr/bin/env python3
"""
Deploy Trade Data Generator using Databricks Asset Bundles
This script deploys the solution to your Databricks workspace
"""

import subprocess
import sys
import os
from pathlib import Path
import json

def check_databricks_cli():
    """Check if Databricks CLI is installed and configured"""
    try:
        result = subprocess.run(["databricks", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ Databricks CLI found: {result.stdout.strip()}")
            return True
        else:
            print("❌ Databricks CLI not working properly")
            return False
    except FileNotFoundError:
        print("❌ Databricks CLI not found")
        print("Install with: pip install databricks-cli")
        return False

def check_bundle_config():
    """Check if bundle configuration is valid"""
    bundle_file = Path("databricks.yml")
    if not bundle_file.exists():
        print("❌ Bundle configuration file not found: databricks.yml")
        return False
    
    print("✓ Bundle configuration file found: databricks.yml")
    return True

def validate_bundle():
    """Validate the bundle configuration"""
    print("\n🔍 Validating bundle configuration...")
    
    try:
        result = subprocess.run(["databricks", "bundle", "validate"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ Bundle validation passed")
            return True
        else:
            print("❌ Bundle validation failed:")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ Bundle validation error: {str(e)}")
        return False

def deploy_bundle(target="dev"):
    """Deploy the bundle to the specified target"""
    print(f"\n🚀 Deploying bundle to target: {target}")
    
    try:
        # Deploy the bundle
        result = subprocess.run(
            ["databricks", "bundle", "deploy", "--target", target],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✓ Bundle deployed successfully!")
            print("\nDeployment output:")
            print(result.stdout)
            return True
        else:
            print("❌ Bundle deployment failed:")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Deployment error: {str(e)}")
        return False

def run_job():
    """Run the deployed job"""
    print("\n▶️  Running the deployed job...")
    
    try:
        # Run the job
        result = subprocess.run(
            ["databricks", "jobs", "run-now", "--job-id", "{{job_id}}"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✓ Job started successfully!")
            print("\nJob output:")
            print(result.stdout)
            return True
        else:
            print("❌ Job execution failed:")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Job execution error: {str(e)}")
        return False

def show_deployment_info():
    """Show information about the deployment"""
    print("\n📋 Deployment Information")
    print("=" * 40)
    
    print("Bundle Configuration:")
    print("  • Name: trade-data-generator")
    print("  • Target: dev (default)")
    print("  • Resources: Jobs, Notebooks")
    
    print("\nDeployed Resources:")
    print("  • Job: Trade Data Generator Job")
    print("  • Schedule: Every hour (0 0 * * * ?)")
    print("  • Notebook: ./src/trade_data_generator")
    
    print("\nJob Parameters:")
    print("  • rows_per_exchange: 500")
    print("  • exchange_filter: all")
    
    print("\nData Storage:")
    print("  • Base path: /dbfs/trade_data")
    print("  • Structure: /{exchange}/bronze/")
    print("  • Format: CSV files with timestamps")

def create_quick_start_guide():
    """Create a quick start guide for the deployed solution"""
    guide = """# Quick Start Guide - Asset Bundle Deployment

## 🎯 What Was Deployed

The Asset Bundle has deployed the following resources to your Databricks workspace:

### Resources Created
- **Job**: Trade Data Generator Job
- **Notebook**: trade_data_generator
- **Schedule**: Runs every hour automatically

### Job Configuration
- **Name**: Trade Data Generator Job
- **Schedule**: Cron expression `0 0 * * * ?` (every hour)
- **Parameters**: 
  - rows_per_exchange: 500
  - exchange_filter: all

## 🚀 How to Use

### 1. Monitor Job Execution
```bash
# Check job status
databricks jobs list

# View job runs
databricks jobs runs-list --job-id <job_id>

# Get job details
databricks jobs get --job-id <job_id>
```

### 2. View Generated Data
```python
# In a Databricks notebook
# List generated files
dbutils.fs.ls("/dbfs/trade_data/")

# Read CSV files
df = spark.read.option("header", "true").csv("/dbfs/trade_data/*/bronze/*.csv")
df.show(5)
```

### 3. Customize Job Parameters
```bash
# Update job parameters
databricks jobs reset --job-id <job_id> --new-settings '{
  "base_parameters": {
    "rows_per_exchange": "1000",
    "exchange_filter": "NYSE"
  }
}'
```

### 4. Manual Job Execution
```bash
# Run job manually
databricks jobs run-now --job-id <job_id> --notebook-params '{
  "rows_per_exchange": "1000",
  "exchange_filter": "NASDAQ"
}'
```

## 📁 Data Structure

Generated data is stored in:
```
/dbfs/trade_data/
├── nyse/bronze/
│   ├── trades_nyse_20241201_090000.csv
│   └── incremental_nyse_20241201_100000.csv
├── nasdaq/bronze/
│   ├── trades_nasdaq_20241201_090000.csv
│   └── incremental_nasdaq_20241201_100000.csv
└── lse/bronze/
    ├── trades_lse_20241201_090000.csv
    └── incremental_lse_20241201_100000.csv
```

## 🔧 Troubleshooting

### Common Issues
1. **Job not running**: Check job status and schedule
2. **Permission errors**: Verify workspace access
3. **Data not generated**: Check job logs and parameters

### Debug Commands
```bash
# Check bundle status
databricks bundle info

# Validate bundle
databricks bundle validate

# View deployment logs
databricks bundle logs
```

## 📚 Next Steps

1. **Monitor**: Watch job execution and data generation
2. **Customize**: Adjust parameters and schedules as needed
3. **Integrate**: Use generated data in your data pipelines
4. **Scale**: Deploy to production environment

## 🆘 Support

- **Bundle Issues**: Check bundle validation and deployment logs
- **Job Issues**: Monitor job execution and logs
- **Data Issues**: Verify file permissions and storage paths
"""
    
    guide_file = Path("ASSET_BUNDLE_QUICK_START.md")
    with open(guide_file, 'w') as f:
        f.write(guide)
    
    print(f"✓ Quick start guide created: {guide_file}")

def main():
    """Main deployment function"""
    
    print("🎯 Databricks Asset Bundle Deployment")
    print("=" * 50)
    
    try:
        # Check prerequisites
        if not check_databricks_cli():
            print("\n❌ Please install and configure Databricks CLI first")
            return 1
        
        if not check_bundle_config():
            print("\n❌ Bundle configuration not found")
            return 1
        
        # Validate bundle
        if not validate_bundle():
            print("\n❌ Bundle validation failed")
            return 1
        
        # Deploy bundle
        if not deploy_bundle("dev"):
            print("\n❌ Bundle deployment failed")
            return 1
        
        # Show deployment information
        show_deployment_info()
        
        # Create quick start guide
        create_quick_start_guide()
        
        print("\n🎉 Asset Bundle deployment completed successfully!")
        print("\n📋 Next steps:")
        print("  1. Check your Databricks workspace for the deployed resources")
        print("  2. Monitor the job execution")
        print("  3. View generated data in DBFS")
        print("  4. Customize parameters as needed")
        
        print(f"\n📚 Documentation:")
        print(f"   • ASSET_BUNDLE_QUICK_START.md - Quick start guide")
        print(f"   • databricks.yml - Bundle configuration")
        print(f"   • src/trade_data_generator.py - Source notebook")
        
        print(f"\n🔗 Useful Commands:")
        print(f"   • databricks bundle info - Check bundle status")
        print(f"   • databricks jobs list - List deployed jobs")
        print(f"   • databricks bundle logs - View deployment logs")
        
    except Exception as e:
        print(f"\n❌ Deployment failed: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
