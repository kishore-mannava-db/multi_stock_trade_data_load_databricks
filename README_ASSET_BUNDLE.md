# Databricks Asset Bundle - Trade Data Generator

This project uses **Databricks Asset Bundles** to deploy a complete trade data generation solution to your Databricks workspace. Asset Bundles provide infrastructure-as-code capabilities for Databricks, making deployment and management simple and repeatable.

## 🚀 What Are Asset Bundles?

**Databricks Asset Bundles** are a modern way to deploy and manage Databricks resources using YAML configuration files. They provide:

- **Infrastructure as Code**: Version-controlled resource definitions
- **Multi-Environment Support**: Dev, staging, and production deployments
- **Automated Deployment**: Simple CLI commands for deployment
- **Resource Management**: Automatic creation and updates of Databricks resources
- **Best Practices**: Enforced patterns and configurations

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Asset Bundle  │    │   Databricks    │    │   Generated     │
│   Configuration │───▶│   Workspace     │───▶│   Trade Data    │
│                 │    │                 │    │                 │
│ • YAML Config  │    │ • Jobs          │    │ • CSV Files     │
│ • Source Code  │    │ • Notebooks     │    │ • Bronze Layer  │
│ • Parameters   │    │ • Schedules     │    │ • Timestamps    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 Project Structure

```
trade-data-generator/
├── databricks.yml                    # Asset Bundle configuration
├── src/
│   └── trade_data_generator.py      # Source notebook
├── deploy_bundle.py                  # Deployment script
├── README_ASSET_BUNDLE.md           # This file
└── ASSET_BUNDLE_QUICK_START.md      # Quick start guide
```

## 🛠️ Prerequisites

### 1. **Databricks CLI Installation**
```bash
# Install Databricks CLI
pip install databricks-cli

# Verify installation
databricks --version
```

### 2. **Databricks Authentication**
```bash
# Configure authentication
databricks configure --token

# Enter your workspace URL and personal access token
# Workspace URL: https://your-workspace.cloud.databricks.com
# Personal Access Token: dapi_xxxxxxxxxxxxxxxxxxxx
```

### 3. **Workspace Access**
- **Admin** or **Workspace Admin** permissions
- **Can Manage** permissions for Jobs, Notebooks, and other resources
- **Unity Catalog** access (if using catalog features)

## 🔧 Configuration

### **Bundle Configuration (`databricks.yml`)**

The main configuration file defines all resources:

```yaml
bundle:
  name: trade-data-generator

targets:
  dev:
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com

  prod:
    workspace:
      host: https://your-prod-workspace.cloud.databricks.com

resources:
  jobs:
    trade-data-generator-job:
      name: Trade Data Generator Job
      tasks:
        - task_key: generate_trade_data
          notebook_task:
            notebook_path: ./src/trade_data_generator
            base_parameters:
              rows_per_exchange: "500"
              exchange_filter: "all"
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: "UTC"
```

### **Environment-Specific Settings**

You can customize settings for different environments:

```yaml
targets:
  dev:
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    variables:
      environment: development
      data_volume: small
  
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    variables:
      environment: production
      data_volume: large
```

## 🚀 Deployment

### **1. Quick Deployment**
```bash
# Deploy to default target (dev)
python3 deploy_bundle.py
```

### **2. Manual Deployment**
```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to specific target
databricks bundle deploy --target dev

# Deploy to production
databricks bundle deploy --target prod
```

### **3. Deployment Verification**
```bash
# Check bundle status
databricks bundle info

# List deployed resources
databricks jobs list
databricks workspace list
```

## 📊 What Gets Deployed

### **Resources Created**

1. **Job**: Trade Data Generator Job
   - **Schedule**: Runs every hour automatically
   - **Parameters**: Configurable rows and exchange filters
   - **Timeout**: 1 hour execution limit

2. **Notebook**: trade_data_generator
   - **Location**: /Shared/Trade Data Generator/
   - **Language**: Python
   - **Dependencies**: dbldatagen

3. **Data Storage**: DBFS directories
   - **Base path**: /dbfs/trade_data/
   - **Structure**: /{exchange}/bronze/
   - **Format**: Timestamped CSV files

### **Job Configuration**

```yaml
jobs:
  trade-data-generator-job:
    name: Trade Data Generator Job
    tasks:
      - task_key: generate_trade_data
        description: Generate incremental trade data
        notebook_task:
          notebook_path: ./src/trade_data_generator
          base_parameters:
            rows_per_exchange: "500"
            exchange_filter: "all"
        timeout_seconds: 3600
    schedule:
      quartz_cron_expression: "0 0 * * * ?"  # Every hour
      timezone_id: "UTC"
```

## 🔄 Job Execution

### **Automatic Execution**
- **Schedule**: Every hour at minute 0
- **Parameters**: 500 rows per exchange, all exchanges
- **Output**: CSV files in DBFS with timestamps

### **Manual Execution**
```bash
# Run job immediately
databricks jobs run-now --job-id <job_id>

# Run with custom parameters
databricks jobs run-now --job-id <job_id> --notebook-params '{
  "rows_per_exchange": "1000",
  "exchange_filter": "NYSE"
}'
```

### **Parameter Customization**
```python
# In the notebook, parameters are accessed via widgets
dbutils.widgets.text("rows_per_exchange", "1000")
dbutils.widgets.text("exchange_filter", "all")

rows_per_exchange = int(dbutils.widgets.get("rows_per_exchange"))
exchange_filter = dbutils.widgets.get("exchange_filter")
```

## 📁 Data Output

### **File Structure**
```
/dbfs/trade_data/
├── nyse/bronze/
│   ├── trades_nyse_20241201_090000.csv
│   ├── incremental_nyse_20241201_100000.csv
│   └── incremental_nyse_20241201_110000.csv
├── nasdaq/bronze/
│   ├── trades_nasdaq_20241201_090000.csv
│   └── incremental_nasdaq_20241201_100000.csv
└── lse/bronze/
    ├── trades_lse_20241201_090000.csv
    └── incremental_lse_20241201_100000.csv
```

### **Data Schema**
Each CSV file contains:
- `trade_id`: Unique identifier
- `symbol`: Stock symbol
- `exchange`: Exchange name
- `price`: Trade price
- `quantity`: Number of shares
- `timestamp`: Trade timestamp
- `trade_type`: BUY/SELL/SHORT/COVER
- `market_center`: Market identifier
- `ingestion_timestamp`: Generation time
- `file_path`: Simulated file path

## 🔧 Customization

### **1. Modify Exchange Configuration**
```python
# In src/trade_data_generator.py
EXCHANGES = {
    'NYSE': {
        'symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'price_ranges': {
            'AAPL': (150.0, 200.0),
            'MSFT': (300.0, 400.0),
            # Add more symbols
        }
    },
    # Add more exchanges
}
```

### **2. Adjust Generation Parameters**
```python
GENERATION_CONFIG = {
    "default_rows_per_exchange": 1000,      # Initial data size
    "incremental_rows_per_exchange": 500,   # Incremental batch size
    "max_partitions": 4                     # Spark partitions
}
```

### **3. Change Schedule**
```yaml
# In databricks.yml
schedule:
  quartz_cron_expression: "0 */30 * * * ?"  # Every 30 minutes
  timezone_id: "UTC"
```

### **4. Add New Parameters**
```python
# Add new widget
dbutils.widgets.text("custom_parameter", "default_value")

# Use in code
custom_value = dbutils.widgets.get("custom_parameter")
```

## 📈 Monitoring and Management

### **Job Monitoring**
```bash
# Check job status
databricks jobs list

# View job runs
databricks jobs runs-list --job-id <job_id>

# Get job details
databricks jobs get --job-id <job_id>
```

### **Data Monitoring**
```python
# In Databricks notebook
# Check file counts
import os
for exchange in ['nyse', 'nasdaq', 'lse']:
    path = f"/dbfs/trade_data/{exchange}/bronze"
    files = os.listdir(path)
    print(f"{exchange}: {len(files)} files")

# Check data quality
df = spark.read.option("header", "true").csv("/dbfs/trade_data/*/bronze/*.csv")
print(f"Total records: {df.count()}")
print(f"Unique symbols: {df.select('symbol').distinct().count()}")
```

### **Bundle Management**
```bash
# Check bundle status
databricks bundle info

# Validate changes
databricks bundle validate

# Deploy updates
databricks bundle deploy --target dev

# View deployment logs
databricks bundle logs
```

## 🔄 CI/CD Integration

### **GitHub Actions Example**
```yaml
name: Deploy to Databricks
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v3
        with:
          python-version: '3.9'
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Configure Databricks
        run: |
          databricks configure --token <<EOF
          ${{ secrets.DATABRICKS_HOST }}
          ${{ secrets.DATABRICKS_TOKEN }}
          EOF
      
      - name: Deploy Bundle
        run: databricks bundle deploy --target prod
```

### **Environment Variables**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_xxxxxxxxxxxxxxxxxxxx"
```

## 🚨 Troubleshooting

### **Common Issues**

#### **1. Authentication Errors**
```bash
# Reconfigure authentication
databricks configure --token

# Check token validity
databricks workspace list
```

#### **2. Bundle Validation Failures**
```bash
# Validate bundle
databricks bundle validate

# Check YAML syntax
python -c "import yaml; yaml.safe_load(open('databricks.yml'))"
```

#### **3. Deployment Failures**
```bash
# Check bundle status
databricks bundle info

# View deployment logs
databricks bundle logs

# Retry deployment
databricks bundle deploy --target dev
```

#### **4. Job Execution Issues**
```bash
# Check job status
databricks jobs list

# View job logs
databricks jobs runs-list --job-id <job_id>

# Check notebook path
databricks workspace list --path /Shared/Trade\ Data\ Generator/
```

### **Debug Commands**
```bash
# Bundle debugging
databricks bundle validate --debug
databricks bundle deploy --target dev --debug

# Job debugging
databricks jobs get --job-id <job_id> --output JSON
databricks jobs runs-get --run-id <run_id> --output JSON
```

## 📚 Best Practices

### **1. Environment Management**
- Use separate targets for dev, staging, and production
- Keep sensitive configuration in environment variables
- Use consistent naming conventions across environments

### **2. Resource Organization**
- Group related resources in the same bundle
- Use descriptive names for jobs and notebooks
- Tag resources for better organization

### **3. Version Control**
- Commit bundle configuration to version control
- Use semantic versioning for releases
- Document configuration changes

### **4. Testing**
- Validate bundle configuration before deployment
- Test in development environment first
- Use parameterized testing for different scenarios

### **5. Monitoring**
- Set up job monitoring and alerting
- Track data generation metrics
- Monitor resource usage and costs

## 🔗 Integration Examples

### **Delta Live Tables (DLT)**
```python
# Create DLT table from generated CSV files
@dlt.table
def bronze_trades():
    return spark.read.option("header", "true").csv("/dbfs/trade_data/*/bronze/*.csv")

@dlt.table
def silver_trades():
    return dlt.read("bronze_trades").filter(col("price") > 0)
```

### **Structured Streaming**
```python
# Read from bronze directory as streaming source
streaming_df = spark.readStream \
    .option("header", "true") \
    .csv("/dbfs/trade_data/*/bronze/*.csv")

# Process streaming data
query = streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/dbfs/checkpoints/streaming") \
    .table("processed_trades")
```

### **Data Quality Monitoring**
```python
# Monitor data quality
def check_data_quality(df):
    total_rows = df.count()
    valid_prices = df.filter(col("price") > 0).count()
    quality_score = valid_prices / total_rows
    
    return {
        "total_rows": total_rows,
        "valid_prices": valid_prices,
        "quality_score": quality_score
    }

# Apply to generated data
quality_metrics = check_data_quality(sample_df)
print(json.dumps(quality_metrics, indent=2))
```

## 📚 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/index.html)
- [Databricks Jobs API](https://docs.databricks.com/api/workspace/jobs)
- [dbldatagen GitHub](https://github.com/databrickslabs/dbldatagen)
- [Databricks Best Practices](https://docs.databricks.com/best-practices/index.html)

## 🤝 Support

For issues and questions:
1. **Check the troubleshooting section above**
2. **Review Databricks documentation**
3. **Check bundle validation and deployment logs**
4. **Contact Databricks support** (if you have support plan)

## 🎉 Benefits of Asset Bundles

1. **Infrastructure as Code**: Version-controlled, repeatable deployments
2. **Multi-Environment**: Easy deployment to dev, staging, and production
3. **Automated Management**: Automatic resource creation and updates
4. **Best Practices**: Enforced patterns and configurations
5. **CI/CD Integration**: Seamless integration with modern development workflows
6. **Scalability**: Easy to extend and modify as requirements grow

---

**Happy Data Generation with Databricks Asset Bundles! 🚀**
