# Configuration-Driven DLT Pipeline with Auto Loader & AutoCDC

## 🚀 Overview

This project implements a **Delta Live Tables (DLT) pipeline** with **Databricks Auto Loader** and **AutoCDC (Change Data Capture)** functionality for processing trade data across multiple stock exchanges. The pipeline uses a configuration-driven approach for maintainability and includes advanced Auto Loader features for efficient streaming incremental processing.

## ✨ Key Features

### 🔧 Configuration-Driven Design
- **Sources Configuration Array**: Define all data sources in a single configuration
- **Dynamic Table Generation**: Functions that create tables based on configuration
- **Flexible Quality Rules**: Configurable data quality checks per source
- **SQL-Based Business Logic**: Meta-programming with string substitution

### 🚀 Auto Loader (Streaming File Ingestion)
- **cloudFiles Format**: Efficient file ingestion with automatic schema inference
- **Schema Evolution**: Automatic handling of schema changes
- **File-Level Change Detection**: Track individual file processing
- **Streaming Processing**: Micro-batch processing for real-time updates
- **Automatic Cleanup**: Archive or delete processed source files
- **Configurable Batch Sizes**: Control processing frequency

### ⚡ AutoCDC (Change Data Capture)
- **Change Data Feed**: Tracks all changes to data automatically
- **Automatic Optimization**: Self-optimizing tables for performance
- **Incremental Processing**: Only processes changed data
- **Change Monitoring**: Real-time change detection and tracking
- **Quality Drift Detection**: Monitors data quality changes over time

## 🏗️ Architecture

```
Bronze Layer (Auto Loader) → Silver Layer (Streaming Quality) → Gold Layer (Business Logic)
       ↓                           ↓                           ↓
   cloudFiles + AutoCDC      Streaming + AutoCDC           AutoCDC Enabled
   File Ingestion            Quality Monitoring            Business Metrics
```

### 📊 Pipeline Structure
1. **Bronze Layer**: Auto Loader ingestion with AutoCDC
2. **Silver Layer**: Streaming quality checks with AutoCDC
3. **Gold Layer**: Business logic transformations with AutoCDC
4. **Auto Loader Tables**: Change monitoring, quality tracking, and file metrics

## 📁 Project Structure

```
├── src/
│   ├── dlt_trade_pipeline.py          # Main DLT pipeline with Auto Loader & AutoCDC
│   └── trade_data_generator.py        # Data generation notebook
├── dlt_pipeline_config.json           # DLT pipeline configuration with Auto Loader
├── databricks.yml                     # Asset Bundle configuration
├── deploy_complete_solution.py        # Deployment script
└── README_AUTOCDC_PIPELINE.md         # This documentation
```

## 🎯 Auto Loader + AutoCDC Benefits

### 📈 Performance Improvements
- **Efficient Streaming Ingestion**: Process files as they arrive
- **Automatic Schema Inference**: No manual schema management needed
- **File-Level Change Detection**: Track individual file processing
- **Micro-batch Processing**: Real-time updates with configurable frequency
- **Automatic Optimization**: Self-tuning tables for better performance
- **Reduced Processing Time**: Faster pipeline execution

### 🔍 Change Detection & Monitoring
- **Real-time File Monitoring**: Track files as they arrive
- **Change History**: Complete audit trail of data modifications
- **Quality Drift**: Detect when data quality changes
- **Schema Evolution**: Handle schema changes automatically
- **Compliance**: Meet regulatory requirements for data lineage

### 🛠️ Operational Benefits
- **Self-healing**: Automatic optimization and compaction
- **File Management**: Automatic source file cleanup
- **Monitoring**: Built-in change tracking and alerts
- **Maintenance**: Reduced manual intervention needed
- **Scalability**: Handles growing data volumes efficiently

## 🚀 Deployment

### Prerequisites
- Databricks CLI installed and configured
- Access to Databricks workspace
- Unity Catalog volumes configured
- Checkpoint directories for Auto Loader

### Quick Deployment
```bash
# Deploy the complete solution
python deploy_complete_solution.py
```

### Manual Deployment Steps
1. **Deploy Asset Bundle**:
   ```bash
   databricks bundle deploy --target dev --profile my
   ```

2. **Import DLT Notebook**:
   ```bash
   databricks workspace import "src/dlt_trade_pipeline.py" "/Shared/Trade Data Generator/dlt_trade_pipeline" --profile my
   ```

3. **Create DLT Pipeline**:
   ```bash
   databricks pipelines create --json-file dlt_pipeline_config.json --profile my
   ```

## ⚙️ Configuration

### Sources Configuration with Auto Loader
```python
sources = [
    {
        "name": "nyse_trades",
        "raw_table": "bronze_nyse_trades",
        "clean_table": "silver_nyse_trades",
        "transform_table": "gold_nyse_trades",
        "source_path": "/Volumes/kishoremannava/default/filearrival/trade_data/nyse/bronze/",
        "checkpoint_path": "/Volumes/kishoremannava/default/filearrival/checkpoints/nyse_trades",
        "primary_filter": "exchange = 'NYSE'",
        "quality_rules": {"valid_trade": "price > 0 AND quantity > 0"},
        "business_logic": "SELECT *, price * quantity AS trade_value FROM __TABLE__"
    }
    # Add more sources as needed
]
```

### Auto Loader Settings
```python
# Auto Loader configuration
spark.readStream.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", checkpoint_path)
.option("cloudFiles.enableAutoCDC", "true")
.option("cloudFiles.maxFilesPerTrigger", "100")
.option("cloudFiles.cleanSource", "archive")
.option("cloudFiles.schemaEvolutionMode", "additive")
```

### AutoCDC Settings
```python
# Table properties for AutoCDC
table_properties={
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableAutoOptimize": "true"
}
```

### Pipeline Configuration
```json
{
  "configuration": {
    "spark.databricks.delta.liveTable.autoOptimize.enabled": "true",
    "spark.databricks.delta.liveTable.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.liveTable.autoOptimize.autoCompact": "true",
    "spark.databricks.delta.enableChangeDataFeed": "true",
    "spark.databricks.cloudFiles.enableAutoCDC": "true",
    "spark.databricks.cloudFiles.maxFilesPerTrigger": "100",
    "spark.databricks.cloudFiles.cleanSource": "archive",
    "spark.databricks.cloudFiles.schemaEvolutionMode": "additive"
  }
}
```

## 🔍 Monitoring and Observability

### Auto Loader Tables
1. **`trade_changes_summary`**: Tracks all changes across bronze layer tables
2. **`quality_changes_monitoring`**: Monitors quality changes and data drift
3. **`file_ingestion_metrics`**: Tracks file ingestion performance and metrics

### Change Detection
- **File Timestamps**: When files were processed
- **Change Sources**: What triggered the changes
- **Quality Metrics**: How data quality evolves over time
- **Performance Metrics**: Processing efficiency and optimization
- **Schema Changes**: Track schema evolution over time

### Quality Monitoring
- **Data Completeness**: Track missing or null values
- **Data Validation**: Monitor quality rule compliance
- **Drift Detection**: Identify when data patterns change
- **Alerting**: Get notified of quality issues
- **Streaming Metrics**: Real-time quality monitoring

## 🚀 Usage Examples

### Adding New Exchange
```python
# Simply add to sources array
{
    "name": "tokyo_trades",
    "raw_table": "bronze_tokyo_trades",
    "clean_table": "silver_tokyo_trades",
    "transform_table": "gold_tokyo_trades",
    "source_path": "/Volumes/kishoremannava/default/filearrival/trade_data/tokyo/bronze/",
    "checkpoint_path": "/Volumes/kishoremannava/default/filearrival/checkpoints/tokyo_trades",
    "primary_filter": "exchange = 'TSE'",
    "quality_rules": {"valid_trade": "price > 0 AND quantity > 0"},
    "business_logic": "SELECT *, price * quantity AS trade_value FROM __TABLE__"
}
```

### Custom Quality Rules
```python
"quality_rules": {
    "valid_trade": "price > 0 AND quantity > 0",
    "price_range": "price >= 0.01 AND price <= 1000000",
    "market_hours": "hour(timestamp) >= 9 AND hour(timestamp) < 16"
}
```

### Advanced Business Logic
```python
"business_logic": """
    SELECT 
        *,
        price * quantity AS trade_value,
        CASE 
            WHEN price > 100 THEN 'HIGH'
            WHEN price > 50 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS price_volatility
    FROM __TABLE__
"""
```

### Auto Loader Configuration
```python
# Customize Auto Loader behavior
.option("cloudFiles.maxFilesPerTrigger", "50")           # Process 50 files per batch
.option("cloudFiles.cleanSource", "delete")              # Delete processed files
.option("cloudFiles.schemaEvolutionMode", "rescue")      # Handle schema conflicts
.option("cloudFiles.rescuedDataColumn", "_rescued_data") # Store problematic data
```

## 🔧 Customization

### Quality Rules
- **Expectations**: Use `@dlt.expect_all_or_fail()` for strict validation
- **Custom Functions**: Create reusable quality check functions
- **Thresholds**: Configurable quality thresholds per source
- **Streaming**: Real-time quality monitoring

### Business Logic
- **SQL Templates**: Use `__TABLE__` placeholder for dynamic table references
- **Complex Transformations**: Support for advanced SQL operations
- **Aggregations**: Built-in support for time-based aggregations
- **Streaming**: Process data as it arrives

### Auto Loader Settings
- **Batch Sizes**: Configure files per trigger
- **Cleanup Policies**: Archive, delete, or move processed files
- **Schema Evolution**: Handle schema changes automatically
- **Error Handling**: Rescue problematic data

### AutoCDC Settings
- **Change Feed**: Enable/disable change tracking per table
- **Optimization**: Configure auto-optimization parameters
- **Compaction**: Set auto-compaction strategies
- **Streaming**: Real-time change detection

## 📊 Performance Tuning

### Auto Loader Optimization
- **Batch Sizing**: Balance latency vs. throughput
- **Schema Location**: Optimize checkpoint storage
- **File Formats**: Choose efficient file formats
- **Cleanup Policies**: Manage storage costs

### AutoCDC Optimization
- **Change Data Feed**: Efficient change tracking
- **Auto-optimization**: Automatic table optimization
- **Auto-compaction**: Smart file compaction
- **Partitioning**: Automatic partition management

### Cluster Configuration
- **Worker Nodes**: Configure based on data volume
- **Node Types**: Choose appropriate instance types
- **Photon**: Enable for faster processing
- **Auto-scaling**: Dynamic cluster sizing

## 🚨 Troubleshooting

### Common Issues
1. **Auto Loader Not Working**: Check checkpoint paths and permissions
2. **Schema Evolution Errors**: Verify schema location and permissions
3. **AutoCDC Not Working**: Check table properties and pipeline configuration
4. **Performance Issues**: Verify Auto Loader and AutoCDC settings
5. **Quality Failures**: Review quality rules and data validation
6. **Deployment Errors**: Check CLI configuration and workspace access

### Debugging
- **Pipeline Logs**: Check DLT pipeline execution logs
- **Table Properties**: Verify Auto Loader and AutoCDC settings
- **Change Feed**: Query change data feed for debugging
- **Quality Metrics**: Monitor quality monitoring tables
- **File Metrics**: Check file ingestion performance

## 🔮 Future Enhancements

### Planned Features
- **Real-time Streaming**: Support for Kafka and other streaming sources
- **Advanced Quality Rules**: Machine learning-based quality detection
- **Custom Auto Loader Policies**: Configurable file processing strategies
- **Integration APIs**: REST APIs for pipeline management
- **Multi-format Support**: Support for Parquet, JSON, and other formats

### Extensibility
- **Plugin Architecture**: Support for custom quality check plugins
- **Template Library**: Reusable pipeline templates
- **Multi-cloud Support**: Support for different cloud providers
- **Advanced Monitoring**: Enhanced observability and alerting
- **Custom File Processors**: Extend Auto Loader with custom logic

## 📚 Additional Resources

- [Databricks DLT Documentation](https://docs.databricks.com/data-engineering/delta-live-tables/)
- [Auto Loader Guide](https://docs.databricks.com/ingestion/auto-loader/)
- [AutoCDC Best Practices](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Delta Live Tables Examples](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-examples.html)
- [Change Data Feed Guide](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Streaming with DLT](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-streaming.html)

## 🤝 Contributing

This project follows a configuration-driven approach for easy customization and extension. Contributions are welcome for:

- New quality rule templates
- Additional business logic patterns
- Auto Loader optimization strategies
- AutoCDC optimization strategies
- Monitoring and alerting enhancements
- Streaming processing improvements

## 📄 License

This project is provided as-is for educational and demonstration purposes. Please refer to Databricks licensing terms for production use.
