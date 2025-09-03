# Databricks notebook source
# MAGIC %md
# MAGIC # Trade Data Generator - Asset Bundle Version
# MAGIC 
# MAGIC This notebook is deployed via Databricks Asset Bundles and generates realistic trade data
# MAGIC for multiple stock exchanges using dbldatagen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Required Packages

# COMMAND ----------

# Install dbldatagen for synthetic data generation
%pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Setup

# COMMAND ----------

# Import required libraries
import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import time
from pathlib import Path
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration

# COMMAND ----------

# Exchange configurations with realistic symbols and price ranges
EXCHANGES = {
    'NYSE': {
        'symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'JPM', 'JNJ', 'V', 'PG', 'UNH'],
        'price_ranges': {
            'AAPL': (150.0, 200.0),
            'MSFT': (300.0, 400.0),
            'GOOGL': (2500.0, 3000.0),
            'TSLA': (200.0, 300.0),
            'JPM': (150.0, 200.0),
            'JNJ': (150.0, 200.0),
            'V': (200.0, 300.0),
            'PG': (140.0, 180.0),
            'UNH': (500.0, 600.0)
        }
    },
    'NASDAQ': {
        'symbols': ['NVDA', 'AMD', 'INTC', 'ORCL', 'CSCO', 'ADBE', 'CRM', 'NFLX', 'PYPL', 'ZM'],
        'price_ranges': {
            'NVDA': (400.0, 600.0),
            'AMD': (100.0, 150.0),
            'INTC': (30.0, 50.0),
            'ORCL': (80.0, 120.0),
            'CSCO': (40.0, 60.0),
            'ADBE': (400.0, 600.0),
            'CRM': (200.0, 300.0),
            'NFLX': (400.0, 600.0),
            'PYPL': (50.0, 80.0),
            'ZM': (60.0, 100.0)
        }
    },
    'LSE': {
        'symbols': ['HSBA', 'BP', 'GSK', 'ULVR', 'RIO', 'BHP', 'VOD', 'BT', 'LLOY', 'BARC'],
        'price_ranges': {
            'HSBA': (5.0, 8.0),
            'BP': (4.0, 6.0),
            'GSK': (12.0, 18.0),
            'ULVR': (40.0, 60.0),
            'RIO': (50.0, 80.0),
            'BHP': (20.0, 30.0),
            'VOD': (0.5, 1.5),
            'BT': (1.0, 2.0),
            'LLOY': (0.3, 0.8),
            'BARC': (1.0, 2.0)
        }
    }
}

# Generation parameters
GENERATION_CONFIG = {
    "default_rows_per_exchange": 1000,
    "incremental_rows_per_exchange": 500,
    "max_partitions": 4
}

# Trade types and market centers
TRADE_TYPES = ['BUY', 'SELL', 'SHORT', 'COVER']
MARKET_CENTERS = ['MAIN', 'ARCA', 'EDGX', 'EDGA', 'BATS', 'IEX']

# Data storage paths in DBFS
STORAGE_CONFIG = {
    "base_path": "/Volumes/kishoremannava/default/filearrival/trade_data",
    "checkpoint_path": "/Volumes/kishoremannava/default/filearrival/checkpoints/trade_pipeline"
}

print(f"Configuration loaded:")
print(f"  • Exchanges: {list(EXCHANGES.keys())}")
print(f"  • Base path: {STORAGE_CONFIG['base_path']}")
print(f"  • Default rows per exchange: {GENERATION_CONFIG['default_rows_per_exchange']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Directory Structure

# COMMAND ----------

# Create base directory structure in DBFS
base_path = Path(STORAGE_CONFIG["base_path"])
base_path.mkdir(exist_ok=True)

# Create exchange directories with bronze structure
for exchange in EXCHANGES.keys():
    exchange_dir = base_path / exchange.lower()
    exchange_dir.mkdir(exist_ok=True)
    
    # Create only bronze directory for raw data
    (exchange_dir / "bronze").mkdir(exist_ok=True)
    
    print(f"✓ Created directory: {exchange_dir}")

print(f"\nDirectory structure created at: {STORAGE_CONFIG['base_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Generation Functions

# COMMAND ----------

def generate_trade_data(exchange, rows=1000, start_time=None, end_time=None):
    """Generate trade data using dbldatagen for a specific exchange"""
    
    if start_time is None:
        start_time = datetime.now() - timedelta(hours=1)
    if end_time is None:
        end_time = datetime.now()
    
    exchange_config = EXCHANGES[exchange]
    
    print(f"Generating {rows} trade records for {exchange}")
    print(f"Time range: {start_time} to {end_time}")
    
    # Create data generator specification
    dg_spec = (dg.DataGenerator(spark, name=f"{exchange}_trades", rows=rows, partitions=GENERATION_CONFIG["max_partitions"])
              .withIdOutput()
              .withColumn("trade_id", StringType(), 
                         expr="concat('TRD_', cast(id as string), '_', cast(rand() * 1000000 as int))")
              .withColumn("symbol", StringType(), 
                         values=exchange_config['symbols'], random=True)
              .withColumn("exchange", StringType(), values=[exchange])
              .withColumn("price", DecimalType(10, 4), 
                         expr="round(rand() * 1000 + 10, 2)")
              .withColumn("quantity", LongType(), 
                         expr="cast(rand() * 10000 + 100 as long)")
              .withColumn("timestamp", TimestampType(), 
                         expr=f"cast('{start_time.isoformat()}' as timestamp) + " +
                              f"cast(rand() * {int((end_time - start_time).total_seconds())} as long) * interval 1 second")
              .withColumn("trade_type", StringType(), 
                         values=TRADE_TYPES, random=True)
              .withColumn("market_center", StringType(), 
                         values=MARKET_CENTERS, random=True)
              .withColumn("ingestion_timestamp", TimestampType(), 
                         expr="current_timestamp()")
              .withColumn("file_path", StringType(), 
                         expr=f"concat('/mnt/data/{exchange.lower()}/trades_', " +
                              "date_format(timestamp, 'yyyyMMdd_HHmm'), '.parquet')")
              )
    
    # Generate the data
    df = dg_spec.build()
    
    # Show sample data
    print(f"Sample data for {exchange}:")
    df.show(5, truncate=False)
    
    return df

def save_to_csv(df, exchange, filename):
    """Save DataFrame to CSV in the appropriate bronze directory"""
    file_path = base_path / exchange.lower() / "bronze" / filename
    
    print(f"Saving to: {file_path}")
    
    # Convert to pandas and save as single CSV file
    df.toPandas().to_csv(str(file_path), header=True, index=False)
    
    print(f"✓ CSV saved: {file_path}")
    return str(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Trade Data (Initial + Incremental)

# COMMAND ----------

# Get parameters from job
dbutils.widgets.text("rows_per_exchange", "1000")
dbutils.widgets.text("exchange_filter", "all")
dbutils.widgets.text("data_type", "both")  # "initial", "incremental", or "both"

rows_per_exchange = int(dbutils.widgets.get("rows_per_exchange"))
exchange_filter = dbutils.widgets.get("exchange_filter")
data_type = dbutils.widgets.get("data_type")

print(f"Job parameters:")
print(f"  • Rows per exchange: {rows_per_exchange}")
print(f"  • Exchange filter: {exchange_filter}")
print(f"  • Data type: {data_type}")

# Filter exchanges if specified
exchanges_to_process = EXCHANGES.keys()
if exchange_filter != "all":
    exchanges_to_process = [exchange_filter]

print(f"\nProcessing exchanges: {list(exchanges_to_process)}")
print("="*50)

# Dictionary to track all generated files
all_generated_files = {}

for exchange in exchanges_to_process:
    print(f"\n{'='*50}")
    print(f"Processing {exchange}")
    print(f"{'='*50}")
    
    exchange_files = []
    
    try:
        # Generate initial data if requested
        if data_type in ["initial", "both"]:
            print(f"Generating initial data for {exchange}...")
            df_initial = generate_trade_data(exchange, rows=rows_per_exchange)
            
            # Save initial data
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"trades_{exchange.lower()}_{timestamp_str}.csv"
            
            file_path = save_to_csv(df_initial, exchange, filename)
            exchange_files.append(("initial", file_path))
            print(f"✓ Initial data saved: {file_path}")
        
        # Generate incremental data if requested
        if data_type in ["incremental", "both"]:
            print(f"Generating incremental data for {exchange}...")
            
            # Use last hour as time range for incremental data
            start_time = datetime.now() - timedelta(hours=1)
            end_time = datetime.now()
            
            df_incremental = generate_trade_data(exchange, 
                                               GENERATION_CONFIG["incremental_rows_per_exchange"], 
                                               start_time, end_time)
            
            # Save incremental data
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"incremental_{exchange.lower()}_{timestamp_str}.csv"
            
            file_path = save_to_csv(df_incremental, exchange, filename)
            exchange_files.append(("incremental", file_path))
            print(f"✓ Incremental data saved: {file_path}")
        
        all_generated_files[exchange] = exchange_files
        print(f"✓ Completed {exchange}: {len(exchange_files)} files generated")
        
    except Exception as e:
        print(f"❌ Error processing {exchange}: {str(e)}")
        all_generated_files[exchange] = []

print(f"\nData generation completed:")
print("="*50)

# Summary of all generated files
total_files = 0
for exchange, files in all_generated_files.items():
    print(f"\n{exchange}:")
    if files:
        for data_type, file_path in files:
            print(f"  ✓ {data_type.capitalize()}: {file_path}")
            total_files += 1
    else:
        print(f"  ❌ No files generated")

print(f"\nTotal files generated: {total_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary and Results

# COMMAND ----------

print("🎉 Trade Data Generation Complete!")
print("="*50)

# Show final statistics
total_files = 0
total_size_mb = 0

for exchange in exchanges_to_process:
    exchange_dir = base_path / exchange.lower() / "bronze"
    if exchange_dir.exists():
        csv_files = list(exchange_dir.glob("*.csv"))
        #exchange_size = sum(f.stat().st_size for f in csv_files if f.exists())
        
        print(f"\n{exchange}:")
        print(f"  • Files generated: {len(csv_files)}")
        #print(f"  • Total size: {exchange_size / (1024 * 1024):.2f} MB")
        
        total_files += len(csv_files)
        #total_size_mb += exchange_size / (1024 * 1024)

print(f"\nOverall Summary:")
print(f"  • Total exchanges: {len(exchanges_to_process)}")
print(f"  • Total files: {total_files}")
#print(f"  • Total size: {total_size_mb:.2f} MB")
print(f"  • Base directory: {STORAGE_CONFIG['base_path']}")

print(f"\nFile locations:")
for exchange in exchanges_to_process:
    print(f"  • {exchange}: {STORAGE_CONFIG['base_path']}/{exchange.lower()}/bronze/")

print(f"\nJob completed successfully at: {datetime.now()}")
