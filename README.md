# AWS Glue ETL Pipeline - Business Intelligence Database

A production-ready ETL pipeline built with AWS Glue and PySpark for processing business intelligence data from the SmartFarm platform. This pipeline extracts data from AWS Glue Catalog, transforms it using PySpark, and loads the results to S3 in both Parquet and CSV formats.

## 📋 Overview

This ETL pipeline processes multiple data domains including:
- **Animal Management**: Livestock data with breed information and reproductive status
- **Collar Monitoring**: IoT collar reporting and linking status
- **User Analytics**: User activity tracking and access patterns
- **Farm Intelligence**: Comprehensive farm statistics and operational metrics

## 🏗️ Architecture

```
┌─────────────────┐
│  Glue Catalog   │
│  (Source Data)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Extractors    │ ◄── Load tables from Glue Catalog
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Queries  │ ◄── Build complex DataFrames with joins
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transformers   │ ◄── Apply business logic transformations
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Loaders      │ ◄── Write to S3 (Parquet + CSV)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   S3 Bucket     │
│ (Output Data)   │
└─────────────────┘
```

## 📁 Project Structure

```
glue_etl/
├── job/
│   └── main_etl.py                    # Main Glue job entry point
├── lib/
│   ├── extractors/
│   │   └── glue_catalog_extractor.py  # Extract data from Glue Catalog
│   ├── queries/
│   │   └── spark_queries.py           # Complex SQL-like queries in PySpark
│   ├── transformers/
│   │   └── pyspark_transformers.py    # Business logic transformations
│   ├── loaders/
│   │   └── s3_loader.py               # Write data to S3
│   └── utils/
│       └── logger.py                  # Logging utility
└── glue_etl_package.zip               # Packaged library for Glue

deploy_to_s3.py                        # Deployment script
```

## 🚀 Deployment

### Prerequisites

- Python 3.9+
- AWS CLI configured with appropriate credentials
- Access to AWS Glue and S3 services
- boto3 library installed

### Deploy to AWS

Use the automated deployment script to package and upload the ETL job to S3:

```bash
python deploy_to_s3.py
```

This script will:
1. ✅ Delete old `main_etl.py` from S3
2. ✅ Upload new `main_etl.py` to `s3://intel-business-intelligence/bi-database/scripts/job/`
3. ✅ Delete old `glue_etl_package.zip` from S3
4. ✅ Create new zip package from `lib/` directory
5. ✅ Upload new `glue_etl_package.zip` to `s3://intel-business-intelligence/bi-database/scripts/`

### Manual Configuration

After deployment, configure your AWS Glue Job with:

- **Script location**: `s3://intel-business-intelligence/bi-database/scripts/job/main_etl_bi_database.py`
- **Python library path**: `s3://intel-business-intelligence/bi-database/scripts/glue_etl_package_bi_database.zip`
- **Glue version**: 4.0
- **Language**: Python 3
- **Worker type**: G.1X or G.2X (recommended)
- **Number of workers**: Based on data volume

## 📊 Data Processing

### Source Database
- **Glue Database**: `smartfarm-dev`
- **Tables**: 17 source tables including animals, farms, collars, users, and related entities

### Output Datasets

The pipeline generates 6 analytical datasets:

1. **`coleiras_reportando`** - Collar reporting status
   - Tracks active/inactive collars
   - Links collars to farms and animals
   - Identifies collars reporting in last 48 hours

2. **`coleiras_vinculadas`** - Linked collars with animal details
   - Active collar-animal assignments
   - Breed information
   - Linking/removal timestamps

3. **`animals`** - Comprehensive animal data
   - Reproductive status (Novilha/Primípara/Multípara)
   - Birth and delivery dates
   - Health and production status
   - Breed composition

4. **`users_active`** - User activity tracking
   - Access timestamps from 2025-01-01 onwards
   - Page-level activity tracking
   - Farm-user relationships

5. **`farm_states`** - Farm operational metrics
   - Collar inventory and status
   - Animal counts by status
   - Geographic information
   - Contract details

6. **`contagem_acessos`** - Manager access analytics
   - Monthly access counts per user
   - Separates manager vs non-manager access
   - Aggregated by year-month

### Output Formats

Each dataset is written in two formats:
- **Parquet**: `s3://intel-business-intelligence/bi-database/etl_parquet/`
- **CSV**: `s3://intel-business-intelligence/bi-database/etl_csv/`

## 🔧 Key Components

### Extractors
[`GlueCatalogExtractor`](glue_etl/lib/extractors/glue_catalog_extractor.py) - Loads tables from AWS Glue Catalog with automatic caching for performance optimization.

### Queries
[`SparkQueries`](glue_etl/lib/queries/spark_queries.py) - Implements complex business logic queries:
- Multi-table joins
- Window functions for ranking
- Temporal filtering (48-hour windows)
- Aggregations and pivots

### Transformers
[`pyspark_transformers.py`](glue_etl/lib/transformers/pyspark_transformers.py) - Business logic transformations:
- **AnimalsTransformerSpark**: Adds reproductive status classification
- **ColeirasReportandoTransformerSpark**: Collar status transformations
- **ColeirasVinculadasTransformerSpark**: Linked collar transformations
- **ContagemAcessosPorUsuarioTransformerSpark**: User access aggregations

### Loaders
[`S3Loader`](glue_etl/lib/loaders/s3_loader.py) - Handles S3 write operations:
- Single-file Parquet output
- Single-file CSV output with headers
- Automatic cleanup of temporary files
- Proper file naming

## 🔍 Monitoring

The pipeline includes comprehensive logging:
- Job start/end timestamps
- Step-by-step progress tracking
- Error handling and reporting
- Data validation checkpoints

Logs are available in AWS CloudWatch under the Glue job execution logs.

## ⚙️ Configuration

Key configurations in [`main_etl.py`](glue_etl/job/main_etl.py:40-41):

```python
GLUE_DB = "smartfarm-dev"
OUTPUT_BUCKET = "s3://intel-business-intelligence/bi-database"
```

Spark configurations for datetime handling:
```python
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
```

## 📝 Development

### Local Testing

For local development and testing, use the included Jupyter notebook:
- [`comparing_files.ipynb`](glue_etl/comparing_files.ipynb) - Compare outputs between different processing methods

### Adding New Transformations

1. Create transformer class in [`pyspark_transformers.py`](glue_etl/lib/transformers/pyspark_transformers.py)
2. Implement transformation logic using PySpark DataFrame API
3. Add to [`main_etl.py`](glue_etl/job/main_etl.py) pipeline
4. Update outputs dictionary with new dataset
5. Redeploy using [`deploy_to_s3.py`](deploy_to_s3.py)

## 🛠️ Technologies

- **AWS Glue** - Serverless ETL service
- **PySpark** - Distributed data processing
- **AWS S3** - Data lake storage
- **Boto3** - AWS SDK for Python
- **Parquet** - Columnar storage format

## 📄 License

See [LICENSE](LICENSE) file for details.

## 🤝 Contributing

This is a production ETL pipeline. All changes should be:
1. Tested locally when possible
2. Reviewed for performance impact
3. Deployed during maintenance windows
4. Monitored post-deployment

---

**Region**: `sa-east-1` (São Paulo)  
**S3 Bucket**: `intel-business-intelligence`  
**Glue Database**: `smartfarm-dev`