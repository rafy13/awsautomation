# AWS DataSync Automation

This repository contains a Python script for automating AWS DataSync operations for bulk data synchronization tasks.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Environment Configuration

Copy the example environment file and configure your settings:

```bash
cp env.example .env
```

Edit the `.env` file with your actual configuration values:

- **Redis Configuration**: Set your Redis host, port, and password
- **MySQL Database Configuration**: Configure your MySQL connection details
- **AWS Configuration**: Configure your AWS region, account ID, EFS, and S3 settings
- **Queue Configuration**: Set up Redis queue names for task management
- **Task Configuration**: Configure task names and settings

### 3. Usage

#### DataSync Operations

```bash
# Basic usage with default .env file
python run_datasync_tasks.py

# Use custom environment file
python run_datasync_tasks.py -e /path/to/custom.env

# Set number of tasks
python run_datasync_tasks.py -t 5
```

## Command Line Arguments

### run_datasync_tasks.py
- `-e, --env_file`: Specify the .env file path (default: .env)
- `-t, --tasks_count`: Number of parallel DataSync tasks (default: 3)

## Environment Variables

All configuration is managed through environment variables. See `env.example` for a complete list of required variables.

Key variables include:
- **Database connections**: MySQL and Redis configuration
- **AWS configuration**: Region, account ID, EFS, S3, and security settings
- **Queue and task management**: Redis queue names and key prefixes
- **Logging configuration**: Log directory settings

### Required Variables:
- `REDIS_HOST`, `REDIS_PORT`: Redis connection details
- `MYSQL_HOST`, `MYSQL_DATABASE`, `MYSQL_DB_USER`, `MYSQL_DB_PASSWORD`: MySQL database connection
- `REGION`, `ACCOUNT_ID`: AWS configuration
- `SOURCE_EFS_ID`, `SOURCE_SUBNET_ID`, `SECURITY_GROUP`: EFS source configuration
- `SOURCE_SUBDIRECTORY`, `DEST_LOCATION_ARN`: DataSync source and destination
- `TASK_NAME`: Name for DataSync tasks

## Logging

Logs are written to the directory specified by `LOG_DIRECTORY` environment variable (default: `log/`).
- Info logs: `log/YYYYMMDD/info-YYYYMMDD-HHMMSS.log`
- Error logs: `log/YYYYMMDD/error-YYYYMMDD-HHMMSS.log`
- Instance-specific logs: `log/{instance_id}_datasync_events.log`

## How It Works

The script manages AWS DataSync operations by:

1. **Loading Configuration**: Reads environment variables from `.env` file
2. **Queue Management**: Uses Redis queues to manage instance processing
3. **Task Management**: Creates and manages multiple DataSync tasks for parallel processing
4. **Instance Processing**: Processes instances from MySQL database metadata
5. **Monitoring**: Tracks running DataSync operations and handles completion/failures
6. **Locking**: Uses Redis-based locking to prevent duplicate processing

The main components include:
- `DatasyncConfig`: Configuration management
- `MySQLDatabase`: Database connection handling
- `TasksManager`: AWS DataSync task creation and management
- `Datasync`: Main orchestration class
