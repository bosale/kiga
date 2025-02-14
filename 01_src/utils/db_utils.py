"""
Database utility functions for SQL Server operations.
"""

import json
from pathlib import Path
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, types
from sqlalchemy.engine import URL
import pandas as pd
from typing import Dict, Optional
import numpy as np

def infer_sql_type(dtype: str, column_name: str) -> types.TypeEngine:
    """
    Infer SQLAlchemy type from pandas dtype.
    
    Args:
        dtype: Pandas dtype string
        column_name: Name of the column (used for length inference)
        
    Returns:
        SQLAlchemy type
    """
    # Handle numpy/pandas dtypes
    if pd.api.types.is_integer_dtype(dtype):
        return types.Integer()
    elif pd.api.types.is_float_dtype(dtype):
        return types.Float()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return types.DateTime()
    elif pd.api.types.is_bool_dtype(dtype):
        return types.Boolean()
    
    # Default to String with appropriate length
    # Use longer length for specific columns that might need it
    if any(keyword in column_name.lower() for keyword in ['beschreibung', 'erlaeuterung', 'kommentar', 'eintrag']):
        return types.String(length=1000)
    return types.String(length=255)

def derive_sql_types(df: pd.DataFrame) -> Dict[str, types.TypeEngine]:
    """
    Derive SQL types from DataFrame columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary mapping column names to SQLAlchemy types
    """
    sql_types = {}
    for column in df.columns:
        dtype = df[column].dtype
        sql_types[column] = infer_sql_type(dtype, column)
    return sql_types

def load_db_config() -> dict:
    """Load database configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def create_connection_string(config: dict) -> str:
    """Create SQL Server connection string from config."""
    connection_url = URL.create(
        "mssql+pyodbc",
        username=config.get('username'),
        password=config.get('password'),
        host=config['server'],
        database=config['database'],
        query={
            'driver': config['driver'],
            'TrustedConnection': config['trusted_connection']
        }
    )
    return str(connection_url)

def get_engine(config: Optional[dict] = None):
    """Get SQLAlchemy engine for database operations."""
    if config is None:
        config = load_db_config()
    connection_string = create_connection_string(config)
    return create_engine(connection_string)

def write_to_sql(df: pd.DataFrame, table_name: str, sql_types: Optional[Dict] = None, logger: Optional[logging.Logger] = None):
    """
    Write DataFrame to SQL Server table.
    
    Args:
        df: DataFrame to write
        table_name: Name of the target table
        sql_types: Optional dictionary mapping column names to SQLAlchemy types. If None, types will be inferred.
        logger: Optional logger instance for logging operations
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Derive SQL types if not provided
        if sql_types is None:
            sql_types = derive_sql_types(df)
            logger.info("SQL types automatically derived from DataFrame")
            
        engine = get_engine()
        schema_name = load_db_config().get('schema_name', 'dbo')
        
        # Create table if it doesn't exist
        metadata = MetaData()
        columns = [Column(name, sql_type) for name, sql_type in sql_types.items()]
        table = Table(table_name, metadata, *columns, schema=schema_name)
        metadata.create_all(engine)
        
        # Write data to table
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='append',
            index=False,
            dtype=sql_types
        )
        
        logger.info(f"Successfully wrote {len(df)} rows to {schema_name}.{table_name}")
        logger.debug("Column types:")
        for col, sql_type in sql_types.items():
            logger.debug(f"  {col}: {sql_type}")
        
    except Exception as e:
        logger.error(f"Error writing to SQL Server: {str(e)}")
        raise 