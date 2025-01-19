import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(script_name, log_directory="03_logs"):
    """
    Sets up a logger that writes to both console and a file.
    
    Args:
        script_name (str): Name of the script (used for log file naming)
        log_directory (str): Directory where log files will be stored
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / log_directory
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"{script_name}_{timestamp}.log"
    log_path = log_dir / log_filename
    
    # Create logger
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    
    # Create formatters and handlers
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = logging.Formatter('%(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger 