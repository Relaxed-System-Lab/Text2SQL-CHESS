"""
SQL Execution Timer Module

This module provides timing and logging for SQL query execution.
Use this to track how long SQL queries take to execute.

Usage:
    from sql_execution_timer import SQLExecutionTimer

    # Create timer instance
    timer = SQLExecutionTimer(log_dir="results/timings", log_to_console=True)

    # In execute_sql function, wrap the execution:
    timer.start("query_1")
    # ... execute SQL ...
    timer.end("query_1", query="SELECT * FROM users")
    
    # Or use context manager:
    with timer.context("query_1", query="SELECT * FROM users"):
        # ... execute SQL ...
        pass

    # Get statistics
    stats = timer.get_statistics()
    timer.dump_to_csv("timings.csv")
"""

import time
import json
import csv
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
from datetime import datetime


@dataclass
class ExecutionRecord:
    """Record of a single SQL execution."""
    query_id: str
    query: Optional[str] = None
    start_time: float = 0.0
    end_time: float = 0.0
    duration_ms: float = 0.0
    success: bool = True
    error_msg: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class SQLExecutionTimer:
    """Timer for tracking SQL query execution times."""

    def __init__(self, log_dir: str = "results/sql_timings", log_to_console: bool = True):
        """
        Initialize the SQL Execution Timer.

        Args:
            log_dir (str): Directory to save timing logs.
            log_to_console (bool): Whether to print timing info to console.
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.log_to_console = log_to_console
        self.records: Dict[str, ExecutionRecord] = {}
        self.timers: Dict[str, float] = {}  # For tracking start times
        
        # Setup logger
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger("SQLExecutionTimer")
        logger.handlers.clear()  # Clear any existing handlers
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = self.log_dir / "sql_execution_times.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler (optional)
        if self.log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            logger.addHandler(console_handler)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger

    def start(self, query_id: str) -> None:
        """
        Start timing a query execution.

        Args:
            query_id (str): Unique identifier for the query.
        """
        self.timers[query_id] = time.time()
        self.logger.info(f"[START] Query: {query_id}")

    def end(self, query_id: str, query: Optional[str] = None, success: bool = True, 
            error_msg: Optional[str] = None) -> float:
        """
        End timing for a query and record the execution.

        Args:
            query_id (str): Unique identifier for the query.
            query (Optional[str]): The actual SQL query string.
            success (bool): Whether the query executed successfully.
            error_msg (Optional[str]): Error message if execution failed.

        Returns:
            float: Duration in milliseconds.
        """
        if query_id not in self.timers:
            self.logger.warning(f"No start time recorded for query: {query_id}")
            return 0.0

        end_time = time.time()
        start_time = self.timers[query_id]
        duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds

        record = ExecutionRecord(
            query_id=query_id,
            query=query,
            start_time=start_time,
            end_time=end_time,
            duration_ms=duration_ms,
            success=success,
            error_msg=error_msg
        )

        self.records[query_id] = record
        
        status = "SUCCESS" if success else "FAILED"
        log_msg = f"[END] Query: {query_id} | Duration: {duration_ms:.2f}ms | Status: {status}"
        if error_msg:
            log_msg += f" | Error: {error_msg}"
        
        self.logger.info(log_msg)
        
        return duration_ms

    @contextmanager
    def context(self, query_id: str, query: Optional[str] = None):
        """
        Context manager for timing SQL execution.

        Usage:
            with timer.context("query_1", query="SELECT * FROM users"):
                # execute SQL here
                pass
        """
        self.start(query_id)
        try:
            yield
            self.end(query_id, query=query, success=True)
        except Exception as e:
            self.end(query_id, query=query, success=False, error_msg=str(e))
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get summary statistics of all recorded executions.

        Returns:
            Dict[str, Any]: Statistics including total, average, min, max times.
        """
        if not self.records:
            return {"message": "No records yet"}

        durations = [r.duration_ms for r in self.records.values() if r.success]
        errors = [r for r in self.records.values() if not r.success]

        if not durations:
            avg_time = min_time = max_time = 0.0
        else:
            avg_time = sum(durations) / len(durations)
            min_time = min(durations)
            max_time = max(durations)

        return {
            "total_queries": len(self.records),
            "successful_queries": len(durations),
            "failed_queries": len(errors),
            "total_time_ms": sum(durations),
            "average_time_ms": avg_time,
            "min_time_ms": min_time,
            "max_time_ms": max_time,
        }

    def dump_to_json(self, filename: Optional[str] = None) -> str:
        """
        Dump all records to JSON file.

        Args:
            filename (Optional[str]): Output filename. Default: sql_execution_records.json

        Returns:
            str: Path to the output file.
        """
        if filename is None:
            filename = "sql_execution_records.json"
        
        output_path = self.log_dir / filename
        records_data = [r.to_dict() for r in self.records.values()]
        
        with open(output_path, 'w') as f:
            json.dump(records_data, f, indent=2)
        
        self.logger.info(f"Execution records saved to: {output_path}")
        return str(output_path)

    def dump_to_csv(self, filename: Optional[str] = None) -> str:
        """
        Dump all records to CSV file.

        Args:
            filename (Optional[str]): Output filename. Default: sql_execution_records.csv

        Returns:
            str: Path to the output file.
        """
        if filename is None:
            filename = "sql_execution_records.csv"
        
        output_path = self.log_dir / filename
        
        if not self.records:
            self.logger.warning("No records to save")
            return str(output_path)

        with open(output_path, 'w', newline='') as f:
            fieldnames = ['query_id', 'duration_ms', 'success', 'timestamp', 'query', 'error_msg']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in self.records.values():
                writer.writerow({
                    'query_id': record.query_id,
                    'duration_ms': f"{record.duration_ms:.2f}",
                    'success': record.success,
                    'timestamp': record.timestamp,
                    'query': record.query if record.query else '',
                    'error_msg': record.error_msg if record.error_msg else ''
                })
        
        self.logger.info(f"CSV report saved to: {output_path}")
        return str(output_path)

    def dump_summary(self, filename: Optional[str] = None) -> str:
        """
        Dump summary statistics to JSON file.

        Args:
            filename (Optional[str]): Output filename. Default: sql_execution_summary.json

        Returns:
            str: Path to the output file.
        """
        if filename is None:
            filename = "sql_execution_summary.json"
        
        output_path = self.log_dir / filename
        stats = self.get_statistics()
        
        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        self.logger.info(f"Summary statistics saved to: {output_path}")
        return str(output_path)

    def print_summary(self) -> None:
        """Print summary statistics to console."""
        stats = self.get_statistics()
        print("\n" + "="*50)
        print("SQL EXECUTION TIMING SUMMARY")
        print("="*50)
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")
        print("="*50 + "\n")
