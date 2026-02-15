"""
utils/data_validator.py
Event Hub consumer for real-time data validation and monitoring
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field
from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import pandas as pd
from tabulate import tabulate

@dataclass
class ValidationStats:
    """Statistics for data validation"""
    total_messages: int = 0
    valid_messages: int = 0
    invalid_messages: int = 0
    schema_errors: int = 0
    value_errors: int = 0
    null_errors: int = 0
    sample_messages: List[Dict] = field(default_factory=list)
    error_messages: List[Dict] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    
    @property
    def validation_rate(self) -> float:
        """Calculate percentage of valid messages"""
        if self.total_messages == 0:
            return 0.0
        return (self.valid_messages / self.total_messages) * 100
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration since start"""
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def messages_per_second(self) -> float:
        """Calculate throughput"""
        if self.duration_seconds == 0:
            return 0.0
        return self.total_messages / self.duration_seconds
    
class TrafficDataValidator:
    """Validates traffic sensor data from Event Hub"""
    # Required fields for traffic sensor data
    REQUIRED_FIELDS = {
        'sensor_id': str,
        'intersection_id': str,
        'timestamp': str,
        'vehicle_count': int,
        'average_speed': float,
        'occupancy_rate': float,
        'vehicle_types': dict,
        'wait_time_seconds': float,
        'queue_length': int,
        'signal_state': str,
        'latitude': float,
        'longitude': float,
        'district': str
    }
    # Valid value ranges
    VALUE_RANGES = {
        'vehicle_count': (0, 1000),
        'average_speed': (0, 100),  # mph
        'occupancy_rate': (0.0, 1.0),
        'wait_time_seconds': (0, 600),  # max 10 minutes
        'queue_length': (0, 500),
        'latitude': (-90, 90),
        'longitude': (-180, 180)
    }
    # Valid enum values
    VALID_ENUMS = {
        'signal_state': ['red', 'yellow', 'green'],
        'district': ['downtown', 'residential', 'industrial', 'suburban']
    }

    def __init__(self):
        self.stats = ValidationStats()

    def _validate_schema(self, message: Dict) -> List[str]:
        errors = []
        for field_name, field_type in self.REQUIRED_FIELDS.items():
            if field_name not in message:
                errors.append(f"Missing field: {field_name}")
                continue
            if message[field_name] is None:
                errors.append(f"Null field: {field_name}")
                continue
            if not isinstance(message[field_name], field_type):
                errors.append(f"Invalid type for field {field_name}: {type(message[field_name])} (expected {field_type})")
        return errors