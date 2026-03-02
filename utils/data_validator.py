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

    def _validate_schema(self, data: Dict) -> List[str]:
        """Validate schema: required fields and data types"""
        errors = []
        
        for field_name, field_type in self.REQUIRED_FIELDS.items():
            # Check if field exists
            if field_name not in data:
                errors.append(f"Missing required field: '{field_name}'")
                continue
            
            # Check if field is null
            if data[field_name] is None:
                errors.append(f"Field '{field_name}' is null")
                continue
            
            # Check data type
            if not isinstance(data[field_name], field_type):
                errors.append(
                    f"Field '{field_name}' has wrong type. "
                    f"Expected {field_type.__name__}, got {type(data[field_name]).__name__}"
                )
        
        # Validate vehicle_types structure
        if 'vehicle_types' in data and isinstance(data['vehicle_types'], dict):
            expected_keys = {'car', 'truck', 'motorcycle', 'bus'}
            actual_keys = set(data['vehicle_types'].keys())
            
            if actual_keys != expected_keys:
                missing = expected_keys - actual_keys
                extra = actual_keys - expected_keys
                if missing:
                    errors.append(f"vehicle_types missing keys: {missing}")
                if extra:
                    errors.append(f"vehicle_types has unexpected keys: {extra}")
        
        return errors

    def _validate_value_ranges(self, data: Dict) -> List[str]:
        """Validate value ranges and enums"""
        errors = []
        for field_name, (min, max) in self.VALUE_RANGES.items():
            if field_name in data:
                value = data[field_name]
                if not (min <= value <= max):
                    errors.append(f"Value for '{field_name}' is out of range: {value}")
        for field_name, values in self.VALID_ENUMS.items():
            if field_name in data:
                value = data[field_name]
                if value not in values:
                    errors.append(f"Value for '{field_name}' is not valid: {value}")
        
        if "timestamp" in data:
            try:
                datetime.fromisoformat(data["timestamp"])
            except ValueError:
                errors.append(f"Invalid timestamp format: {data['timestamp']}")
        
        if 'vehicle_types' in data and isinstance(data['vehicle_types'], dict):
            total_from_types = sum(data['vehicle_types'].values())
            if 'vehicle_count' in data:
                if abs(total_from_types - data['vehicle_count']) > 2:  # Allow small rounding errors
                    errors.append(
                        f"vehicle_count ({data['vehicle_count']}) doesn't match "
                        f"sum of vehicle_types ({total_from_types})"
                    )
        
        return errors
    def update_stats(self, is_valid: bool, errors: List[str], data: Dict):
        """Update validation statistics"""
        self.stats.total_messages += 1
        if is_valid:
            self.stats.valid_messages += 1
            # sample
            if len(self.stats.sample_messages) < 5:
                self.stats.sample_messages.append(data)
        else:
            self.stats.invalid_messages += 1

            for error in errors:
                if 'Missing required field' in error or 'wrong type' in error:
                    self.stats.schema_errors += 1
                elif 'out of range' in error or 'invalid value' in error:
                    self.stats.value_errors += 1
                elif 'null' in error:
                    self.stats.null_errors += 1
            
            if len(self.stats.sample_errors) < 10:
                self.stats.sample_errors.append({
                    'data': data,
                    'errors': errors
                })

class WeatherDataValidator:
    """Validates weather data from Event Hub"""
    
    REQUIRED_FIELDS = {
        'station_id': str,
        'timestamp': str,
        'temperature_f': float,
        'humidity': float,
        'precipitation_rate': float,
        'visibility_miles': float,
        'wind_speed_mph': float,
        'condition': str,
        'latitude': float,
        'longitude': float
    }
    
    VALUE_RANGES = {
        'temperature_f': (-50, 150),
        'humidity': (0.0, 1.0),
        'precipitation_rate': (0.0, 10.0),  # inches/hour
        'visibility_miles': (0.0, 20.0),
        'wind_speed_mph': (0.0, 150.0),
        'latitude': (-90, 90),
        'longitude': (-180, 180)
    }
    
    VALID_ENUMS = {
        'condition': ['clear', 'cloudy', 'rain', 'heavy_rain', 'snow', 'fog']
    }
    
    def __init__(self):
        self.stats = ValidationStats()
    
    def validate_message(self, message_body: str) -> Tuple[bool, List[str]]:
        """Validate a weather message"""
        errors = []
        
        try:
            data = json.loads(message_body)
        except json.JSONDecodeError as e:
            return False, [f"JSON parsing error: {str(e)}"]
        
        # Schema validation
        for field_name, field_type in self.REQUIRED_FIELDS.items():
            if field_name not in data:
                errors.append(f"Missing required field: '{field_name}'")
                continue
            
            if data[field_name] is None:
                errors.append(f"Field '{field_name}' is null")
                continue
            
            if not isinstance(data[field_name], field_type):
                errors.append(
                    f"Field '{field_name}' has wrong type. "
                    f"Expected {field_type.__name__}, got {type(data[field_name]).__name__}"
                )
        
        # Value validation
        for field_name, (min_val, max_val) in self.VALUE_RANGES.items():
            if field_name in data:
                value = data[field_name]
                if not (min_val <= value <= max_val):
                    errors.append(
                        f"Field '{field_name}' value {value} is out of range [{min_val}, {max_val}]"
                    )
        
        for field_name, valid_values in self.VALID_ENUMS.items():
            if field_name in data:
                value = data[field_name]
                if value not in valid_values:
                    errors.append(
                        f"Field '{field_name}' has invalid value '{value}'. "
                        f"Must be one of: {valid_values}"
                    )
        
        return len(errors) == 0, errors
    
    def update_stats(self, is_valid: bool, errors: List[str], message_data: Dict):
        """Update validation statistics"""
        self.stats.total_messages += 1
        
        if is_valid:
            self.stats.valid_messages += 1
            if len(self.stats.sample_messages) < 5:
                self.stats.sample_messages.append(message_data)
        else:
            self.stats.invalid_messages += 1
            if len(self.stats.error_messages) < 10:
                self.stats.error_messages.append({
                    'data': message_data,
                    'errors': errors
                })

class EventHubDataValidator:
    """Main validator that consumes from Event Hubs and validates data"""
    def __init__(
        self,
        eventhub_namespace: str,
        eventhub_name: str,
        consumer_group: str = "$Default",
        use_key_vault: bool = True,
        keyvault_name: Optional[str] = None,
        connection_string_secret: Optional[str] = None,
        connection_string: Optional[str] = None
    ):
        pass