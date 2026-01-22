# Building a Production-Ready Smart City Traffic Management System with Azure Databricks

## A Complete End-to-End IoT Analytics Platform

*Learn how to build a real-time traffic analytics system using Azure Databricks, Delta Lake, and Azure Data Factory*

---

## Table of Contents
1. [Introduction & Business Case](#introduction)
2. [Architecture Overview](#architecture)
3. [Prerequisites & Setup](#prerequisites)
4. [Part 1: Infrastructure Setup](#part-1)
5. [Part 2: Data Generation & Ingestion](#part-2)
6. [Part 3: Delta Lake Implementation (Medallion Architecture)](#part-3)
7. [Part 4: Real-Time Stream Processing](#part-4)
8. [Part 5: Advanced Analytics & ML Models](#part-5)
9. [Part 6: Dashboards & Visualization](#part-6)
10. [Part 7: Monitoring & Optimization](#part-7)
11. [Conclusion & Next Steps](#conclusion)

---

## <a name="introduction"></a>1. Introduction & Business Case

### The Problem
Modern cities face critical challenges:
- Traffic congestion costs the US economy **$166 billion annually**
- Average commuter loses **54 hours per year** in traffic
- Emergency response times are unpredictable
- Air quality deteriorates due to idling vehicles

### Our Solution
A real-time traffic management platform that:
- ✅ Processes **1 million+ sensor events per minute**
- ✅ Predicts traffic patterns **30-60 minutes ahead**
- ✅ Detects accidents within **seconds**
- ✅ Optimizes traffic light timing dynamically
- ✅ Provides city planners with actionable insights

### What You'll Build
By the end of this tutorial, you'll have:
- A complete streaming data pipeline
- Delta Lake medallion architecture (Bronze → Silver → Gold)
- Real-time ML model inference
- Interactive dashboards
- Production-ready monitoring

**Time to Complete**: 4-6 hours  
**Skill Level**: Intermediate  
**Cost**: ~$50-100 (can be minimized with Azure credits)

---

## <a name="architecture"></a>2. Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
├─────────────────────────────────────────────────────────────────────┤
│  IoT Sensors  │  Weather API  │  Event Calendar  │  Traffic Cameras │
└────────┬────────────┬────────────────┬─────────────────┬─────────────┘
         │            │                │                 │
         v            v                v                 v
    ┌────────────────────────────────────────────────────────┐
    │           Azure IoT Hub / Event Hubs                   │
    └────────────────────┬───────────────────────────────────┘
                         │
                         v
    ┌────────────────────────────────────────────────────────┐
    │        Azure Databricks (Structured Streaming)         │
    │  ┌──────────────────────────────────────────────┐     │
    │  │  BRONZE Layer (Raw Data - Delta Lake)        │     │
    │  └──────────────┬───────────────────────────────┘     │
    │                 v                                       │
    │  ┌──────────────────────────────────────────────┐     │
    │  │  SILVER Layer (Cleansed & Validated)         │     │
    │  └──────────────┬───────────────────────────────┘     │
    │                 v                                       │
    │  ┌──────────────────────────────────────────────┐     │
    │  │  GOLD Layer (Business-Level Aggregates)      │     │
    │  └──────────────┬───────────────────────────────┘     │
    └─────────────────┼───────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         v                         v
┌─────────────────┐       ┌──────────────────┐
│   ML Models     │       │  Power BI        │
│  (MLflow)       │       │  Dashboards      │
└─────────────────┘       └──────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Generation** | Python Script | Simulate IoT sensors |
| **Ingestion** | Azure Event Hubs | Message broker for streaming data |
| **Stream Processing** | Databricks Structured Streaming | Real-time data processing |
| **Storage** | Azure Data Lake Gen2 + Delta Lake | Scalable, ACID-compliant storage |
| **Batch Orchestration** | Azure Data Factory | Schedule and monitor pipelines |
| **Analytics** | Apache Spark (PySpark) | Distributed data processing |
| **ML Platform** | MLflow on Databricks | Model training and deployment |
| **Visualization** | Databricks SQL + Power BI | Interactive dashboards |
| **Monitoring** | Azure Monitor + Log Analytics | Platform health monitoring |

---

## <a name="prerequisites"></a>3. Prerequisites & Setup

### Azure Resources Required

```bash
# Estimated Monthly Cost Breakdown:
# - Azure Databricks (Standard): ~$200-300
# - Event Hubs (Standard): ~$25
# - Data Lake Storage Gen2: ~$10-20
# - Azure Data Factory: ~$10
# Total: ~$245-355/month (can use free trials/credits)
```

### 3.1 Azure Subscription Setup

1. **Get Azure Credits** (if you don't have them):
   - New users: $200 free credit
   - Students: $100 via Azure for Students
   - Already have subscription? Proceed to next step

2. **Create Resource Group**:
   ```bash
   # Via Azure CLI
   az login
   az group create \
     --name rg-smartcity-traffic \
     --location eastus2
   ```

### 3.2 Required Azure Services

#### A. Azure Data Lake Storage Gen2

```bash
# Create storage account
az storage account create \
  --name sasctraffic<yourname> \
  --resource-group rg-smartcity-traffic \
  --location eastus2 \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create containers
az storage container create \
  --name bronze \
  --account-name sasctraffic<yourname>

az storage container create \
  --name silver \
  --account-name sasctraffic<yourname>

az storage container create \
  --name gold \
  --account-name sasctraffic<yourname>

az storage container create \
  --name checkpoints \
  --account-name sasctraffic<yourname>
```

#### B. Azure Event Hubs

```bash
# Create Event Hubs namespace
az eventhubs namespace create \
  --name ehn-smartcity-traffic \
  --resource-group rg-smartcity-traffic \
  --location eastus2 \
  --sku Standard \
  --capacity 1

# Create Event Hubs
az eventhubs eventhub create \
  --name traffic-sensors \
  --namespace-name ehn-smartcity-traffic \
  --resource-group rg-smartcity-traffic \
  --partition-count 4 \
  --message-retention 1

az eventhubs eventhub create \
  --name weather-events \
  --namespace-name ehn-smartcity-traffic \
  --resource-group rg-smartcity-traffic \
  --partition-count 2 \
  --message-retention 1

# Get connection string (save this!)
az eventhubs namespace authorization-rule keys list \
  --resource-group rg-smartcity-traffic \
  --namespace-name ehn-smartcity-traffic \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString \
  --output tsv
```

#### C. Azure Databricks Workspace

```bash
# Create Databricks workspace
az databricks workspace create \
  --name dbw-smartcity-traffic \
  --resource-group rg-smartcity-traffic \
  --location eastus2 \
  --sku premium
```

**Manual Steps in Azure Portal**:
1. Navigate to your Databricks workspace
2. Click "Launch Workspace"
3. Generate a Personal Access Token:
   - Click User Settings (top right)
   - Developer → Access Tokens → Generate New Token
   - **Save this token securely!**

### 3.3 Local Development Setup

```bash
# Install required tools
pip install azure-eventhub==5.11.4
pip install azure-identity==1.14.0
pip install pandas==2.0.3
pip install faker==19.6.2
pip install databricks-cli==0.18.0

# Configure Databricks CLI
databricks configure --token
# Enter your workspace URL: https://adb-<workspace-id>.azuredatabricks.net
# Enter your token: <paste-token-here>
```

### 3.4 Project Structure

Create the following folder structure locally:

```
smartcity-traffic/
├── config/
│   ├── settings.yaml
│   └── sensor_config.json
├── data_generators/
│   ├── traffic_sensor_simulator.py
│   ├── weather_simulator.py
│   └── event_generator.py
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.py
│   │   ├── 02_silver_transformation.py
│   │   ├── 03_gold_aggregation.py
│   │   ├── 04_ml_traffic_prediction.py
│   │   └── 05_anomaly_detection.py
│   └── jobs/
│       └── pipeline_config.json
├── adf/
│   └── pipelines/
│       └── daily_batch_pipeline.json
├── dashboards/
│   └── traffic_dashboard.sql
└── README.md
```

---

## <a name="part-1"></a>4. Part 1: Infrastructure Setup

### 4.1 Create Databricks Cluster

**Via Databricks UI**:

1. Navigate to **Compute** → **Create Cluster**
2. Use these settings:

```yaml
Cluster Name: traffic-analytics-cluster
Cluster Mode: Standard
Databricks Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1)
Autopilot: Enabled
Worker Type: Standard_DS3_v2 (14 GB Memory, 4 Cores)
Min Workers: 2
Max Workers: 8
Enable autoscaling: Yes
Terminate after: 120 minutes of inactivity
```

**Advanced Options**:
```yaml
Spark Config:
  spark.databricks.delta.preview.enabled: true
  spark.databricks.delta.optimizeWrite.enabled: true
  spark.databricks.delta.autoCompact.enabled: true
  
Environment Variables:
  PYSPARK_PYTHON: /databricks/python3/bin/python3
```

3. **Install Libraries** on the cluster:
   - `azure-eventhub==5.11.4`
   - `mlflow==2.8.0`
   - `geopy==2.4.0`
   - `folium==0.14.0`

### 4.2 Configure Azure Key Vault (Security Best Practice)

```bash
# Create Key Vault
az keyvault create \
  --name kv-smartcity-traffic \
  --resource-group rg-smartcity-traffic \
  --location eastus2

# Store secrets
az keyvault secret set \
  --vault-name kv-smartcity-traffic \
  --name eventhub-connection-string \
  --value "<your-connection-string>"

az keyvault secret set \
  --vault-name kv-smartcity-traffic \
  --name storage-account-key \
  --value "<your-storage-key>"
```

**Link Key Vault to Databricks**:

In Databricks, create a secret scope:

```python
# In Databricks notebook
# Navigate to: https://<databricks-instance>#secrets/createScope
# Scope Name: smartcity-secrets
# DNS Name: https://kv-smartcity-traffic.vault.azure.net/
# Resource ID: /subscriptions/<sub-id>/resourceGroups/rg-smartcity-traffic/providers/Microsoft.KeyVault/vaults/kv-smartcity-traffic
```

### 4.3 Mount Azure Data Lake to Databricks

Create notebook: `00_mount_storage.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Storage Gen2 to Databricks

# COMMAND ----------

# Configuration
storage_account_name = "sasctraffic<yourname>"
container_names = ["bronze", "silver", "gold", "checkpoints"]
mount_point_base = "/mnt/smartcity"

# Get storage account key from Key Vault
storage_account_key = dbutils.secrets.get(scope="smartcity-secrets", key="storage-account-key")

# COMMAND ----------

def mount_container(container_name):
    """Mount a container from ADLS Gen2"""
    mount_point = f"{mount_point_base}/{container_name}"
    
    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"✓ {mount_point} already mounted")
        return
    
    # Mount configuration
    configs = {
        f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": storage_account_key
    }
    
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    
    try:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✓ Successfully mounted {mount_point}")
    except Exception as e:
        print(f"✗ Error mounting {mount_point}: {str(e)}")

# COMMAND ----------

# Mount all containers
for container in container_names:
    mount_container(container)

# COMMAND ----------

# Verify mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# Test write access
test_path = f"{mount_point_base}/bronze/test.txt"
dbutils.fs.put(test_path, "Mount test successful!", overwrite=True)
print(f"✓ Write test successful: {test_path}")

# Read back
content = dbutils.fs.head(test_path)
print(f"✓ Read test successful: {content}")

# Cleanup
dbutils.fs.rm(test_path)
```

---

## <a name="part-2"></a>5. Part 2: Data Generation & Ingestion

### 5.1 Traffic Sensor Simulator

Create `data_generators/traffic_sensor_simulator.py`:

```python
"""
Traffic Sensor Simulator
Generates realistic traffic data for smart city simulation
"""

import json
import time
import random
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData
from dataclasses import dataclass, asdict
from typing import List, Tuple
import math

@dataclass
class Intersection:
    """Represents a traffic intersection"""
    intersection_id: str
    name: str
    latitude: float
    longitude: float
    lanes_north_south: int
    lanes_east_west: int
    has_camera: bool
    district: str

@dataclass
class TrafficReading:
    """Single traffic sensor reading"""
    sensor_id: str
    intersection_id: str
    timestamp: str
    vehicle_count: int
    average_speed: float  # mph
    occupancy_rate: float  # 0-1
    vehicle_types: dict  # {car: 45, truck: 3, motorcycle: 2, bus: 1}
    wait_time_seconds: float
    queue_length: int
    signal_state: str  # red, yellow, green
    latitude: float
    longitude: float
    district: str

class CityGrid:
    """Generates realistic city intersection grid"""
    
    def __init__(self, city_center: Tuple[float, float], grid_size: int = 10):
        self.city_center = city_center
        self.grid_size = grid_size
        self.intersections = self._generate_intersections()
    
    def _generate_intersections(self) -> List[Intersection]:
        """Generate grid of intersections"""
        intersections = []
        
        # Districts with different characteristics
        districts = {
            "downtown": {"traffic_multiplier": 1.5, "camera_prob": 0.8},
            "residential": {"traffic_multiplier": 0.7, "camera_prob": 0.3},
            "industrial": {"traffic_multiplier": 1.2, "camera_prob": 0.5},
            "suburban": {"traffic_multiplier": 0.5, "camera_prob": 0.2}
        }
        
        lat_base, lon_base = self.city_center
        
        for i in range(self.grid_size):
            for j in range(self.grid_size):
                # Offset from center (approx 0.01 degrees = 1.1 km)
                lat = lat_base + (i - self.grid_size/2) * 0.01
                lon = lon_base + (j - self.grid_size/2) * 0.01
                
                # Determine district based on distance from center
                distance = math.sqrt(i**2 + j**2)
                if distance < 3:
                    district = "downtown"
                elif distance < 5:
                    district = "residential"
                elif distance < 7:
                    district = "industrial"
                else:
                    district = "suburban"
                
                district_config = districts[district]
                
                intersection = Intersection(
                    intersection_id=f"INT-{i:02d}{j:02d}",
                    name=f"{chr(65+i)} St & {j+1} Ave",
                    latitude=round(lat, 6),
                    longitude=round(lon, 6),
                    lanes_north_south=random.choice([2, 3, 4]),
                    lanes_east_west=random.choice([2, 3, 4]),
                    has_camera=random.random() < district_config["camera_prob"],
                    district=district
                )
                intersections.append(intersection)
        
        return intersections

class TrafficSimulator:
    """Simulates realistic traffic patterns"""
    
    def __init__(self, city_grid: CityGrid):
        self.city_grid = city_grid
        self.time_of_day_patterns = {
            "night": (0, 6, 0.2),      # midnight-6am: 20% capacity
            "morning_rush": (6, 9, 1.5),  # 6am-9am: 150% capacity
            "midday": (9, 16, 0.8),    # 9am-4pm: 80% capacity
            "evening_rush": (16, 19, 1.6), # 4pm-7pm: 160% capacity
            "evening": (19, 24, 0.6)   # 7pm-midnight: 60% capacity
        }
    
    def get_traffic_multiplier(self, timestamp: datetime) -> float:
        """Calculate traffic multiplier based on time of day"""
        hour = timestamp.hour
        
        for period, (start, end, multiplier) in self.time_of_day_patterns.items():
            if start <= hour < end:
                # Add some randomness
                return multiplier * random.uniform(0.8, 1.2)
        
        return 1.0
    
    def get_weather_impact(self) -> float:
        """Simulate weather impact on traffic"""
        # In production, this would call a real weather API
        weather_conditions = {
            "clear": 1.0,
            "rain": 0.7,
            "heavy_rain": 0.5,
            "snow": 0.4,
            "fog": 0.6
        }
        
        # Weighted random selection
        condition = random.choices(
            list(weather_conditions.keys()),
            weights=[0.6, 0.2, 0.05, 0.05, 0.1]
        )[0]
        
        return weather_conditions[condition]
    
    def generate_reading(self, intersection: Intersection, timestamp: datetime) -> TrafficReading:
        """Generate a single traffic reading"""
        
        # Base traffic for intersection
        base_capacity = (intersection.lanes_north_south + intersection.lanes_east_west) * 10
        
        # Apply time-of-day pattern
        time_multiplier = self.get_traffic_multiplier(timestamp)
        
        # District multiplier
        district_multipliers = {
            "downtown": 1.5,
            "residential": 0.7,
            "industrial": 1.2,
            "suburban": 0.5
        }
        district_mult = district_multipliers[intersection.district]
        
        # Weather impact
        weather_mult = self.get_weather_impact()
        
        # Calculate vehicle count
        vehicle_count = int(base_capacity * time_multiplier * district_mult * weather_mult)
        vehicle_count = max(0, vehicle_count + random.randint(-5, 5))
        
        # Calculate occupancy (percentage of intersection occupied)
        max_capacity = base_capacity * 2
        occupancy_rate = min(1.0, vehicle_count / max_capacity)
        
        # Average speed (inversely proportional to occupancy)
        base_speed = 35  # mph
        average_speed = base_speed * (1 - occupancy_rate * 0.7)
        average_speed = max(5, average_speed)  # minimum 5 mph
        
        # Vehicle type distribution
        total_vehicles = vehicle_count
        vehicle_types = {
            "car": int(total_vehicles * random.uniform(0.75, 0.85)),
            "truck": int(total_vehicles * random.uniform(0.05, 0.12)),
            "motorcycle": int(total_vehicles * random.uniform(0.02, 0.05)),
            "bus": int(total_vehicles * random.uniform(0.01, 0.03))
        }
        # Ensure sum equals total
        vehicle_types["car"] = total_vehicles - sum([v for k, v in vehicle_types.items() if k != "car"])
        
        # Wait time and queue length (higher when congested)
        wait_time = occupancy_rate * random.uniform(30, 120)  # seconds
        queue_length = int(occupancy_rate * base_capacity * 0.5)
        
        # Signal state (simplified)
        signal_state = random.choice(["red", "yellow", "green"])
        
        return TrafficReading(
            sensor_id=f"{intersection.intersection_id}-SENSOR-01",
            intersection_id=intersection.intersection_id,
            timestamp=timestamp.isoformat(),
            vehicle_count=vehicle_count,
            average_speed=round(average_speed, 2),
            occupancy_rate=round(occupancy_rate, 3),
            vehicle_types=vehicle_types,
            wait_time_seconds=round(wait_time, 1),
            queue_length=queue_length,
            signal_state=signal_state,
            latitude=intersection.latitude,
            longitude=intersection.longitude,
            district=intersection.district
        )

class EventHubPublisher:
    """Publishes traffic data to Azure Event Hubs"""
    
    def __init__(self, connection_string: str, eventhub_name: str):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            eventhub_name=eventhub_name
        )
    
    def send_batch(self, readings: List[TrafficReading]):
        """Send batch of readings to Event Hub"""
        event_data_batch = self.producer.create_batch()
        
        for reading in readings:
            event_data = EventData(json.dumps(asdict(reading)))
            try:
                event_data_batch.add(event_data)
            except ValueError:
                # Batch is full, send it and create a new one
                self.producer.send_batch(event_data_batch)
                event_data_batch = self.producer.create_batch()
                event_data_batch.add(event_data)
        
        # Send remaining events
        if len(event_data_batch) > 0:
            self.producer.send_batch(event_data_batch)
    
    def close(self):
        """Close the producer"""
        self.producer.close()

def main():
    """Main simulation loop"""
    
    # Configuration
    CONNECTION_STRING = "Endpoint=sb://ehn-smartcity-traffic.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY"
    EVENTHUB_NAME = "traffic-sensors"
    
    # Initialize
    city_grid = CityGrid(
        city_center=(40.7128, -74.0060),  # New York coordinates
        grid_size=10  # 10x10 grid = 100 intersections
    )
    
    simulator = TrafficSimulator(city_grid)
    publisher = EventHubPublisher(CONNECTION_STRING, EVENTHUB_NAME)
    
    print(f"🚦 Traffic Simulator Started")
    print(f"📍 Monitoring {len(city_grid.intersections)} intersections")
    print(f"📡 Publishing to Event Hub: {EVENTHUB_NAME}")
    print("-" * 60)
    
    try:
        iteration = 0
        while True:
            iteration += 1
            current_time = datetime.now()
            
            # Generate readings for all intersections
            readings = []
            for intersection in city_grid.intersections:
                reading = simulator.generate_reading(intersection, current_time)
                readings.append(reading)
            
            # Publish to Event Hub
            publisher.send_batch(readings)
            
            # Stats
            total_vehicles = sum(r.vehicle_count for r in readings)
            avg_speed = sum(r.average_speed for r in readings) / len(readings)
            avg_occupancy = sum(r.occupancy_rate for r in readings) / len(readings)
            
            print(f"[{current_time.strftime('%H:%M:%S')}] Iteration {iteration}")
            print(f"  📊 Total Vehicles: {total_vehicles:,}")
            print(f"  🚗 Avg Speed: {avg_speed:.1f} mph")
            print(f"  📈 Avg Occupancy: {avg_occupancy:.1%}")
            print(f"  ✉️  Events Sent: {len(readings)}")
            print("-" * 60)
            
            # Wait before next iteration (simulate 30-second intervals)
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n⏹️  Simulation stopped by user")
    finally:
        publisher.close()
        print("✓ Publisher closed")

if __name__ == "__main__":
    main()
```

### 5.2 Weather Data Simulator

Create `data_generators/weather_simulator.py`:

```python
"""
Weather Data Simulator
Generates weather events that impact traffic
"""

import json
import time
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from dataclasses import dataclass, asdict

@dataclass
class WeatherReading:
    """Weather observation"""
    station_id: str
    timestamp: str
    temperature_f: float
    humidity: float
    precipitation_rate: float  # inches/hour
    visibility_miles: float
    wind_speed_mph: float
    condition: str  # clear, rain, snow, fog, etc.
    latitude: float
    longitude: float

class WeatherSimulator:
    """Simulates weather patterns"""
    
    def __init__(self, city_center: tuple):
        self.city_center = city_center
        self.current_condition = "clear"
        self.condition_duration = 0
    
    def generate_weather(self) -> WeatherReading:
        """Generate weather reading"""
        
        # Determine if weather should change
        if self.condition_duration == 0:
            self.current_condition = random.choices(
                ["clear", "cloudy", "rain", "heavy_rain", "snow", "fog"],
                weights=[0.4, 0.3, 0.15, 0.05, 0.05, 0.05]
            )[0]
            self.condition_duration = random.randint(4, 20)  # duration in intervals
        
        self.condition_duration -= 1
        
        # Base values by condition
        condition_params = {
            "clear": {"temp": (65, 85), "humidity": (0.3, 0.5), "precip": 0, "visibility": 10},
            "cloudy": {"temp": (60, 75), "humidity": (0.5, 0.7), "precip": 0, "visibility": 10},
            "rain": {"temp": (55, 70), "humidity": (0.7, 0.9), "precip": 0.1, "visibility": 5},
            "heavy_rain": {"temp": (50, 65), "humidity": (0.85, 0.95), "precip": 0.5, "visibility": 2},
            "snow": {"temp": (20, 35), "humidity": (0.7, 0.9), "precip": 0.2, "visibility": 3},
            "fog": {"temp": (55, 65), "humidity": (0.9, 1.0), "precip": 0, "visibility": 0.5}
        }
        
        params = condition_params[self.current_condition]
        
        return WeatherReading(
            station_id="WEATHER-CENTRAL-01",
            timestamp=datetime.now().isoformat(),
            temperature_f=round(random.uniform(*params["temp"]), 1),
            humidity=round(random.uniform(*params["humidity"]), 2),
            precipitation_rate=params["precip"] * random.uniform(0.8, 1.2),
            visibility_miles=params["visibility"] * random.uniform(0.9, 1.1),
            wind_speed_mph=round(random.uniform(0, 25), 1),
            condition=self.current_condition,
            latitude=self.city_center[0],
            longitude=self.city_center[1]
        )

def publish_weather_data(connection_string: str, eventhub_name: str):
    """Publish weather data to Event Hub"""
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name
    )
    
    simulator = WeatherSimulator(city_center=(40.7128, -74.0060))
    
    print("🌤️  Weather Simulator Started")
    print("-" * 60)
    
    try:
        iteration = 0
        while True:
            iteration += 1
            weather = simulator.generate_weather()
            
            # Send to Event Hub
            event_data = EventData(json.dumps(asdict(weather)))
            producer.send_batch([event_data])
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Iteration {iteration}")
            print(f"  🌡️  {weather.condition.upper()}: {weather.temperature_f}°F")
            print(f"  💧 Precipitation: {weather.precipitation_rate:.2f} in/hr")
            print(f"  👁️  Visibility: {weather.visibility_miles:.1f} miles")
            print("-" * 60)
            
            # Update every 5 minutes
            time.sleep(300)
            
    except KeyboardInterrupt:
        print("\n⏹️  Weather simulation stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    CONNECTION_STRING = "YOUR_EVENT_HUB_CONNECTION_STRING"
    publish_weather_data(CONNECTION_STRING, "weather-events")
```

### 5.3 Configuration File

Create `config/settings.yaml`:

```yaml
# Smart City Traffic Configuration

azure:
  subscription_id: "your-subscription-id"
  resource_group: "rg-smartcity-traffic"
  location: "eastus2"

storage:
  account_name: "sasctraffic<yourname>"
  containers:
    bronze: "bronze"
    silver: "silver"
    gold: "gold"
    checkpoints: "checkpoints"
  mount_points:
    bronze: "/mnt/smartcity/bronze"
    silver: "/mnt/smartcity/silver"
    gold: "/mnt/smartcity/gold"

eventhub:
  namespace: "ehn-smartcity-traffic"
  hubs:
    traffic_sensors: "traffic-sensors"
    weather_events: "weather-events"
  consumer_group: "$Default"

databricks:
  workspace_url: "https://adb-xxxxx.azuredatabricks.net"
  cluster_id: "your-cluster-id"

simulation:
  city_center:
    latitude: 40.7128
    longitude: -74.0060
  grid_size: 10
  sensor_interval_seconds: 30
  weather_interval_seconds: 300

medallion_architecture:
  bronze:
    path: "/mnt/smartcity/bronze/traffic"
    checkpoint: "/mnt/smartcity/checkpoints/bronze"
    format: "delta"
    mode: "append"
  silver:
    path: "/mnt/smartcity/silver/traffic"
    checkpoint: "/mnt/smartcity/checkpoints/silver"
    format: "delta"
    mode: "append"
  gold:
    aggregations:
      - name: "hourly_summary"
        path: "/mnt/smartcity/gold/hourly_summary"
      - name: "intersection_metrics"
        path: "/mnt/smartcity/gold/intersection_metrics"
      - name: "district_analytics"
        path: "/mnt/smartcity/gold/district_analytics"
```

---

**This is Part 1 of the tutorial. Would you like me to continue with:**
- **Part 3: Delta Lake Implementation (Bronze/Silver/Gold layers)**
- **Part 4: Real-Time Stream Processing**
- **Part 5: ML Models for Traffic Prediction**
- **Part 6: Dashboards & Visualization**
- **Part 7: Production Deployment & Monitoring**

Let me know which section you'd like next, or if you want me to continue sequentially!

