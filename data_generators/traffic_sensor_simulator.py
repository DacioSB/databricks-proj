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
from utils.config_loader import get_config

#import azure.eventhub
#import azure.core

#print(azure.eventhub.__version__)
#print(azure.core.__version__)

config = get_config()

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
            "night": (0, 6, 0.2),
            "morning_rush": (6, 9, 1.5),
            "midday": (9, 16, 0.8),
            "evening_rush": (16, 19, 1.6),
            "evening": (19, 24, 0.6)
        }
    def get_traffic_multiplier(self, timestamp: datetime) -> float:
        hour = timestamp.hour
        for _, (start_hour, end_hour, multiplier) in self.time_of_day_patterns.items():
            if start_hour <= hour < end_hour:
                return multiplier * random.uniform(0.8, 1.2)
        return 1.0
    def get_weather_impact(self) -> float:
        """Simulate weather impact on traffic"""
        # TODO: In production, this would call a weather API
        weather_conditions = {
            "clear": 1.0,
            "rain": 0.7,
            "heavy_rain": 0.5,
            "snow": 0.4,
            "fog": 0.6
        }

        condition = random.choices(
            list(weather_conditions.keys()),
            weights=[0.6, 0.2, 0.05, 0.05, 0.1],
        )[0]

        return weather_conditions[condition]
    def generate_reading(self, intersection: Intersection, timestamp: datetime) -> TrafficReading:
        """Generate a single traffic reading"""
        base_capacity = (intersection.lanes_north_south + intersection.lanes_east_west) * 10
        time_multiplier = self.get_traffic_multiplier(timestamp)
        # District multiplier
        district_multipliers = {
            "downtown": 1.5,
            "residential": 0.7,
            "industrial": 1.2,
            "suburban": 0.5
        }
        district_mult = district_multipliers[intersection.district]
        weather_mult = self.get_weather_impact()

        vehicle_count = int(base_capacity * time_multiplier * district_mult * weather_mult)
        vehicle_count = max(0, vehicle_count + random.randint(-5, 5))

        max_capacity = base_capacity * 2
        occupancy_rate = min(1.0, vehicle_count / max_capacity)

        # Average speed (inversely proportional to occupancy)
        base_speed = 35 #mph
        average_speed = base_speed * (1 - occupancy_rate * 0.7)
        average_speed = max(5, average_speed)
        
        # Vehicle type distribution
        total_vehicles = vehicle_count
        vehicle_types = {
            "car": int(total_vehicles * random.uniform(0.75, 0.85)),
            "truck": int(total_vehicles * random.uniform(0.05, 0.12)),
            "motorcycle": int(total_vehicles * random.uniform(0.02, 0.05)),
            "bus": int(total_vehicles * random.uniform(0.01, 0.03))
        }
        vehicle_types["car"] = total_vehicles - sum(
            [v for k, v in vehicle_types.items() if k != "car"]
        )

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
    def __init__(self, event_hub_conn_string: str, event_hub_name: str):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=event_hub_conn_string,
            eventhub_name=event_hub_name
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

        if len(event_data_batch) > 0:
            self.producer.send_batch(event_data_batch)
    def close(self):
        """Close the producer"""
        self.producer.close()

def main():
    """
    Main function
    """
    CONNECTION_STRING = dbutils.secrets.get(scope="smartcity-secrets", key="eventhub-connection-string")
    EVENTHUB_NAME = "traffic-sensors"

    city_grid = CityGrid(
        city_center=(40.7128, -74.0060),  # New York City
        grid_size=10
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

            readings = []

            for intersection in city_grid.intersections:
                reading = simulator.generate_reading(intersection, current_time)
                readings.append(reading)

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

            time.sleep(30)

    except KeyboardInterrupt:
        print("🚦 Traffic Simulator Stopped")
    finally:
        publisher.close()
        print("🚦 Traffic Simulator Stopped")

if __name__ == "__main__":
    main()