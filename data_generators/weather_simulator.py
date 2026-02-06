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
        """Generate a weather reading"""
        if self.condition_duration == 0:
            self.current_condition = random.choices(
                ["clear", "cloudy", "rain", "heavy_rain", "snow", "fog"],
                weights=[0.4, 0.3, 0.15, 0.05, 0.05, 0.05]
            )[0]
            self.condition_duration = random.randint(4, 20)  # in intervals
        
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
    CONNECTION_STRING = CONNECTION_STRING = dbutils.secrets.get(scope="smartcity-secrets", key="eventhub-connection-string")
    publish_weather_data(CONNECTION_STRING, "weather-events")