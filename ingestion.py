import requests
from datetime import datetime, timedelta
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# === AIRCRAFT TELEMETRY from Opensky Network===
def get_opensky_telemetry(limit=5): # connects to Opensky Rest API and gets aircraft telemetry
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get("states", [])
            aircrafts = []
            for item in data[:limit]:
                aircrafts.append({
                    "icao24": item[0],
                    "callsign": item[1].strip() if item[1] else "N/A",
                    "country": item[2],
                    "longitude": item[5],
                    "latitude": item[6],
                    "velocity": item[9],
                    "heading": item[10],
                    "altitude": item[13]
                })
            return aircrafts
        else:
            print(f"❌ OpenSky failed: {response.status_code}")
            return []
    except Exception as e:
        print("❌ OpenSky error:", e)
        return []

# === live METAR WEATHER data from NOAA ===
def get_metar_data(airport_codes=["KATL", "JFK", "LAX"]):
    base_url = "https://aviationweather.gov/api/data/metar"
    params = {
        "ids": ",".join(airport_codes),
        "format": "json"
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ METAR failed: {response.status_code}")
            return []
    except Exception as e:
        print("❌ METAR error:", e)
        return []

# === Generates SIMULATED NOTAMs (airspace restriction notices) ===
def generate_simulated_notams():
    now = datetime.utcnow()
    return [
        {
            "id": "NOTAM001",
            "message": "Restricted airspace over Sector S1 due to military activity",
            "sector_id": "S1",
            "effective_from": now.isoformat(),
            "effective_to": (now + timedelta(hours=2)).isoformat()
        },
        {
            "id": "NOTAM002",
            "message": "Avoid Sector S2 due to severe weather",
            "sector_id": "S2",
            "effective_from": now.isoformat(),
            "effective_to": (now + timedelta(hours=1, minutes=30)).isoformat()
        }
    ]

# === NEO4J LOADER ===
class AirspaceGraph: # connects to Neo4j and inserts nodes and relationships into the Knowledge Graph

    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def close(self):
        self.driver.close()

    def insert_aircraft(self, aircrafts): # loops through aircraft list and inserts as (:Aircraft) nodes
        with self.driver.session() as session:
            for ac in aircrafts:
                session.execute_write(self._create_aircraft_node, ac)

    def insert_weather(self, metar_data): # inserts METAR reports as (:WeatherCell) nodes
        with self.driver.session() as session:
            for wx in metar_data:
                session.execute_write(self._create_weather_node, wx)

    def insert_notams(self, notam_list): # inserts NOTAMs and links them to Sector nodes via (:NOTAM)-[:restricts]->(:Sector)
        with self.driver.session() as session:
            for notam in notam_list:
                session.execute_write(self._create_notam_node, notam)

    def insert_sectors(self): # creates 2 static sectors: NorthZone (S1) and EastZone (S2)
        sectors = [
            {"id": "S1", "name": "NorthZone"},
            {"id": "S2", "name": "EastZone"}
        ]
        with self.driver.session() as session:
            for s in sectors:
                session.execute_write(self._create_sector_node, s)

    @staticmethod
    def _create_sector_node(tx, sector):
        tx.run("""
            MERGE (:Sector {id: $id, name: $name})
        """, id=sector["id"], name=sector["name"])

    
    # === Node Creation ===
    @staticmethod
    def _create_aircraft_node(tx, ac):
        tx.run("""
            MERGE (a:Aircraft {icao24: $icao24})
            SET a.callsign = $callsign,
                a.country = $country,
                a.latitude = $lat,
                a.longitude = $lon,
                a.altitude = $alt,
                a.velocity = $vel,
                a.heading = $hdg
        """, icao24=ac["icao24"], callsign=ac["callsign"], country=ac["country"],
             lat=ac["latitude"], lon=ac["longitude"], alt=ac["altitude"],
             vel=ac["velocity"], hdg=ac["heading"])

    @staticmethod
    def _create_notam_node(tx, notam):
        tx.run("""
            MERGE (n:NOTAM {id: $id})
            SET n.message = $message,
                n.effective_from = $from_time,
                n.effective_to = $to_time
            WITH n
            MATCH (s:Sector {id: $sector_id})
            MERGE (n)-[:restricts]->(s)
        """, 
        id=notam["id"],
        message=notam["message"],
        from_time=notam["effective_from"],
        to_time=notam["effective_to"],
        sector_id=notam["sector_id"])

    @staticmethod
    def _create_weather_node(tx, wx):
        tx.run("""
            MERGE (w:WeatherCell {station: $station})
            SET w.raw = $raw,
                w.observed_at = $obs
        """, 
        station=wx.get("station_id", "UNKNOWN"),
        raw=wx.get("raw_text", "N/A"),
        obs=wx.get("observation_time", "N/A"))



# === MAIN PIPELINE ===
def main():
    print("🔄 Fetching real-time aircraft data...")
    aircraft_data = get_opensky_telemetry()

    print("🔄 Fetching METAR weather...")
    metar_data = get_metar_data()

    print("🛑 Generating simulated NOTAMs...")
    notams = generate_simulated_notams()

    print("🔗 Inserting all data into Neo4j...")
    kg = AirspaceGraph()
    kg.insert_sectors()               # Ensure sectors exist
    kg.insert_aircraft(aircraft_data)
    kg.insert_weather(metar_data)
    kg.insert_notams(notams)
    kg.close()

    print("✅ Done: Aircraft, Weather, Sectors, and NOTAMs inserted into Neo4j.")

# === EXECUTE ===
if __name__ == "__main__":
    main()
