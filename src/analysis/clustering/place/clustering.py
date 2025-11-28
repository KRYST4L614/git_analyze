import concurrent
import multiprocessing as mp
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Any

import cachetools
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.geocoders import Nominatim

from src.analysis.clustering.place.repo.repo import LocationRepository
from src.storage.unit_of_work import UnitOfWork

warnings.filterwarnings('ignore')


class GeoLocationService:
    def __init__(self, cache_size: int = 1000):
        self.geolocator = Nominatim(user_agent="contributor_analysis")
        self.cache = cachetools.LRUCache(maxsize=cache_size)
        self.last_request_time = 0

    def geocode_location(self, location_str: str) -> Optional[Dict]:
        if not location_str or location_str.lower() in ['', 'unknown', 'none', 'null']:
            return None

        cache_key = location_str.lower().strip()
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            location = self.geolocator.geocode(
                location_str,
                addressdetails=True,
                language='en',
                timeout=60,
            )

            if location:
                result = {
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'address': location.raw.get('address', {}),
                    'raw': location.raw
                }
                self.cache[cache_key] = result
                return result

        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Geocoding error for '{location_str}': {e}")
        except Exception as e:
            print(f"Unexpected error for '{location_str}': {e}")

        return None

    def extract_country_from_geodata(self, geodata: Dict) -> str:
        if not geodata:
            return None

        address = geodata.get('address', {})
        country = address.get('country', '')

        if country:
            return country.lower()

        return None

    def extract_city_from_geodata(self, geodata: Dict) -> str:
        if not geodata:
            return None

        address = geodata.get('address', {})

        city_fields = ['city', 'town', 'village', 'municipality']
        for field in city_fields:
            if field in address and address[field]:
                return address[field].lower()

        return None


class ContributorLocationClustering:
    def __init__(self, database_url: str, location_repo: LocationRepository, n_workers: int = None):
        self.database_url = database_url
        self.location_repo = location_repo
        self.n_workers = n_workers or mp.cpu_count()
        self.geo_service = GeoLocationService()

    def _process_contributor_chunk(self, contributor_chunk, database_url, chunk_id):

        uow = UnitOfWork(database_url)
        self.location_repo.uow = uow

        geo_service = GeoLocationService()

        successful_count = 0

        try:
            print(f"Chunk {chunk_id}: Processing {len(contributor_chunk)} contributors")

            for _, row in contributor_chunk.iterrows():
                contributor_id = row['contributor_id']
                original_location = str(row['location'] or '')

                if not original_location or original_location.strip() == '':
                    continue

                geodata = geo_service.geocode_location(original_location)

                if not geodata:
                    continue

                country = geo_service.extract_country_from_geodata(geodata)
                city = geo_service.extract_city_from_geodata(geodata)

                if not country:
                    continue

                try:
                    success = self.location_repo.save_contributor_location(
                        contributor_id=contributor_id,
                        original_location=original_location,
                        country_name=country,
                        city_name=city,
                        latitude=geodata.get('latitude'),
                        longitude=geodata.get('longitude')
                    )
                    if success:
                        successful_count += 1
                        print(f"Chunk {chunk_id}: ✓ Saved location for contributor {contributor_id}: {country}, {city}")
                except Exception as e:
                    print(f"Chunk {chunk_id}: ✗ Error saving location for contributor {contributor_id}: {e}")

            print(f"Chunk {chunk_id} completed: {successful_count}/{len(contributor_chunk)} locations determined")
            return successful_count

        except Exception as e:
            print(f"Chunk {chunk_id}: Error processing chunk: {e}")
            return 0
        finally:
            uow.dispose()

    def run_location_analysis(self) -> Dict[str, Any]:
        print("Starting contributor location analysis...")

        try:
            location_repo = LocationRepository(self.database_url)
            df = location_repo.load_contributor_location_data()

            if df.empty:
                return {"error": "No contributor data available"}

            print(f"Loaded {len(df)} contributors")

            chunk_size = max(1, len(df) // self.n_workers)
            contributor_chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            print(f"Split into {len(contributor_chunks)} chunks")

            total_successful = 0

            with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                future_to_chunk = {
                    executor.submit(
                        self._process_contributor_chunk,
                        chunk, self.database_url, chunk_id
                    ): chunk_id for chunk_id, chunk in enumerate(contributor_chunks)
                }

                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_id = future_to_chunk[future]
                    try:
                        result = future.result()
                        total_successful += result
                        print(f"Chunk {chunk_id} finished: {result} successful locations")
                    except Exception as e:
                        print(f"Chunk {chunk_id}: Error in future: {e}")

            print(f"Completed: {total_successful}/{len(df)} locations determined")

            return {
                "successful_locations": total_successful,
                "total_contributors": len(df)
            }

        except Exception as e:
            print(f"Error during location analysis: {e}")
            return {"error": str(e)}


class LocationAnalyzer:
    def __init__(self, database_url: str, n_workers: int = None):
        UnitOfWork(database_url).create_location_tables()
        location_repo = LocationRepository(database_url)

        self.analyzer = ContributorLocationClustering(database_url, location_repo, n_workers)

    def analyze(self) -> Dict:
        return self.analyzer.run_location_analysis()
