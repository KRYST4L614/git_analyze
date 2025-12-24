from typing import Dict, Any
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import cachetools

_geo_cache = cachetools.LRUCache(maxsize=1000)
_geolocator = Nominatim(user_agent="spark_contributor_analysis")


def geocode_location_udf(location_str: str) -> Dict[str, Any]:
    if not location_str or location_str.lower() in ['', 'unknown', 'none', 'null']:
        return {"latitude": None, "longitude": None, "country": None, "city": None}

    cache_key = location_str.lower().strip()
    if cache_key in _geo_cache:
        return _geo_cache[cache_key]

    try:
        location = _geolocator.geocode(
            location_str,
            addressdetails=True,
            language='en',
            timeout=30,
        )

        if location:
            address = location.raw.get('address', {})
            country = address.get('country', '')

            city_fields = ['city', 'town', 'village', 'municipality']
            city = None
            for field in city_fields:
                if field in address and address[field]:
                    city = address[field]
                    break

            result = {
                "latitude": float(location.latitude),
                "longitude": float(location.longitude),
                "country": country.lower() if country else None,
                "city": city.lower() if city else None
            }
            _geo_cache[cache_key] = result
            return result

    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Geocoding error for '{location_str}': {e}")
    except Exception as e:
        print(f"Unexpected error for '{location_str}': {e}")

    return {"latitude": None, "longitude": None, "country": None, "city": None}


def extract_country_udf(geodata: Dict[str, Any]) -> str:
    if not geodata:
        return None
    return geodata.get("country")


def extract_city_udf(geodata: Dict[str, Any]) -> str:
    if not geodata:
        return None
    return geodata.get("city")