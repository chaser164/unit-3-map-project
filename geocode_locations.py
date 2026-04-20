#!/usr/bin/env python3
"""
Geocodes candlepin bowling locations from the research CSV.
Uses Nominatim (OpenStreetMap) — free, no API key required.
Outputs: locations_geocoded.csv with columns: name, address, lat, lon
"""

import csv
import time
import sys
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

INPUT_FILE = "Candlepin Research - Sheet1.csv"
OUTPUT_FILE = "locations_geocoded.csv"

# Nominatim requires a unique user_agent string
geolocator = Nominatim(user_agent="candlepin_research_geocoder_v1")


def geocode_address(address: str, retries: int = 3) -> tuple[float, float] | None:
    for attempt in range(retries):
        try:
            location = geolocator.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude
            # Try with just city/state if full address fails
            parts = address.split(",")
            if len(parts) >= 3:
                fallback = ",".join(parts[1:]).strip()
                location = geolocator.geocode(fallback, timeout=10)
                if location:
                    print(f"  (used fallback: {fallback})")
                    return location.latitude, location.longitude
            return None
        except GeocoderTimedOut:
            if attempt < retries - 1:
                time.sleep(2)
        except GeocoderServiceError as e:
            print(f"  Service error: {e}")
            return None
    return None


def main():
    results = []
    failed = []

    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header row

        rows = [(row[0].strip(), row[1].strip()) for row in reader
                if len(row) >= 2 and row[0].strip() and row[1].strip()]

    print(f"Found {len(rows)} locations to geocode.\n")

    for i, (name, address) in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] {name}")
        print(f"  Address: {address}")

        coords = geocode_address(address)

        if coords:
            lat, lon = coords
            print(f"  -> {lat:.6f}, {lon:.6f}")
            results.append({"name": name, "address": address, "lat": lat, "lon": lon})
        else:
            print(f"  -> FAILED to geocode")
            failed.append({"name": name, "address": address})

        # Nominatim rate limit: max 1 request/second
        time.sleep(1.1)

    # Write output
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "address", "lat", "lon"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone! Geocoded {len(results)}/{len(rows)} locations.")
    print(f"Output written to: {OUTPUT_FILE}")

    if failed:
        print(f"\nFailed ({len(failed)}):")
        for loc in failed:
            print(f"  - {loc['name']}: {loc['address']}")
        print("\nFor failed locations, try manually looking up coordinates on")
        print("https://www.latlong.net or https://www.google.com/maps")


if __name__ == "__main__":
    main()
