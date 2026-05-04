"""
Downloads NYC Yellow Taxi trip data (parquet) + taxi zone lookup CSV.

The NYC TLC publishes monthly parquet files. We grab several months to get
close to the ~2 GB target from the assignment.

Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
import os
import sys
from pathlib import Path
from urllib.request import urlretrieve

TRIPS_DIR = Path("./data/trips")
ZONES_DIR = Path("./data/zones")

# Each file is roughly 45-55 MB; 12 months ~ 600 MB. Double up on 2023 to get ~1.2GB+.
# Extend the list if you want to push past 2 GB.
MONTHS = [
    ("2023", f"{m:02d}") for m in range(1, 13)
] + [
    ("2022", f"{m:02d}") for m in range(1, 13)
]

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download(url: str, dest: Path):
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[skip] {dest.name} already exists ({dest.stat().st_size / 1e6:.1f} MB)")
        return
    print(f"[get]  {url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        urlretrieve(url, tmp)
        tmp.rename(dest)
        print(f"       -> {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    except Exception as e:
        if tmp.exists():
            tmp.unlink()
        print(f"[fail] {url}: {e}", file=sys.stderr)


def main():
    TRIPS_DIR.mkdir(parents=True, exist_ok=True)
    ZONES_DIR.mkdir(parents=True, exist_ok=True)

    for year, month in MONTHS:
        url = TRIP_URL.format(year=year, month=month)
        dest = TRIPS_DIR / f"yellow_tripdata_{year}-{month}.parquet"
        download(url, dest)

    download(ZONE_URL, ZONES_DIR / "taxi_zone_lookup.csv")

    total = sum(p.stat().st_size for p in TRIPS_DIR.glob("*.parquet"))
    print(f"\nTotal trip data: {total / 1e9:.2f} GB across {len(list(TRIPS_DIR.glob('*.parquet')))} files")


if __name__ == "__main__":
    main()
