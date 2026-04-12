import requests
import json
import pandas as pd
import zipfile
import io
import os
import time

CENSUS_KEY = "517c96c37c7f07b5ed2f19ea4c2cb0e0214bcb36"
YEARS      = [2020, 2021, 2022, 2023, 2024]

os.makedirs("data/raw", exist_ok=True)

# format: (place FIPS, state FIPS, city name, state abbreviation)
CITIES = [
    ("07000", "01", "Birmingham",   "AL"),
    ("55000", "04", "Phoenix",      "AZ"),
    ("44000", "06", "Los Angeles",  "CA"),
    ("20000", "08", "Denver",       "CO"),
    ("35000", "12", "Jacksonville", "FL"),
    ("04000", "13", "Atlanta",      "GA"),
    ("14000", "17", "Chicago",      "IL"),
    ("55000", "22", "New Orleans",  "LA"),
    ("04000", "24", "Baltimore",    "MD"),
    ("22000", "26", "Detroit",      "MI"),
    ("63000", "36", "New York City","NY"),
    ("60000", "42", "Philadelphia", "PA"),
    ("52006", "47", "Nashville",    "TN"),
    ("45000", "48", "Houston",      "TX"),
    ("65000", "48", "San Antonio",  "TX"),
    ("19000", "48", "Dallas",       "TX"),
    ("63000", "53", "Seattle",      "WA"),
    ("38000", "29", "Kansas City",  "MO"),
    ("55000", "40", "Oklahoma City","OK"),
    ("40000", "32", "Las Vegas",    "NV"),
    ("50000", "11", "Washington",   "DC"),
]

CITY_COORDS = {
    "Birmingham_AL":   (33.5186,  -86.8104),
    "Phoenix_AZ":      (33.4484, -112.0740),
    "Los Angeles_CA":  (34.0522, -118.2437),
    "Denver_CO":       (39.7392, -104.9903),
    "Jacksonville_FL": (30.3322,  -81.6557),
    "Atlanta_GA":      (33.7490,  -84.3880),
    "Chicago_IL":      (41.8781,  -87.6298),
    "New Orleans_LA":  (29.9511,  -90.0715),
    "Baltimore_MD":    (39.2904,  -76.6122),
    "Detroit_MI":      (42.3314,  -83.0458),
    "New York City_NY":(40.7128,  -74.0060),
    "Philadelphia_PA": (39.9526,  -75.1652),
    "Nashville_TN":    (36.1627,  -86.7816),
    "Houston_TX":      (29.7604,  -95.3698),
    "San Antonio_TX":  (29.4241,  -98.4936),
    "Dallas_TX":       (32.7767,  -96.7970),
    "Seattle_WA":      (47.6062, -122.3321),
    "Kansas City_MO":  (39.0997,  -94.5786),
    "Oklahoma City_OK":(35.4676,  -97.5164),
    "Las Vegas_NV":    (36.1699, -115.1398),
    "Washington_DC":   (38.9072,  -77.0369),
}

# EPA CBSA names for our 20 cities
CITY_MAP = {
    "Birmingham, AL":   "Birmingham-Hoover, AL",
    "Phoenix, AZ":      "Phoenix-Mesa-Chandler, AZ",
    "Los Angeles, CA":  "Los Angeles-Long Beach-Anaheim, CA",
    "Denver, CO":       "Denver-Aurora-Lakewood, CO",
    "Jacksonville, FL": "Jacksonville, FL",
    "Atlanta, GA":      "Atlanta-Sandy Springs-Alpharetta, GA",
    "Chicago, IL":      "Chicago-Naperville-Elgin, IL-IN-WI",
    "New Orleans, LA":  "New Orleans-Metairie, LA",
    "Baltimore, MD":    "Baltimore-Columbia-Towson, MD",
    "Detroit, MI":      "Detroit-Warren-Dearborn, MI",
    "New York City, NY":"New York-Newark-Jersey City, NY-NJ-PA",
    "Philadelphia, PA": "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
    "Nashville, TN":    "Nashville-Davidson--Murfreesboro--Franklin, TN",
    "Houston, TX":      "Houston-The Woodlands-Sugar Land, TX",
    "San Antonio, TX":  "San Antonio-New Braunfels, TX",
    "Dallas, TX":       "Dallas-Fort Worth-Arlington, TX",
    "Seattle, WA":      "Seattle-Tacoma-Bellevue, WA",
    "Kansas City, MO":  "Kansas City, MO-KS",
    "Oklahoma City, OK":"Oklahoma City, OK",
    "Las Vegas, NV":    "Las Vegas-Henderson-Paradise, NV",
    "Washington, DC":   "Washington-Arlington-Alexandria, DC-VA-MD-WV",
}

CENSUS_VARS = ",".join([
    "B19013_001E",  # median household income
    "B25064_001E",  # median gross rent
    "B25077_001E",  # median home value
    "B23025_005E",  # unemployed population
    "B01003_001E",  # total population
    "B08301_001E",  # total commuters
    "B08301_010E",  # public transit commuters
    "NAME",
])



# CENSUS
def fetch_census_city(place_fips, state_fips, city_name, state_abbr, year):
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        "get": CENSUS_VARS,
        "for": f"place:{place_fips}",
        "in":  f"state:{state_fips}",
        "key": CENSUS_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code == 204: return None
    if resp.status_code == 400:
        print(f"  Bad request for {city_name} ({year})")
        return None
    if resp.status_code == 429:
        print("  Rate limited, waiting 30s...")
        time.sleep(30)
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} for {city_name} ({year})")
        return None
    if not resp.text.strip(): return None

    data = resp.json()
    if len(data) < 2: return None

    row = dict(zip(data[0], data[1]))
    return {
        "city_key":               f"{city_name.lower().replace(' ', '_')}_{state_abbr.lower()}",
        "city_state":             f"{city_name}, {state_abbr}",
        "city_name":              city_name,
        "state_abbr":             state_abbr,
        "year":                   year,
        "median_household_income":row.get("B19013_001E"),
        "median_gross_rent":      row.get("B25064_001E"),
        "median_home_value":      row.get("B25077_001E"),
        "unemployed_population":  row.get("B23025_005E"),
        "total_population":       row.get("B01003_001E"),
        "commute_total":          row.get("B08301_001E"),
        "commute_public_transit": row.get("B08301_010E"),
    }


def fetch_all_census():
    rows = []
    for year in YEARS:
        for place_fips, state_fips, city_name, state_abbr in CITIES:
            print(f"  {city_name}, {state_abbr} ({year})...")
            row = fetch_census_city(place_fips, state_fips, city_name, state_abbr, year)
            if row: rows.append(row)
            time.sleep(0.2)
    df = pd.DataFrame(rows)
    df.to_csv("data/raw/census_sample.csv", index=False)
    print(f"Saved {len(df)} rows → data/raw/census_sample.csv")
    return df



# WEATHER 

def fetch_weather_city(city_name, state_abbr, lat, lon, year):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":  lat,
        "longitude": lon,
        "start_date":f"{year}-01-01",
        "end_date":  f"{year}-12-31",
        "daily":     "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,sunshine_duration",
        "timezone":  "America/New_York",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.exceptions.Timeout:
            print(f"  Timeout (attempt {attempt+1}/3) for {city_name} {year}, retrying...")
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"  Error for {city_name} {year}: {e}")
            return None
    else:
        print(f"  Failed after 3 attempts for {city_name} {year}, skipping...")
        return None

    daily = resp.json().get("daily", {})
    def avg(lst): return round(sum(lst) / len(lst), 1) if lst else None
    def clean(lst): return [x for x in lst if x is not None]

    temps_max = clean(daily.get("temperature_2m_max", []))
    temps_min = clean(daily.get("temperature_2m_min", []))
    precip    = clean(daily.get("precipitation_sum",  []))
    snow      = clean(daily.get("snowfall_sum",       []))
    sunshine  = clean(daily.get("sunshine_duration",  []))

    return {
        "city_key":                f"{city_name.lower().replace(' ', '_')}_{state_abbr.lower()}",
        "year":                    year,
        "avg_high_temp_c":         avg(temps_max),
        "avg_low_temp_c":          avg(temps_min),
        "annual_precip_mm":        round(sum(precip), 1) if precip else None,
        "snow_days":               sum(1 for s in snow if s > 0),
        "avg_sunshine_hrs_per_day":round((sum(sunshine) / len(sunshine)) / 3600, 1) if sunshine else None,
    }


def fetch_all_weather():
    rows = []
    for year in YEARS:
        for city_state, (lat, lon) in CITY_COORDS.items():
            city_name, state_abbr = city_state.split("_", 1)
            print(f"  {city_name}, {state_abbr} ({year})...")
            row = fetch_weather_city(city_name, state_abbr, lat, lon, year)
            if row: rows.append(row)
            time.sleep(1)   # increased to avoid 429s
    df = pd.DataFrame(rows)
    df.to_csv("data/raw/weather_sample.csv", index=False)
    print(f"Saved {len(df)} rows → data/raw/weather_sample.csv")
    return df


# AIR QUALITY 

def explore_json_structure(json_data):
    """Review JSON structure and navigation — same as Rush's original."""
    data = json_data
    print("Data type:", type(data))
    print("Top-level keys:", list(data.keys()))
    print("\nFirst result:")
    print(json.dumps(data['results'][0], indent=2))
    name_key = 'name' if 'name' in data['results'][0] else 'CBSA'
    all_names = [result[name_key] for result in data['results']]
    print(f"Found {len(all_names)} locations.")
    print(all_names[:5], "...")


def simple_api_request(year):
    """Download EPA's annual AQI by CBSA zip for a given year."""
    url = f"https://aqs.epa.gov/aqsweb/airdata/annual_aqi_by_cbsa_{year}.zip"
    print(f"Fetching: {url}")
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        print(f"  Could not fetch {year} (HTTP {response.status_code})")
        return None
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f)
    return {"results": df.to_dict(orient="records")}


def convert_to_dataframe(data, year):
    """Filter EPA response to our cities and extract AQI fields."""
    records = []
    for result in data['results']:
        cbsa = str(result.get('CBSA', '')).strip()
        matched_city = None
        for city_label, cbsa_name in CITY_MAP.items():
            if cbsa == cbsa_name:
                matched_city = city_label
                break
        if matched_city is None:
            continue
        records.append({
            "city_state":          matched_city,
            "city_key":            matched_city.split(", ")[0].lower().replace(" ", "_") + "_" + matched_city.split(", ")[1].lower(),
            "year":                year,
            "days_with_aqi":       result.get("Days with AQI"),
            "good_days":           result.get("Good Days"),
            "moderate_days":       result.get("Moderate Days"),
            "unhealthy_sens_days": result.get("Unhealthy for Sensitive Groups Days"),
            "unhealthy_days":      result.get("Unhealthy Days"),
            "very_unhealthy_days": result.get("Very Unhealthy Days"),
            "hazardous_days":      result.get("Hazardous Days"),
            "max_aqi":             result.get("Max AQI"),
            "median_aqi":          result.get("Median AQI"),
            "days_co":             result.get("Days CO"),
            "days_no2":            result.get("Days NO2"),
            "days_ozone":          result.get("Days Ozone"),
            "days_pm25":           result.get("Days PM2.5"),
            "days_pm10":           result.get("Days PM10"),
        })
    return pd.DataFrame(records)


def fetch_all_airquality():
    all_dfs = []
    for year in YEARS:
        data = simple_api_request(year)
        if data is None: continue
        explore_json_structure(data)
        df = convert_to_dataframe(data, year)
        print(f"  Extracted {len(df)} cities for {year}")
        all_dfs.append(df)
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.to_csv("data/raw/airquality_sample.csv", index=False)
    print(f"Saved {len(final_df)} rows → data/raw/airquality_sample.csv")
    return final_df


def merge_csvs():
    """
    Load the three saved CSVs and merge them on city_key + year.
    Runs independently — no need to re-fetch data.
    """
    census_df  = pd.read_csv("data/raw/census_sample.csv")
    weather_df = pd.read_csv("data/raw/weather_sample.csv")
    aq_df      = pd.read_csv("data/raw/airquality_sample.csv")

    merged = census_df \
        .merge(weather_df, on=["city_key", "year"], how="left") \
        .merge(aq_df,      on=["city_key", "year"], how="left")

    # drop duplicate city_state col if both exist after merge
    if "city_state_x" in merged.columns:
        merged = merged.rename(columns={"city_state_x": "city_state"}).drop(columns=["city_state_y"], errors="ignore")

    merged.to_csv("data/raw/merged_dataset.csv", index=False)
    print(f"\n✓ Merged dataset: {len(merged)} rows × {len(merged.columns)} columns")
    print(f"  Saved → data/raw/merged_dataset.csv")
    print(merged[["city_key", "year", "total_population", "median_aqi", "avg_high_temp_c"]].to_string(index=False))
    return merged


# test exercises 

def exercise_1(df):
    """What cities had the worst air quality each year?"""
    worst = df.groupby('year')['median_aqi'].idxmax()
    print("\nWorst AQI city per year:")
    print(df.loc[worst, ['year', 'city_state', 'median_aqi']].to_string(index=False))

def exercise_2(df):
    """What's the average number of good days per city?"""
    avg_good = df.groupby('city_state')['good_days'].mean().sort_values(ascending=False)
    print("\nAvg good air quality days per city:")
    print(avg_good.to_string())

def exercise_3(df):
    """Extract a list of all city names — list comprehension!"""
    city_names = [city for city in df['city_state'].unique()]
    print(f"\nFound {len(city_names)} cities: {city_names}")
    return city_names


# MAIN
def main():
    # --- Step 1: Fetch and save each dataset individually ---
    print("\n--- Fetching Census ACS Data ---")
    # fetch_all_census()

    print("\n--- Fetching Weather Data ---")
    # fetch_all_weather()

    print("\n--- Fetching Air Quality Data (EPA) ---")
    # aq_df = fetch_all_airquality()

    # --- Step 2: Merge the saved CSVs ---
    print("\n--- Merging Saved CSVs ---")
    merged_df = merge_csvs()

    # --- Exercises on air quality data ---
    # exercise_1(aq_df)
    # exercise_2(aq_df)
    # exercise_3(aq_df)

    print("\nDone! Check data/raw/ for all output files.")

    # load the two datasets
    merged_df = pd.read_csv("data/raw/merged_dataset.csv")
    crime_df = pd.read_csv("data/raw/crime_csv.txt")

    # drop DC and FL rows from crime data since they aren't in our city dataset
    crime_df = crime_df[~crime_df["state_abbr"].isin(["DC", "FL"])]

    # drop the blank rows (SD 2021 FL etc)
    crime_df = crime_df.dropna(subset=["arrests_per_100k"])


    final_df = merged_df.merge(
        crime_df[["state_abbr", "year", "arrests_per_person", "arrests_per_100k"]],
        on=["state_abbr", "year"],
        how="left"
    )

    # save
    final_df.to_csv("data/raw/merged_dataset.csv", index=False)
    print(f"✓ Done — {len(final_df)} rows, {len(final_df.columns)} columns")
    print(final_df[["city_key", "year", "state_abbr", "arrests_per_100k"]].to_string(index=False))

if __name__ == "__main__":
    main()
