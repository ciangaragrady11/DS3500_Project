import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import importlib
import sys
import os

MODULE = "api"

sys.path.insert(0, os.path.dirname(__file__))

_mod                = importlib.import_module(MODULE)
fetch_census_city   = _mod.fetch_census_city
fetch_weather_city  = _mod.fetch_weather_city
convert_to_dataframe= _mod.convert_to_dataframe
exercise_1          = _mod.exercise_1
exercise_2          = _mod.exercise_2
exercise_3          = _mod.exercise_3
CITY_MAP            = _mod.CITY_MAP
PATCH               = f"{MODULE}.requests.get"

@pytest.fixture
def sample_aqi_df():
    return pd.DataFrame({
        "city_state": ["Chicago, IL", "Phoenix, AZ", "Chicago, IL", "Phoenix, AZ"],
        "year":       [2022, 2022, 2023, 2023],
        "median_aqi": [55, 80, 60, 75],
        "good_days":  [200, 150, 190, 160],
    })


@pytest.fixture
def sample_epa_data():
    return {"results": [
        {
            "CBSA": "Chicago-Naperville-Elgin, IL-IN-WI",
            "Days with AQI": 365, "Good Days": 200, "Moderate Days": 100,
            "Unhealthy for Sensitive Groups Days": 40, "Unhealthy Days": 20,
            "Very Unhealthy Days": 4, "Hazardous Days": 1,
            "Max AQI": 175, "Median AQI": 52,
            "Days CO": 0, "Days NO2": 5, "Days Ozone": 80,
            "Days PM2.5": 270, "Days PM10": 10,
        },
        {
            "CBSA": "Phoenix-Mesa-Chandler, AZ",
            "Days with AQI": 365, "Good Days": 140, "Moderate Days": 150,
            "Unhealthy for Sensitive Groups Days": 60, "Unhealthy Days": 10,
            "Very Unhealthy Days": 2, "Hazardous Days": 0,
            "Max AQI": 160, "Median AQI": 68,
            "Days CO": 0, "Days NO2": 3, "Days Ozone": 120,
            "Days PM2.5": 200, "Days PM10": 40,
        },
        {
            "CBSA": "Some Unknown City, XX",   # should be filtered out
            "Days with AQI": 365, "Good Days": 300, "Moderate Days": 50,
            "Unhealthy for Sensitive Groups Days": 10, "Unhealthy Days": 5,
            "Very Unhealthy Days": 0, "Hazardous Days": 0,
            "Max AQI": 90, "Median AQI": 30,
            "Days CO": 0, "Days NO2": 0, "Days Ozone": 50,
            "Days PM2.5": 50, "Days PM10": 5,
        },
    ]}


# test 1: census 
def test_fetch_census_city_success():
    fake_json = [
        ["B19013_001E","B25064_001E","B25077_001E","B23025_005E",
         "B01003_001E","B08301_001E","B08301_010E","NAME","state","place"],
        ["62000","1200","300000","45000",
         "2700000","1200000","400000","Chicago city, Illinois","17","14000"],
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "non-empty"
    mock_resp.json.return_value = fake_json

    with patch(PATCH, return_value=mock_resp):
        result = fetch_census_city("14000", "17", "Chicago", "IL", 2022)

    assert result is not None
    assert result["city_name"] == "Chicago"
    assert result["year"] == 2022
    assert result["median_household_income"] == "62000"
    assert result["total_population"] == "2700000"


# test 2: census and error code 

def test_fetch_census_city_bad_request():
    mock_resp = MagicMock()
    mock_resp.status_code = 400

    with patch(PATCH, return_value=mock_resp):
        result = fetch_census_city("99999", "99", "FakeCity", "XX", 2022)

    assert result is None


# test 3: weather — correct aggregation

def test_fetch_weather_city_aggregation():
    fake_daily = {
        "daily": {
            "temperature_2m_max": [30.0, 32.0, 28.0],
            "temperature_2m_min": [20.0, 22.0, 18.0],
            "precipitation_sum":  [5.0, 0.0, 10.0],
            "snowfall_sum":       [0.0, 2.0, 0.0],
            "sunshine_duration":  [36000.0, 18000.0, 27000.0],
        }
    }
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = fake_daily
    mock_resp.raise_for_status = MagicMock()

    with patch(PATCH, return_value=mock_resp):
        result = fetch_weather_city("Chicago", "IL", 41.8781, -87.6298, 2022)

    assert result is not None
    assert result["avg_high_temp_c"] == 30.0
    assert result["avg_low_temp_c"] == 20.0
    assert result["annual_precip_mm"] == 15.0
    assert result["snow_days"] == 1
    assert result["avg_sunshine_hrs_per_day"] == 7.5


# test 4: convert_to_dataframe — filters correctly 

def test_convert_to_dataframe_filters(sample_epa_data):
    df = convert_to_dataframe(sample_epa_data, 2022)
    assert len(df) == 2
    assert set(df["city_state"]) == {"Chicago, IL", "Phoenix, AZ"}
    assert (df["year"] == 2022).all()


# test 5: convert_to_dataframe — AQI fields correct

def test_convert_to_dataframe_aqi_fields(sample_epa_data):
    df = convert_to_dataframe(sample_epa_data, 2022)
    chicago = df[df["city_state"] == "Chicago, IL"].iloc[0]
    assert chicago["median_aqi"] == 52
    assert chicago["good_days"] == 200
    assert chicago["max_aqi"] == 175
    assert chicago["days_pm25"] == 270


# test 6: exercise_1 — worst AQI city per year 

def test_exercise_1_worst_city(sample_aqi_df, capsys):
    exercise_1(sample_aqi_df)
    assert "Phoenix, AZ" in capsys.readouterr().out


# test 7: exercise_2 — sorted by avg good days 

def test_exercise_2_avg_good_days(sample_aqi_df, capsys):
    exercise_2(sample_aqi_df)
    out = capsys.readouterr().out
    assert out.index("Chicago, IL") < out.index("Phoenix, AZ")


# test 8: exercise_3 — unique city list 

def test_exercise_3_unique_cities(sample_aqi_df):
    cities = exercise_3(sample_aqi_df)
    assert isinstance(cities, list)
    assert len(cities) == 2
    assert len(cities) == len(set(cities))
    assert "Chicago, IL" in cities
    assert "Phoenix, AZ" in cities
