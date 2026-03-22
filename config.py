from dataclasses import dataclass

# Use a real email here. NWS wants a meaningful User-Agent.
USER_AGENT = "JamesWeatherResearch/1.0 (your_email@example.com)"


@dataclass
class CityConfig:
    name: str
    series_ticker: str
    lat: float
    lon: float
    timezone: str
    sigma_f: float = 2.5
    low_series_ticker: str = ""
    rain_series_ticker: str = ""
    snow_series_ticker: str = ""
    wind_series_ticker: str = ""
    rain_sigma: float = 0.2
    snow_sigma: float = 0.4
    wind_sigma: float = 4.0


CITIES = [
    CityConfig(
        name="NYC",
        series_ticker="KXHIGHNY",
        low_series_ticker="KXLOWNY",
        rain_series_ticker="KXRAINNYC",
        snow_series_ticker="KXNYCSNOWM",
        lat=40.7829,   # Central Park area
        lon=-73.9654,
        timezone="America/New_York",
        sigma_f=2.2,
    ),
    CityConfig(
        name="Los Angeles",
        series_ticker="KXHIGHLAX",
        low_series_ticker="KXLOWLAX",
        rain_series_ticker="KXRAINLAXM",
        snow_series_ticker="KXLAXSNOWM",
        lat=33.93806,   # KLAX / Los Angeles International Airport
        lon=-118.38889,
        timezone="America/Los_Angeles",
        sigma_f=2.8,
    ),
    CityConfig(
        name="Chicago",
        series_ticker="KXHIGHCHI",
        low_series_ticker="KXLOWCHI",
        rain_series_ticker="KXRAINCHIM",
        snow_series_ticker="KXCHISNOWM",
        lat=41.9786,   # O'Hare International Airport (close to Chicago area weather series)
        lon=-87.9048,
        timezone="America/Chicago",
        sigma_f=2.5,
    ),
    CityConfig(
        name="Miami",
        series_ticker="KXHIGHMIA",
        low_series_ticker="KXLOWMIA",
        rain_series_ticker="KXRAINMIAM",
        snow_series_ticker="KXMIASNOWM",
        lat=25.7959,   # Miami International Airport / central Miami area
        lon=-80.2870,
        timezone="America/New_York",
        sigma_f=2.5,
    ),
    CityConfig(
        name="Austin",
        series_ticker="KXHIGHAUS",
        low_series_ticker="KXLOWAUS",
        rain_series_ticker="KXRAINAUSM",
        snow_series_ticker="KXAUSSNOWM",
        lat=30.1975,   # Austin-Bergstrom International Airport (Austin area heat markets)
        lon=-97.6664,
        timezone="America/Chicago",
        sigma_f=2.5,
    ),
]
