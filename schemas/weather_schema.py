from pydantic import BaseModel
from datetime import datetime


class WeatherRaw(BaseModel):
    city: str
    latitude: float
    longitude: float
    temperature: float
    windspeed: float
    winddirection: float
    weathercode: int
    timestamp: datetime