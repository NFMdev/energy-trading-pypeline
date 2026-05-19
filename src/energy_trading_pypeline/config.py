from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

AppEnv = Literal["local", "test", "dev", "prod"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: AppEnv = "local"

    database_host: str = "localhost"
    database_port: int = 5432
    database_name: str = "energy_pipeline"
    database_user: str = "energy_user"
    database_password: str = "energy_password"
    database_url: str = (
        "postgresql+psycopg://energy_user:energy_password@localhost:5432/energy_pipeline"
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_raw_topic: str = "energy.market.raw.v1"
    kafka_consumer_group: str = "energy-market-ingestion-v1"
    generator_market_areas: str = "DK1,DK2,DE,SE3,NO2"
    generator_source: str = "energy-generator"

    @property
    def market_areas(self) -> list[str]:
        return [
            market_area.strip().upper()
            for market_area in self.generator_market_areas.split(",")
            if market_area.strip()
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
