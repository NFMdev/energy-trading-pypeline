from dataclasses import dataclass


@dataclass(frozen=True)
class InvalidEnergyMarketEvent:
    topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_key: str | None
    payload: bytes
    payload_text: str | None
    error_type: str
    error_message: str
    consumer_group: str
