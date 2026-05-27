from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from energy_trading_pypeline.domain.invalid_energy_market_event import InvalidEnergyMarketEvent
from energy_trading_pypeline.persistence.repository.invalid_energy_market_event_repository import (
    InvalidEnergyMarketEventRepository,
)


class KafkaMessage(Protocol):
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...


class InvalidEnergyMarketEventProcessor:
    def __init__(self, session_factory: sessionmaker[Session], consumer_group: str) -> None:
        self._session_factory = session_factory
        self._consumer_group = consumer_group

    def process(self, message: KafkaMessage, error: Exception) -> bool:
        with self._session_factory.begin() as session:
            repository = InvalidEnergyMarketEventRepository(session)
            invalid_event = InvalidEnergyMarketEvent(
                topic=message.topic(),
                kafka_partition=message.partition(),
                kafka_offset=message.offset(),
                kafka_key=self._decode_optional_bytes(message.key()),
                payload=self._payload_bytes(message.value()),
                payload_text=self._decode_optional_bytes(message.value()),
                error_type=type(error).__name__,
                error_message=str(error),
                consumer_group=self._consumer_group,
            )

            return repository.save(invalid_event)

    @staticmethod
    def _payload_bytes(value: bytes | None) -> bytes:
        if value is None:
            return b""

        return value

    @staticmethod
    def _decode_optional_bytes(value: bytes | None) -> str | None:
        if value is None:
            return None

        return value.decode("utf-8", errors="replace")
