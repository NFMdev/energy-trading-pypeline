from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent


def serialize_energy_market_event(event: EnergyMarketEvent) -> bytes:
    return event.model_dump_json().encode("utf-8")


def deserialize_energy_market_event(payload: bytes) -> EnergyMarketEvent:
    return EnergyMarketEvent.model_validate_json(payload.decode("utf-8"))
