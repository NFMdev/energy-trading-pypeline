from dataclasses import dataclass


@dataclass(slots=True)
class ProducerRuntimeStats:
    produced_events: int = 0
    publish_failures: int = 0

    def record_produced_event(self) -> None:
        self.produced_events += 1

    def record_publish_failure(self) -> None:
        self.publish_failures += 1


@dataclass(slots=True)
class ConsumerRuntimeStats:
    valid_events_processed: int = 0
    duplicate_events: int = 0
    stale_events: int = 0
    invalid_events_persisted: int = 0
    generated_alerts: int = 0
    processing_failures: int = 0
    committed_messages: int = 0

    def record_valid_event_processed(self, inserted_alerts: int) -> None:
        self.valid_events_processed += 1
        self.generated_alerts += inserted_alerts

    def record_duplicate_event(self) -> None:
        self.duplicate_events += 1

    def record_stale_event(self) -> None:
        self.stale_events += 1

    def record_invalid_event_persisted(self) -> None:
        self.invalid_events_persisted += 1

    def record_processing_failure(self) -> None:
        self.processing_failures += 1

    def record_committed_messages(self) -> None:
        self.committed_messages += 1
