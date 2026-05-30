from dataclasses import dataclass, field


@dataclass(slots=True)
class EventCountSummaryReporter:
    interval_events: int
    _last_reported_count: int = field(default=0, init=False)

    def should_report(self, current_count: int) -> bool:
        if (
            self.interval_events <= 0
            or current_count - self._last_reported_count < self.interval_events
        ):
            return False

        self._last_reported_count = current_count
        return True
