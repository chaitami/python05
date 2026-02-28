from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Callable


Stats = Dict[str, Union[str, int, float]]


# =========================
# ABSTRACT BASE CLASS
# =========================
class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError

    # Default implementation (required)
    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        crit: str = criteria.lower()
        return [x for x in data_batch if crit in str(x).lower()]

    # Default implementation (required)
    def get_stats(self) -> Stats:
        return {
            "stream_id": self.stream_id,
            "type": self.stream_type,
            "items_processed": self.count,
        }


# =========================
# SENSOR STREAM
# =========================
class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self.temperature: float = 0.0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Sensor batch must be a list")

            if len(data_batch) < 2:
                raise ValueError("Sensor batch requires at least temp and humidity")

            self.warnings = []
            self.count = len(data_batch)

            temp: float = float(data_batch[0])
            humidity: float = float(data_batch[1])
            pressure: Optional[float] = (
                float(data_batch[2]) if len(data_batch) > 2 else None
            )

            self.temperature = temp

            if temp > 50 or temp < -10:
                self.warnings.append("extreme temperature")
            if humidity < 20 or humidity > 80:
                self.warnings.append("extreme humidity")
            if pressure is not None and (pressure < 950 or pressure > 1050):
                self.warnings.append("extreme pressure")

            parts: List[str] = [
                f"temp:{temp}",
                f"humidity:{humidity}",
            ]
            if pressure is not None:
                parts.append(f"pressure:{pressure}")

            return "[" + ", ".join(parts) + "]"

        except Exception as e:
            return f"FAILED: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Stats:
        base: Stats = super().get_stats()
        base.update({
            "avg_temp": self.temperature,
            "warnings": len(self.warnings)
        })
        return base


# =========================
# TRANSACTION STREAM
# =========================
class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.net_flow: int = 0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Transaction batch must be a list")

            self.warnings = []
            self.count = len(data_batch)

            values: List[int] = []

            for item in data_batch:
                if not isinstance(item, int):
                    raise TypeError("Transactions must be integers")
                values.append(item)

            self.net_flow = sum(values)

            parts: List[str] = []
            for n in values:
                if abs(n) > 10000:
                    self.warnings.append("extreme transaction")
                if n == 0:
                    self.warnings.append("invalid zero transaction")
                    continue
                parts.append(
                    ("buy:" if n > 0 else "sell:") + str(abs(n))
                )

            return "[" + ", ".join(parts) + "]"

        except Exception as e:
            return f"FAILED: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Stats:
        base: Stats = super().get_stats()
        base.update({
            "net_flow": self.net_flow,
            "warnings": len(self.warnings)
        })
        return base


# =========================
# EVENT STREAM
# =========================
class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.errors: int = 0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Event batch must be a list")

            self.warnings = []
            self.count = len(data_batch)

            events: List[str] = [str(e) for e in data_batch]

            self.errors = len(
                [e for e in events if e.lower() == "error"]
            )

            if self.errors > 0:
                self.warnings.append("error detected")

            return "[" + ", ".join(events) + "]"

        except Exception as e:
            return f"FAILED: {e}"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Stats:
        base: Stats = super().get_stats()
        base.update({
            "errors_detected": self.errors,
            "warnings": len(self.warnings)
        })
        return base


# =========================
# POLYMORPHIC PROCESSOR
# =========================
class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def register(self, stream: DataStream) -> None:
        if not isinstance(stream, DataStream):
            raise TypeError("Must register a DataStream subtype")
        self.streams.append(stream)

    def process_all(
        self,
        batches: Dict[str, List[Any]],
        criteria: Optional[str] = None,
        transform: Optional[Callable[[Any], Any]] = None,
    ) -> None:

        for stream in self.streams:
            batch: List[Any] = batches.get(stream.stream_id, [])

            filtered: List[Any] = stream.filter_data(batch, criteria)

            if transform is not None:
                filtered = [transform(x) for x in filtered]

            result: str = stream.process_batch(filtered)
            print(f"- {stream.stream_type}: {result}")


# =========================
# DEMO
# =========================
def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.register(sensor)
    processor.register(transaction)
    processor.register(event)

    batches = {
        "SENSOR_001": [22.5, 65, 1013],
        "TRANS_001": [100, -150, 75],
        "EVENT_001": ["login", "error", "logout"],
    }

    print("=== Polymorphic Stream Processing ===")
    processor.process_all(batches)

    print("\nStream filtering active: High-priority only")
    processor.process_all(batches, criteria="hp")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()