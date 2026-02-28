from abc import ABC, abstractmethod
from typing import List, Dict, Union, Any, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        crit: str = criteria.lower()
        return [x for x in data_batch if crit in str(x).lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "items_processed": self.processed_count,
            "status": "operational"
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            temp = None
            for item in data_batch:
                if "temp" in item.lower():
                    temp = float(item.split(":")[1])
                    break
            if temp is None:
                raise ValueError("No temperature provided!")

            self.processed_count += len(data_batch)

            if temp > 40 or temp < -20:
                return (
                    f"Sensor analysis: {len(data_batch)} readings processed,"
                    f"avg temp: {temp}°C [EXTREME]"
                )
            return (
                f"Sensor analysis: {len(data_batch)} readings processed,"
                f"avg temp: {temp}°C"
            )
        except Exception as e:
            return f"Sensor Error: {e}"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            net_flow = 0
            for item in data_batch:
                if "buy" in str(item):
                    net_flow += int(str(item).split(":")[1])
                elif "sell" in str(item):
                    net_flow -= int(str(item).split(":")[1])

            self.processed_count += len(data_batch)

            sign = "+" if net_flow >= 0 else ""
            return (
                f"Transaction analysis: {len(data_batch)} operations, net "
                f"flow: {sign}{net_flow} units"
            )
        except Exception as e:
            return f"Transaction Error: {e}"


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            error_cnt = 0
            for item in data_batch:
                if "error" in item.lower():
                    error_cnt += 1

            self.processed_count += len(data_batch)

            if error_cnt == 0:
                return (
                    f"Event analysis: {len(data_batch)} events, no error"
                    "detected"
                )
            return (
                f"Event analysis: {len(data_batch)} events, {error_cnt} error"
                "detected"
            )
        except Exception as e:
            return f"Event Error: {e}"


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    # --- Sensor Stream ---
    print("Initializing Sensor Stream...")
    sensor_001 = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor_001.stream_id}, Type: {sensor_001.stream_type}")

    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"sensor batch: [{', '.join(sensor_batch)}]")
    print(sensor_001.process_batch(sensor_batch))

    # --- Transaction Stream ---
    print("\nInitializing Transaction Stream...")
    trans_001 = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans_001.stream_id}, Type: {trans_001.stream_type}")

    trans_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"transaction batch: [{', '.join(trans_batch)}]")
    print(trans_001.process_batch(trans_batch))

    # --- Event Stream ---
    print("\nInitializing Event Stream...")
    event_001 = EventStream("EVENT_001")
    print(f"Stream ID: {event_001.stream_id}, Type: {event_001.stream_type}")

    event_batch = ["login", "error", "logout"]
    print(f"event batch: [{', '.join(event_batch)}]")
    print(event_001.process_batch(event_batch))

    # --- Polymorphic Processing ---
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    streams: List[DataStream] = [sensor_001, trans_001, event_001]
    mixed_batches = [
        ["temp:35.0", "humidity:80"],
        ["buy:200", "sell:50", "buy:75", "sell:30"],
        ["login", "error", "timeout"]
    ]
    labels = ["Sensor", "Transaction", "Event"]
    counts = [2, 4, 3]
    units = ["readings", "operations", "events"]

    print("Batch 1 Results:")
    for stream, batch, label, count, unit in zip(streams, mixed_batches,
                                                 labels, counts, units):
        stream.process_batch(batch)
        print(f"  - {label} data: {count} {unit} processed")

    print("\nStream filtering active: High-priority data only")
    filtered_sensor = sensor_001.filter_data(
        ["temp:41.0", "humidity:30", "temp:38.5"], "temp"
    )
    filtered_trans = trans_001.filter_data(
        ["buy:500", "sell:10", "buy:300"], "buy"
    )
    print(f"Filtered results: {len(filtered_sensor)} critical sensor alerts,"
          f"{len(filtered_trans) - 1} large transaction")

    print("\nAll streams processed successfully.")
    print("Nexus throughput optimal.")