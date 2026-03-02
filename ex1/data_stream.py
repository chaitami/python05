
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        crit: str = criteria.lower()
        return [
            item for item in data_batch
            if crit in str(item).lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "count": self.count,
            "total": 10.5,
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self.count: int = 0
        self.temperature: float = 0.0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            temps: List[float] = [
                float(item.split(":")[1])
                for item in data_batch
                if isinstance(item, str) and "temp" in item.lower()
            ]
            humidities: List[int] = [
                int(item.split(":")[1])
                for item in data_batch
                if isinstance(item, str) and "humidity" in item.lower()
            ]

            pressures: List[int] = [
                int(item.split(":")[1])
                for item in data_batch
                if isinstance(item, str) and "pressure" in item.lower()
            ]

            if not temps:
                raise ValueError("No temperature provided!")

            self.temperature = sum(temps) / len(temps)

            self.count = len(data_batch)

            self.warnings = ["Error:" + str(item) for item in temps
                             if item > 50 or item < -10]
            self.warnings += ["Error:" + str(item) for item in humidities
                              if item > 60 or item < 30]

            parts: List[str] = ([f"temp:{item}" for item in temps
                                if item is not None] +
                                [f"humidity:{item}" for item in humidities
                                if item is not None] +
                                [f"pressure:{item}" for item in pressures
                                if item is not None])

            return "[" + ", ".join(parts) + "]"

        except Exception as e:
            return f"Sensor Error: {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'data': self.temperature,
                'nw': len(self.warnings),
                'status': "operational"
        }


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.data: int = 0
        self.count: int = 0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            allitems: List[int] = [
                int(str(item).split(":")[1]) if "buy" in str(item).lower()
                else -int(str(item).split(":")[1])
                for item in data_batch
                if "buy" in str(item).lower() or "sell" in str(item).lower()
            ]
            self.warnings = ["extreme transaction" for n in allitems
                             if abs(n) > 10000]
            self.count = len(data_batch)
            self.data = sum(allitems)
            parts: List[str] = [("buy:" if n > 0 else "sell:") + str(n)
                                for n in allitems]
            return "[" + ", ".join(parts) + "]"
        except Exception as e:
            return f"Transaction Error: {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'data': self.data,
                'nw': len(self.warnings),
                'total': 10.5,
                'status': "operational"
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.twarn: int = 0
        self.count: int = 0
        self.warnings: List[str] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        self.count = len(data_batch)

        result: List[str] = [str(item) for item in data_batch]
        self.warnings = ["error detected" for item in data_batch
                         if str(item).lower() == "error"]
        return "[" + ", ".join(result) + "]"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "hp":
            return self.warnings
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'twarn': self.twarn,
                'nw': len(self.warnings),
                'total': 10.5,
                'status': "operational"
        }


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def register(self, stream: DataStream) -> None:
        if not isinstance(stream, DataStream):
            raise TypeError("Only DataStream subtypes allowed")
        self.streams.append(stream)

    def safe_process(self, stream: DataStream, batch: List[Any]) -> str:
        try:
            return stream.process_batch(batch)
        except Exception as e:
            return f"FAILED: {e}"

    def filter_all(
        self,
        batches_by_id: Dict[str, List[Any]],
        criteria: Optional[str] = None,
    ) -> Dict[str, List[Any]]:

        return {
            stream.stream_id: stream.filter_data(
                batches_by_id.get(stream.stream_id, []),
                criteria
            )
            for stream in self.streams
        }

    def process_all(
        self,
        batches_by_id: Dict[str, List[Any]],
        criteria: Optional[str] = None,
        transform: Optional[Any] = None,
    ) -> List[str]:

        filtered_by_id: Dict[str, List[Any]] = self.filter_all(
            batches_by_id,
            criteria
        )

        return [
            self.safe_process(
                stream,
                [
                    transform(x) if isinstance(x, str) else x
                    for x in filtered_by_id.get(stream.stream_id, [])
                ] if transform is not None
                else filtered_by_id.get(stream.stream_id, [])
            )
            for stream in self.streams
        ]


def ft_data_stream() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    SE01 = "SENSOR_001"
    SS = SensorStream(SE01)
    s_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Stream ID: {SE01}, Type: {SS.stream_type}")
    try:
        print("Processing sensor batch: "
              f"{SS.process_batch(s_data)}")

        s_stats = SS.get_stats()
        print("Sensor analysis: "
              f"{s_stats['nb']} readings processed, "
              f"avg temp: {s_stats['data']}°C")
    except Exception as e:
        print(e)

    print("\nInitializing Transaction Stream...")
    TR01 = "TRANS_001"
    TS = TransactionStream(TR01)
    t_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {TR01}, Type: {TS.stream_type}")
    try:
        print("Processing transaction batch: " + TS.process_batch(t_data))
        t_stats = TS.get_stats()
        s = '-'
        if isinstance(t_stats['data'], int) and t_stats['data'] > 0:
            s = '+'
        print("Transaction analysis: "
              f"{t_stats['nb']} operations, "
              f"net flow: {s}{t_stats['data']} units")
    except Exception as e:
        print(e)

    print("\nInitializing Event Stream...")
    EV01 = "EVENT_001"
    ES = EventStream(EV01)
    e_data = ['login', 'error', 'logout']
    print(f"Stream ID: {EV01}, Type: {ES.stream_type}")
    try:
        print(f"Processing event batch: [{', '.join(map(str, e_data))}]")
        ES.process_batch(e_data)
        e_stats = ES.get_stats()
        print("Event analysis: "
              f"{e_stats['nb']} events, "
              f"{e_stats['nw']} error detected")
    except Exception as e:
        print(e)

    print("\n=== Polymorphic Stream Processing ===")

    print("Processing mixed stream types through unified interface...\n")
    s2_data = ["temp:70", "humidity:70"]
    t2_data = ["buy:3", "sell:700", "buy:870", "sell:12569"]
    e2_data = ['login', 'error', 'logout']
    try:
        processor = StreamProcessor()
        s2 = SensorStream("SENSOR_002")
        t2 = TransactionStream("TRANS_002")
        e2 = EventStream("EVENT_002")

        processor.register(s2)
        processor.register(t2)
        processor.register(e2)

        batches = {
            "SENSOR_002": s2_data,
            "TRANS_002": t2_data,
            "EVENT_002": e2_data,
        }

        processor.process_all(batches)

        print("Batch 1 Results:")
        print(f"- Sensor data: {s2.count} readings processed")
        print(f"- Transaction data: {t2.count} operations processed")
        print(f"- Event data: {e2.count} events processed")

        print("\nStream filtering active: High-priority data only")

        sensor_hp = s2.filter_data(s2_data, "hp")
        transaction_hp = t2.filter_data(t2_data, "hp")

        print(
            f"Filtered results: "
            f"{len(sensor_hp)} critical sensor alerts, "
            f"{len(transaction_hp)} large transaction"
        )

        print("\nAll streams processed successfully."
              " Nexus throughout optimal.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    ft_data_stream()
