#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        ...

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        ...

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        ...


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.data = 0
        self.warn = []

    def process_batch(self, data_batch: List[Any]) -> str:
        self.count = len(data_batch)
        self.data = data_batch[0]
        result = ""
        param = ['temp:', 'humidity:', 'pressure:']
        if data_batch[0] > 50 or data_batch[0] < -10:
            self.warn.append(f"extreme temperature: {data_batch[0]}")
        if data_batch[1] < 20 or data_batch[1] > 80:
            self.warn.append(f"extreme humidity rate: {data_batch[1]}")
        for i, data in enumerate(data_batch):
            result += f"{param[i]}"
            result += f"{str(data)} "
            i += 1
        return result

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "hp":
            return self.warn
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'data': self.data,
                'warn': len(self.warn)
        }


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.data = 0
        self.warn = []

    def process_batch(self, data_batch: List[Any]) -> str:
        self.count = len(data_batch)
        self.data = sum(data_batch)

        result = ""
        for n in data_batch:
            if n > 10000 or n < -10000:
                self.warn.append("extreme transaction")
            if n > 0:
                result += 'buy:'
            elif n < 0:
                result += 'sell'
            else:
                self.warn.append("transaction can't be 0")
            result += f"{n}"
        return result

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "hp":
            return self.warn
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'data': self.data,
                'warn': len(self.warn)
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.data = 0
        self.warn = []

    def process_batch(self, data_batch: List[Any]) -> str:
        self.count = len(data_batch)

        result = ""
        for e in data_batch:
            result += f"{e},"
            if e != data_batch[-1]:
                result += ' '
            if e == 'error':
                self.warn.append("error detected")
            self.data = len(self.warn)
        return result

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
                'nb': self.count,
                'data': self.data,
                'warn': len(self.warn)
        }


class StreamProcessor:
    def __init__(self, name: str) -> None:
        self.name = name
        if "SENSOR" in name:
            self.type = SensorStream(name)
        elif "TRANS" in name:
            self.type = TransactionStream(name)
        elif "EVENT" in name:
            self.type = EventStream(name)
        else:
            raise TypeError("enter valid type stream_id")

    def process_batch(self, data_batch: List[Any]) -> str:
        return self.type.process_batch(data_batch)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == 'hp':
            data_batch = self.type.filter_data(data_batch, 'hp')
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return self.type.get_stats()


def ft_data_stream() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    s001 = "SENSOR_001"
    sstream = SensorStream(s001)
    s_data = [22.5, 65, 1013]
    print(f"Stream ID: {s001}, Type: Environmental Data")
    try:
        print("Processing sensor batch: "
              f"[temp:{s_data[0]}, "
              f"humidity:{s_data[1]}, "
              f"pressure:{s_data[2]}]")
        sstream.process_batch(s_data)
        s_stats = sstream.get_stats()
        print("Sensor analysis: "
              f"{s_stats['nb']} readings processed, "
              f"avg temp: {s_stats['data']}°C")
    except Exception as e:
        print(e)
    finally:
        print()

    print("Initializing Transaction Stream...")
    t001 = "TRANS_001"
    tstream = TransactionStream(t001)
    t_data = [100, -150, 75]
    print(f"Stream ID: {t001}, Type: Financial Data")
    try:
        print("Processing transaction batch: "
              f"[buy:{t_data[0]}, sell:{abs(t_data[1])}, buy:{t_data[2]}]")
        tstream.process_batch(t_data)
        t_stats = tstream.get_stats()
        s = '-'
        if isinstance(t_stats['data'], int) and t_stats['data'] > 0:
            s = '+'
        print("Sensor analysis: "
              f"{t_stats['nb']} operations, "
              f"net flow: {s}{t_stats['data']} units")
    except Exception as e:
        print(e)
    finally:
        print()

    print("Initializing Event Stream...")
    e001 = "EVENT_001"
    estream = EventStream(e001)
    e_data = ['login', 'error', 'logout']
    print(f"Stream ID: {e001}, Type: System Events")
    try:
        print(f"Processing event batch: {e_data}")
        estream.process_batch(e_data)
        e_stats = estream.get_stats()
        print("Event analysis: "
              f"{e_stats['nb']} events, "
              f"{e_stats['data']} error detected")
    except Exception as e:
        print(e)
    finally:
        print()

    print("=== Polymorphic Stream Processing ===")

    print("Processing mixed stream types through unified interface...\n")
    s2_data = [70, 10]
    t2_data = [60, 700, -870, 12569]
    e2_data = ['login', 'ssh', 'error']
    try:
        s002 = StreamProcessor("SENSOR_002")
        t002 = StreamProcessor("TRANS_002")
        e002 = StreamProcessor("EVENT_002")

        s002.process_batch(s2_data)
        s2_stats = s002.get_stats()
        t002.process_batch(t2_data)
        t2_stats = t002.get_stats()
        e002.process_batch(e2_data)
        e2_stats = e002.get_stats()

        print("Batch 1 Results:")
        print(f"- Sensor data: {s2_stats['nb']} readings processed")
        print(f"- Transaction data: {t2_stats['nb']} operations processed")
        print(f"- Event data: {e2_stats['nb']} events processed")
        print()

        print("Stream filtering active: High-priority data only")
        s2_warns = s002.filter_data(s2_data, 'hp')
        t2_warns = t002.filter_data(t2_data, 'hp')
        print("Filtered results: "
              f"{len(s2_warns)} critical sensor alerts, "
              f"{len(t2_warns)} large transaction")
        print()

        print("All streams processed successfully. Nexus throughput optimal.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    ft_data_stream()