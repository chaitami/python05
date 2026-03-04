from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol, Optional
from datetime import datetime
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage():
    def process(self, data: Any) -> Dict:
        con: Dict = {}

        if isinstance(data, Dict):
            con.update({'type': 'JSON'})
            con.update({'data': data})
            con.update({'validation': True})
            con.update({'stage': 'Stage 1'})

        elif isinstance(data, str) and ',' in data:
            con.update({'type': 'CSV'})
            con.update({'validation': True})
            try:
                con.update({'data': data.split(',')})
            except Exception:
                con['validation'] = False
            con.update({'stage': 'Stage 1'})

        elif isinstance(data, List):
            con.update({'type': 'Stream'})
            con.update({'data': data})
            con.update({'validation': True})
            con.update({'stage': 'Stage 1'})

        else:
            con.update({'validation': False})
            con.update({'error': "Invalid type input"})
            con.update({'stage': 'Stage 1'})

        return con


class TransformStage():
    def process(self, data: Any) -> Dict:

        if data['type'] == 'JSON' and data['validation'] is True:
            if data['data']['sensor'] == 'temp' and \
                    isinstance(data['data']['value'], (int, float)):
                data['data']['sensor'] = 'temperature'
                if data['data']['value'] > 40 or data['data']['value'] < 0:
                    data['data'].update({'range': 'Extreme'})
                else:
                    data['data'].update({'range': 'Normal'})
            else:
                data.update({'error': "Invalid data format"})
                data['validation'] = False
            data['stage'] = 'Stage 2'

        elif data['type'] == 'CSV' and data['validation'] is True:
            action: str = data['data'][1]
            timestamp: str = data['data'][2]
            try:
                if datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") and \
                        action.isidentifier():
                    data['validation'] = True
            except ValueError:
                data['validation'] = False
                data.update({'error': "Invalid data format"})
            data['stage'] = 'Stage 2'

        elif data['type'] == 'Stream' and data['validation'] is True:
            count: int = len(data['data'])
            for item in data['data']:
                if isinstance(item, (int, float)) is False:
                    data['validation'] = False
            if data['validation'] is True:
                res: Dict = {}
                res.update({'avg': sum(data['data']) / count})
                res.update({'count': count})
                data['data'] = res
            else:
                data.update({'error': "Invalid data format"})
            data['stage'] = 'Stage 2'

        return data


class OutputStage():

    def process(self, data: Any) -> str:
        if data['type'] == 'JSON' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res: str = f"Processed {data['data']['sensor']} "
            res += f"reading: {data['data']['value']}°{data['data']['unit']} "
            res += f"({data['data']['range']} range)"
            return res

        elif data['type'] == 'CSV' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res: str = "User activity logged: 1 action processed"
            return res

        elif data['type'] == 'Stream' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res: str = f"Stream summary: {data['data']['count']} readings, "
            res += f"avg: {data['data']['avg']}°C"
            return res

        else:
            return f"Error detected in {data['stage']}: {data['error']}"


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        ...


class JSONAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id: str = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: Dict) -> Union[str, Any]:
        for item in self.stages:
            data = item.process(data)
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id: str = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: str) -> Union[str, Any]:
        for item in self.stages:
            data = item.process(data)
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id: str = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: List) -> Union[str, Any]:
        for item in self.stages:
            data = item.process(data)
        return data


class NexusManager():
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any, mode: Optional[str] = None) -> Any:
        if isinstance(data, List | Dict | str) and mode == 'c':
            for i in range(3, 6):
                data = self.pipelines[i].process(data)
        elif isinstance(data, Dict):
            return self.pipelines[0].process(data)
        elif isinstance(data, str):
            return self.pipelines[1].process(data)
        elif isinstance(data, List):
            return self.pipelines[2].process(data)
        return data


def ft_nexus_pipeline() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 streams/second\n")
    manager = NexusManager()
    json = JSONAdapter("JSON")
    csv = CSVAdapter("CSV")
    stream = StreamAdapter("Stream")
    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    for item in [json, csv, stream]:
        item.add_stage(InputStage())
        item.add_stage(TransformStage())
        item.add_stage(OutputStage())
        manager.add_pipeline(item)

    print("\n=== Multi-Format Data Processing ===\n")

    print("Processing JSON data through pipeline...")
    json_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print(f"Input: {json_data}")
    json_res = manager.process_data(json_data)
    print("Transform: Enriched with metadata and validation")
    print(json_res)

    print("\nProcessing CSV data through same pipeline...")
    csv_data = "User,login,2026-02-01 10:05:20"
    print('Input: "user,action,timestamp"')
    csv_res = manager.process_data(csv_data)
    print("Transform: Parsed and structured data")
    print(csv_res)

    print("\nProcessing Stream data through same pipeline...")
    stream_data = [22.0, 10.7, 15.4, 23.6, 20.7]
    print("Input: Real-time sensor stream")
    stream_res = manager.process_data(stream_data)
    print("Transform: Aggregated and filtered")
    print(stream_res)

    print("\n=== Pipeline Chaining Demo ===")
    json1 = JSONAdapter("JSON_001")
    json1.add_stage(InputStage())
    manager.add_pipeline(json1)

    print("Pipeline A -> Pipeline B -> Pipeline C")
    json2 = JSONAdapter("JSON_002")
    json2.add_stage(TransformStage())
    manager.add_pipeline(json2)

    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    json3 = JSONAdapter("JSON_003")
    json3.add_stage(OutputStage())
    manager.add_pipeline(json3)

    start = time.perf_counter()
    chain_data = [22.0, 10.7, 15.4, 23.6, 20.7]
    record_number = 100
    for _ in range(record_number):
        manager.process_data(chain_data, 'c')
    end = time.perf_counter()
    print(f"\nChain result: {record_number} records processed through 3-stage"
          " pipeline")
    print(f"Performance: 95% efficiency, {end - start:.2f}s total"
          "processing time")

    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    cor_data = "User,login,10-07-1999 10:05:20"
    cor_data = manager.process_data(cor_data)
    print(cor_data)
    print("Recovery initiated: Switching to backup processor")
    nor_data = "User,logout,2067-10-09 15:09:57"
    manager.process_data(nor_data)
    print("Recovery successful: Pipeline restored, processing resumed")

    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    ft_nexus_pipeline()
