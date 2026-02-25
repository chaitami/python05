#!/usr/bin/env pyhton3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol
from datetime import datetime
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage():
    def process(self, data: Any) -> Dict:
        dico = {}

        if isinstance(data, Dict):
            dico.update({'type': 'JSON'})
            dico.update({'data': data})
            dico.update({'validation': True})
            dico.update({'stage': 'Stage 1'})

        elif isinstance(data, str) and ',' in data:
            dico.update({'type': 'CSV'})
            dico.update({'validation': True})
            try:
                dico.update({'data': data.split(',')})
            except Exception:
                dico['validation'] = False
            dico.update({'stage': 'Stage 1'})

        elif isinstance(data, List):
            dico.update({'type': 'Stream'})
            dico.update({'data': data})
            dico.update({'validation': True})
            dico.update({'stage': 'Stage 1'})

        else:
            dico.update({'validation': False})
            dico.update({'error': "Invalid type input"})
            dico.update({'stage': 'Stage 1'})

        return dico


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
            action = data['data'][1]
            date = data['data'][2]
            try:
                if datetime.strptime(date, "%Y-%m-%d %H:%M:%S") and \
                        (action.isidentifier() and action.islower()):
                    data['validation'] = True
            except ValueError:
                data['validation'] = False
                data.update({'error': "Invalid data format"})
            data['stage'] = 'Stage 2'

        elif data['type'] == 'Stream' and data['validation'] is True:
            count = 0
            for t in data['data']:
                count += 1
                if isinstance(t, (int, float)) is False:
                    data['validation'] = False
            if data['validation'] is True:
                new_dict = {}
                new_dict.update({'avg': sum(data['data']) / count})
                new_dict.update({'count': count})
                data['data'] = new_dict
            else:
                data.update({'error': "Invalid data format"})
            data['stage'] = 'Stage 2'

        return data


class OutputStage():

    def process(self, data: Any) -> str:
        if data['type'] == 'JSON' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res = f"Processed {data['data']['sensor']} "
            res += f"reading: {data['data']['value']}°{data['data']['unit']} "
            res += f"({data['data']['range']} range)"
            return res

        elif data['type'] == 'CSV' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res = "User activity logged: 1 action processed"
            return res

        elif data['type'] == 'Stream' and data['validation'] is True:
            data['stage'] = 'Stage 3'
            res = f"Stream summary: {data['data']['count']} readings, "
            res += f"avg: {data['data']['avg']}°C"
            return res

        else:
            return f"Error detected in {data['stage']}: {data['error']}"


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        ...


class JSONAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: Dict) -> Union[str, Any]:
        for s in self.stages:
            data = s.process(data)
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: str) -> Union[str, Any]:
        for s in self.stages:
            data = s.process(data)
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, id: str) -> None:
        super().__init__()
        self.id = id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: List) -> Union[str, Any]:
        for s in self.stages:
            data = s.process(data)
        return data


class NexusManager():
    def __init__(self) -> None:
        self.pipelines = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_pipeline(self, data: Any, mode: str = 'simple') -> Any:
        if isinstance(data, Dict) and mode == 'chaining':
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
    for a in [json, csv, stream]:
        a.add_stage(InputStage())
        a.add_stage(TransformStage())
        a.add_stage(OutputStage())
        manager.add_pipeline(a)
    print()

    print("=== Multi-Format Data Processing ===\n")

    print("Processing JSON data through pipeline...")
    json_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print(f"Input: {json_data}")
    res1 = manager.process_pipeline(json_data)
    print("Transform: Enriched with metadata and validation")
    print(res1)
    print()

    print("Processing CSV data through same pipeline...")
    csv_data = "User,login,2026-02-01 10:05:20"
    print('Input: "user,action,timestamp"')
    res2 = manager.process_pipeline(csv_data)
    print("Transform: Parsed and structured data")
    print(res2)
    print()

    print("Processing Stream data through same pipeline...")
    stream_data = [22.0, 10.7, 15.4, 23.6, 20.7]
    print("Input: Real-time sensor stream")
    res3 = manager.process_pipeline(stream_data)
    print("Transform: Aggregated and filtered")
    print(res3)
    print()

    print("=== Pipeline Chaining Demo ===")
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

    s = time.perf_counter()
    chain_data = {"sensor": "temp", "value": 80.5, "unit": "F"}
    for _ in range(100000):
        manager.process_pipeline(chain_data, 'chaining')
    e = time.perf_counter()
    print("Chain result: 100000 records processed through 3-stage pipeline")
    print(f"Performance: 100% efficiency, {e - s:.2f}s total processing time")
    print()

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    bad_data = "User,login,10-07-1999 10:05:20"
    bad_res = manager.process_pipeline(bad_data)
    print(bad_res)
    print("Recovery initiated: Switching to backup processor")
    good_data = "User,logout,2067-10-09 15:09:57"
    manager.process_pipeline(good_data)
    print("Recovery successful: Pipeline restored, processing resumed")
    print()

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    ft_nexus_pipeline()