#!/usr/bin/env python3
from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        ret = "An unexpected error occured"
        try:
            result = f"{len(data)} "
            result += f"{sum(data)} "
            result += f"{sum(data) / len(data)}"
            ret = self.format_output(result)
            if ret == "An unexpected error occured":
                raise Exception(ret)
        except Exception as e:
            print(e)
        finally:
            return ret

    def validate(self, data: Any) -> bool:
        print(f"Processing data: {data}")
        for d in data:
            if isinstance(d, int) is False:
                return False
        return True

    def format_output(self, result: str) -> str:
        d1, d2, d3 = result.split()
        return f"Processed {d1} numeric values, sum={d2}, avg={d3}"


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        ret = "An unexpected error occured"
        try:
            result = f"{len(data)} "
            result += f"{len(data.split())}"
            ret = self.format_output(result)
            if ret == "An unexpected error occured":
                raise Exception(ret)
        except Exception as e:
            print(e)
        finally:
            return ret

    def validate(self, data: Any) -> bool:
        print(f'Processing data: "{data}"')
        if isinstance(data, str) is True:
            return True
        return False

    def format_output(self, result: str) -> str:
        d1, d2 = result.split()
        return f"Processed text: {d1} characters, {d2} words"


class LogProcessor(DataProcessor):

    def __init__(self) -> None:
        ...

    def process(self, data: Any) -> str:
        ret = "An unexpected error occured"
        try:

            ret = self.format_output(data)
            if ret == "An unexpected error occured":
                raise Exception(ret)
        except Exception as e:
            print(e)
        finally:
            return ret

    def validate(self, data: Any) -> bool:
        print(f'Processing data: "{data}"')
        if data.split()[0] == "ERROR:" or data.split()[0] == "INFO:":
            return True
        return False

    def format_output(self, result: str) -> str:
        if result.split()[0] == "ERROR:":
            info = result.removeprefix('ERROR: ')
            state = result.removesuffix(f": {info}")
            res = f"[ALERT] {state} level detected: {info}"
        else:
            info = result.removeprefix('INFO: ')
            state = result.removesuffix(f": {info}")
            res = f"[INFO] {state} level detected: {info}"
        return (res)


def ft_stream_processor() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    data1 = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    num = NumericProcessor()
    try:
        if num.validate(data1) is True:
            print("Validation: Numeric data verified")
        else:
            raise ValueError("Error: Invalid numeric data")
        res1 = num.process(data1)
        print(f"Output: {res1}")
    except ValueError as e:
        print(e)
    finally:
        print()

    data2 = "Hello Nexus World"
    print("Initializing Text Processor...")
    words = TextProcessor()
    try:
        if words.validate(data2) is True:
            print("Validation: Text data verified")
        else:
            raise ValueError("Error: Invalid text data")
        res = words.process(data2)
        print(f"Output: {res}")
    except ValueError as e:
        print(e)
    finally:
        print()

    data3 = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    log = LogProcessor()
    try:
        if log.validate(data3) is True:
            print("Validation: Log entry verified")
        else:
            raise ValueError("Error: Invalid log entry")
        res = log.process(data3)
        print(f"Output: {res}")
    except ValueError as e:
        print(e)
    finally:
        print()

    print("=== Polymorphic Processing Demo ===")
    ty = [(NumericProcessor(), [1, 2, 3]),
          (TextProcessor(), "hello world!"),
          (LogProcessor(), "INFO: System ready")]
    for t, d in ty:
        print(t.process(d))
    print()

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    ft_stream_processor()