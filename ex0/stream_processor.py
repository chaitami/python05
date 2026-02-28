
from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        res = "An unexpected error occured"
        try:
            result = f"{len(data)} "
            result += f"{sum(data)} "
            result += f"{sum(data) / len(data)}"
            res = self.format_output(result)
            if res == "An unexpected error occured":
                raise Exception(res)
        except Exception as e:
            print(e)
        finally:
            return res

    def validate(self, data: Any) -> bool:
        print(f"Processing data: {data}")
        for value in data:
            if isinstance(value, int) is False:
                return False
        return True

    def format_output(self, result: str) -> str:
        n1, n2, n3 = result.split()
        return f"Processed {n1} numeric values, sum={n2}, avg={n3}"


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        res = "An unexpected error occured"
        try:
            result = f"{len(data)} "
            result += f"{len(data.split())}"
            res = self.format_output(result)
            if res == "An unexpected error occured":
                raise Exception(res)
        except Exception as e:
            print(e)
        finally:
            return res

    def validate(self, data: Any) -> bool:
        print(f'Processing data: "{data}"')
        if isinstance(data, str) is True:
            return True
        return False

    def format_output(self, result: str) -> str:
        nc, nw = result.split()
        return f"Processed text: {nc} characters, {nw} words"


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        res = "An unexpected error occured"
        try:

            res = self.format_output(data)
            if res == "An unexpected error occured":
                raise Exception(res)
        except Exception as e:
            print(e)
        finally:
            return res

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
    case1 = NumericProcessor()
    try:
        if case1.validate(data1) is True:
            print("Validation: Numeric data verified")
        else:
            raise ValueError("Error: Invalid numeric data")
        res1 = case1.process(data1)
        print(f"Output: {res1}")
    except ValueError as e:
        print(e)

    data2 = "Hello Nexus World"
    print("\nInitializing Text Processor...")
    case2 = TextProcessor()
    try:
        if case2.validate(data2) is True:
            print("Validation: Text data verified")
        else:
            raise ValueError("Error: Invalid text data")
        res = case2.process(data2)
        print(f"Output: {res}")
    except ValueError as e:
        print(e)

    data3 = "ERROR: Connection timeout"
    print("\nInitializing Log Processor...")
    case3 = LogProcessor()
    try:
        if case3.validate(data3) is True:
            print("Validation: Log entry verified")
        else:
            raise ValueError("Error: Invalid log entry")
        res = case3.process(data3)
        print(f"Output: {res}")
    except ValueError as e:
        print(e)

    print("\n=== Polymorphic Processing Demo ===")
    all = [(NumericProcessor(), [1, 2, 3]),
           (TextProcessor(), "hello world!"),
           (LogProcessor(), "INFO: System ready")]
    counter = 1
    for o, data in all:
        print(f"Result {counter}: {o.process(data)}")
        counter += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    ft_stream_processor()
