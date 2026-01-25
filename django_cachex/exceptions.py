from typing import Any


class ConnectionInterruptedError(Exception):
    def __init__(self, connection: Any, parent: Any = None) -> None:
        self.connection = connection

    def __str__(self) -> str:
        error_type = type(self.__cause__).__name__
        error_msg = str(self.__cause__)
        return f"Redis {error_type}: {error_msg}"


# Backwards compatibility alias
ConnectionInterrupted = ConnectionInterruptedError


class CompressorError(Exception):
    pass


class SerializerError(Exception):
    pass
