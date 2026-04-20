class CollectorError(Exception):
    """Base for all collector errors."""


class ConfigError(CollectorError):
    """Configuration file invalid or missing."""


class CheckpointError(CollectorError):
    """Checkpoint read/write failed."""


class SourceError(CollectorError):
    """S3 source failure (auth, not-found, throttled...)."""


class MapperError(CollectorError):
    """Mapper failed to format an event."""


class SinkSendError(CollectorError):
    """Sink could not deliver to the SIEM after retries."""


class FilterCompileError(CollectorError):
    """Filter expression did not compile."""
