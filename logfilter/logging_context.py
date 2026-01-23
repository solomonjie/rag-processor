from contextvars import ContextVar
import logging

trace_id_var = ContextVar("trace_id", default="-")

class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        return True