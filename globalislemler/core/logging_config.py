
import json
import logging
import logging.handlers
from pathlib import Path

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in ("args","asctime","created","exc_info","exc_text","filename","funcName",
                         "levelname","levelno","lineno","module","msecs","message","msg","name",
                         "pathname","process","processName","relativeCreated","stack_info","thread","threadName"):
                base[k] = v
        return json.dumps(base, ensure_ascii=False)

def setup_logging(log_dir: Path, level: str = "INFO") -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    text_log = log_dir / "ingest.log"
    json_log = log_dir / "ingest.jsonl"
    error_log = log_dir / "errors.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    h1 = logging.handlers.RotatingFileHandler(text_log, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
    h1.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s"))
    h1.setLevel(logger.level)

    h2 = logging.handlers.RotatingFileHandler(json_log, maxBytes=10*1024*1024, backupCount=10, encoding="utf-8")
    h2.setFormatter(JSONFormatter())
    h2.setLevel(logger.level)

    h3 = logging.handlers.RotatingFileHandler(error_log, maxBytes=2*1024*1024, backupCount=3, encoding="utf-8")
    h3.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s"))
    h3.setLevel(logging.WARNING)

    c = logging.StreamHandler()
    c.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s"))

    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.addHandler(h1); logger.addHandler(h2); logger.addHandler(h3); logger.addHandler(c)
