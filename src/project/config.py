from pydantic.dataclasses import dataclass
from decouple import config


@dataclass
class Config:
    DEBUG: bool = False
    REDIS_HOST: str = config("REDIS_HOST", default="localhost")
    REDIS_URI: str = "redis://%s:%s/%s" % (
        config("REDIS_HOST", default="localhost"),
        config("REDIS_PORT", default="6379"),
        config("REDIS_DBNUM", default="0"),
    )
    PGSQL_URI: str = "postgresql://%s:%s@%s/%s" % (
        config("PGSQL_USER", default="user"),
        config("PGSQL_PASS", default="password"),
        config("PGSQL_HOST", default="localhost"),
        config("PGSQL_DATABASE", default="mydb"),
    )
    MONGODB_NAME: str = config("MONGODB_NAME", default="test")
    MONGO_URI: str = "mongodb://%s:%s@%s" % (
        config("MONGODB_USER", default="user"),
        config("MONGODB_PASS", default="password"),
        config("MONGODB_HOST", default="localhost"),
    )


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default"
        },
        "rotating": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "default",
            "filename": config("LOG_PATH", default="app.log"),
            "when": "midnight",
            "backupCount": 3
        }
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "CRITICAL",
            "propagate": True
        },
        "app": {
            "handlers": ["rotating"],
            "level": "DEBUG"
        }
    }
}


# Usage
if __name__ == '__main__':
    cfg = Config()
    print(cfg.MONGO_URI)
