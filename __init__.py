import logging.config
import pathlib
import logging

BASE_DIR = pathlib.Path(__file__).parent.resolve()

DATA_DIR = BASE_DIR / 'data'


LOGGING_CONF = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': logging.DEBUG,
        },
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'level': logging.INFO,
            'filename': str(BASE_DIR / 'app.log'),
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['file'],
            'level': logging.DEBUG,
            'propagate': False
        }
    }
}


def get_logger(name: str):
    logging.config.dictConfig(LOGGING_CONF)
    return logging.getLogger(name)


logger = get_logger('api_entrepises')
