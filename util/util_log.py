from datetime import datetime
import logging as log

a = None
b = None


def init(filename, level=log.INFO):
    log.basicConfig(filename=filename, level=level)
    # pass


def start(method):
    global a
    a = datetime.now()
    log.info(('{0} started at {1}'.format(method, a)).encode('utf-8', errors='ignore'))


def stop(method):
    global b
    b = datetime.now()
    log.info(('{0} ended at: {1}'.format(method, b)).encode('utf-8', errors='ignore'))
    log.info(('{0} took: {1}'.format(method, b - a)).encode('utf-8', errors='ignore'))


def info(message):
    log.info(message.encode('utf-8', errors='ignore'))


def error(message):
    log.error(message.encode('utf-8', errors='ignore'))


def log_diff(message, res_dict):
    diff = []
    for k, v in res_dict.items():
        diff.extend([i['child'] for i in v])
    info(message+' {}'.format(len(diff)))

    return diff