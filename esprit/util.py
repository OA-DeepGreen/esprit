from datetime import datetime


def now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
