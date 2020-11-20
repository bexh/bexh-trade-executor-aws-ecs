from datetime import datetime


def get_current_utc_iso() -> str:
    dt = datetime.utcnow()
    return dt_to_iso(dt)


def dt_to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
