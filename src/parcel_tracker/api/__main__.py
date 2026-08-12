"""``python -m parcel_tracker.api`` belépési pont."""
import uvicorn

from ..config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "parcel_tracker.api.main:app",
        host="0.0.0.0",  # noqa: S104 - konténerben futunk
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
