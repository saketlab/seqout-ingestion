import datetime
import functools
import inspect

import humanfriendly
import typer


def _parse_chunk_size(value: str) -> int:
    try:
        return humanfriendly.parse_size(value)
    except humanfriendly.InvalidSize:
        raise typer.BadParameter(f"expected size string, got '{value}'")


def _parse_timespan(value: str) -> int:
    try:
        return humanfriendly.parse_timespan(value)
    except humanfriendly.InvalidTimespan:
        raise typer.BadParameter(f"expected timespan string, got '{value}'")


def _parse_date(value: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise typer.BadParameter(f"expected YYYY-MM-DD date string, got '{value}'")


def _validate_date_range(func):
    sig = inspect.signature(func)

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        start = bound.arguments.get("start_date")
        end = bound.arguments.get("end_date")

        if start is not None and end is not None and start > end:
            raise typer.BadParameter(
                f"start date ({start}) must be <= end date ({end})"
            )

        return func(*args, **kwargs)

    # preserve original signature for typer introspection
    wrapper.__signature__ = sig  # type: ignore
    return wrapper
