import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


def install_uvloop() -> None:
    """Install uvloop as the asyncio event loop policy when supported.

    ``uvloop`` only ships wheels for POSIX platforms (Linux, macOS). On
    unsupported platforms -- most notably Windows -- or when ``uvloop`` is
    not installed, this function is a safe no-op and the standard asyncio
    event loop policy remains active. This function is intentionally
    designed to never raise so it can be safely called as an import-time
    side effect.

    Returns:
        None.
    """
    if sys.platform.startswith("win"):
        # uvloop does not support Windows; never attempt to import or
        # activate it there, keep the standard asyncio policy instead.
        logger.debug("uvloop is not supported on Windows; skipping activation.")
        return
    try:
        import uvloop  # pylint: disable=import-outside-toplevel

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        uvloop.install()
    except ImportError:
        logger.debug(
            "uvloop is not installed; using the default asyncio event loop policy."
        )
    except Exception as exc:  # pylint: disable=broad-except
        # Defensive: never let event-loop policy activation break `import
        # asyncdb` regardless of the underlying platform/runtime cause.
        logger.debug(
            "uvloop could not be activated (%s); using the default asyncio "
            "event loop policy.",
            exc,
        )
