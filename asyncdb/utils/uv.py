import asyncio


def install_uvloop():
    """install uvloop and set as default loop for asyncio."""
    try:
        import uvloop  # noqa # pylint: disable=import-outside-toplevel
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        uvloop.install()
    except ImportError:
        pass
