import logging


def async_try_catch_loop(f):
    async def applicator(*args, **kwargs):
        while True:
            try:
                await f(*args, **kwargs)
            except:
                logging.error('exception in %s', f.__name__, exc_info=True)

    return applicator


def try_catch_loop(f):
    def applicator(*args, **kwargs):
        while True:
            try:
                f(*args, **kwargs)
            except:
                logging.error('exception in %s', f.__name__, exc_info=True)

    return applicator


def _testing():
    @try_catch_loop
    def foo():
        from time import sleep
        sleep(1)
        raise Exception('aaaaaaaaaaa')

    foo()


if __name__ == '__main__':
    _testing()
