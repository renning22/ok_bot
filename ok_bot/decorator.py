from absl import app, logging


def try_catch_loop(f):
    def applicator(*args, **kwargs):
        while True:
            try:
                f(*args, **kwargs)
            except Exception as ex:
                logging.error('exception in %s', f.__name__, exc_info=True)

    return applicator


def _testing(_):

    @try_catch_loop
    def foo():
        from time import sleep
        sleep(1)
        raise Exception('aaaaaaaaaaa')

    foo()


if __name__ == '__main__':
    app.run(_testing)
