import eventlet
from absl import app, logging


class Future:
    def __init__(self):
        self._event = eventlet.event.Event()

    def set(self, result=None, exc=None):
        """Can be called multiple times, but only first one counts."""
        if self._event.ready():
            return False
        self._event.send(result=result, exc=exc)
        return True

    def get(self, timeout_sec=None):
        """it waits forever when timeout_sec is None .

        Returns None if timed out.
        """
        return self._event.wait(timeout=timeout_sec)


def _testing(_):
    f = Future()
    logging.info('start to sleep')
    logging.info('finish sleep: %s', f.get(2))

    f.set('AAA')
    logging.info('returns immediately: %s', f.get(5))
    f.set('BBB')
    f.set('CCC')
    logging.info('still AAA: %s', f.get(5))

    # Test set exception
    # f = Future()
    # f.set(exc=Exception('test exception'))
    # f.get()


if __name__ == '__main__':
    app.run(_testing)
