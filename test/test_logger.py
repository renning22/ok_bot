import unittest
import logging
import os
import re
import contextlib
import time

from ok_bot import logger


@contextlib.contextmanager
def temp_log_file():
    tmp_log_file = '/tmp/test_log.log'
    root_logger = logging.getLogger()
    fh = logging.FileHandler(tmp_log_file)
    fh.setFormatter(logging.Formatter(logger.LOG_FORMAT))
    root_logger.addHandler(fh)
    fd = open(tmp_log_file)
    try:
        yield fd
    finally:
        if os.path.exists(tmp_log_file):
            os.remove(tmp_log_file)
        fd.close()


class TestLogger(unittest.TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.DEBUG)
        self.sample_log = 'Risk comes from not knowing what you are doing.'

    def test_debug_is_logged(self):
        with self.assertLogs(logging.getLogger(), level='DEBUG') as context:
            logging.debug(self.sample_log)
        self.assertIn(f'DEBUG:root:{self.sample_log}',
                      context.output)

    def test_correct_log_origination_file(self):
        with temp_log_file() as log_fd:
            logging.info(self.sample_log)
            log_fd.flush()
            log = log_fd.read()
            self.assertTrue(
                re.search(
                    r'INFO    [0-9: ,-]+ test_logger.py:[0-9 ]+\]'
                    f' {self.sample_log}',
                    log) is not None
            )

    def test_correct_log_origination_file_in_transaction(self):
        with temp_log_file() as log_fd:
            trans_logger = logger.create_transaction_logger('TRANSACTION-1')
            trans_logger.info(self.sample_log)
            log_fd.flush()
            log = log_fd.read()
            self.assertTrue(
                re.search(
                    r'INFO    [0-9: ,-]+ test_logger.py:'
                    r'[0-9 ]+\] \[\+  0\.00s\]'
                    fr' {self.sample_log}',
                    log) is not None
            )

    def test_log_every_n_seconds(self):
        trans_logger = logger.create_transaction_logger('TRANSACTION-1')
        with self.assertLogs(logging.getLogger(), level='INFO') as context:
            for _ in range(5):
                trans_logger.log_every_n_seconds(
                    logging.INFO,
                    self.sample_log,
                    2
                )
                time.sleep(1)  # sleep 1 second
        self.assertEqual(len(context.output), 3)


if __name__ == '__main__':
    unittest.main()
