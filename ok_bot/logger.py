import logging as py_logging
import os
import time

from absl import app

os.makedirs('transaction', exist_ok=True)


class RelativeTimeFormatter(py_logging.Formatter):
    def __init__(self):
        super().__init__(fmt='%(message)s')
        self.created_time = time.time()

    def format(self, record):
        relative_time = time.time() - self.created_time
        prefix = f'{relative_time:12.8f} sec : '
        return prefix + super().format(record)


def create_transaction_logger(id):
    logger = py_logging.getLogger(f'absl.{id}')
    fh = py_logging.FileHandler(f'transaction/{id}.log')
    fh.setFormatter(RelativeTimeFormatter())
    logger.addHandler(fh)
    return logger


def init_global_logger():
    os.makedirs('transaction', exist_ok=True)
    if flags.FLAGS.logtofile:
        os.makedirs('log', exist_ok=True)
        logging.get_absl_handler().use_absl_log_file('ok_bot', 'log')
    if flags.FLAGS.alsologtoslack:
        logging.get_absl_logger().addHandler(SlackLoggingHandler('INFO'))


def _testing(_):
    logger = create_transaction_logger('test_id_0')
    logger.info('1111')
    logger.critical('2222')

    logger = create_transaction_logger('test_id_1')
    logger.info('3333')
    time.sleep(1)
    logger.critical('4444')


if __name__ == '__main__':
    app.run(_testing)
