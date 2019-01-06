import re
import logging
from parser import Parser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)


class PostgreSQLParser(Parser):
    def __init__(self, rds_instances):
        super().__init__(rds_instances, ['postgres'])

    def _prune(self, message):
        match = re.match(
            r'[\W\S]+(?P<t1>\(\d{0,10}\):)' +
            r'[\W\D\S]+(?P<t2>:\[\d{0,10}\]:)', message)

        if not(match and match.group('t1') and match.group('t2')):
            return

        message = message.split(match.group('t1'))[-1]
        while '  ' in message:
            message = message.strip().strip('\t') \
                .replace('  ', ' ').replace('\t', ' ')

        return message

    def _parse(self, group_name, streams):

        for stream in streams:

            self.show_stream_separator(group_name)
            if not stream.get('events'):
                continue

            for event in stream['events']:
                message = self._prune(event['message'])

                logger.info('[%s] %s' % (self.get_time(event[
                    'timestamp']), message)) if message else None
