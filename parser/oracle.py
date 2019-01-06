import logging
from parser import Parser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)


class OracleParser(Parser):
    def __init__(self, rds_instances):
        super().__init__(rds_instances, ['oracle'])

    def _prune_listener(self, message):

        if 'connect_data' not in message.lower() or \
                'host=__jdbc__' in message.lower() or \
                ('user=rdsdb' in message.lower() and
                 'command=status' in message.lower()):
            return

        while '  ' in message:
            message = message.strip().strip('\t') \
                .replace('  ', ' ').replace('\t', ' ')

        return message

    def _parse(self, group_name, streams):

        if not group_name.endswith('listener'):
            return

        for stream in streams:

            self.show_stream_separator(group_name)
            if not stream.get('events'):
                continue

            for event in stream['events']:

                message = self._prune_listener(event['message'])

                logger.info('[%s] %s' % (self.get_time(event[
                    'timestamp']), message)) if message else None
