import re
import logging
from parser import Parser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)


class MySQLParser(Parser):

    def __init__(self, rds_instances):
        super().__init__(rds_instances, ['mysql', 'mariadb'])
        self.safe_uids = list()

    def _prune_general(self, message):

        match = re.match('\d{6} \d\d:\d\d:\d\d', message)
        if match:
            message = message.split(
                match.group())[-1].strip().strip('\t')
        else:
            message = message.strip().strip('\t')

        if message.lower().endswith('select 1'):
            self.safe_uids.append(message.split(' ')[0])
        elif 'rdsadmin@localhost' in message.lower():
            self.safe_uids.append(message.split(' ')[0])

        if message.split(' ')[0] in self.safe_uids:
            return

        return message

    def _prune_audit(self, message):
        if len(message.split(',')) < 3:
            return

        if message.split(',')[2] in ['rdsadmin'] and \
                message.split(',')[3] == 'localhost':
            return

        return ', '.join(message.split(',')[2:])

    def _prune_error(self, message):
        pass

    def _parse(self, group_name, streams):

        for stream in streams:

            self.show_stream_separator(group_name)
            if not stream.get('events'):
                continue

            for event in stream['events']:
                if group_name.endswith('error'):
                    message = self._prune_error(event['message'])

                elif group_name.endswith('audit'):
                    message = self._prune_audit(event['message'])

                elif group_name.endswith('general'):
                    message = self._prune_general(event['message'])

                logger.info('[%s] %s' % (self.get_time(event[
                    'timestamp']), message)) if message else None
