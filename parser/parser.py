import logging
import datetime as dt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)


class Parser:

    def __init__(self, rds_instances, engines):

        self.instances = dict()
        for name, instance in rds_instances.items():
            if not instance.get('Engine'):
                continue

            if not instance.get('logGroups'):
                continue

            for engine in engines:
                if engine in instance['Engine']:
                    self.instances[name] = instance

        logger.debug('Instances to parse:', self.instances.keys())

    def show_stream_separator(self, group_name):
        logger.info('')
        logger.info('#' + '=' * 60)
        logger.info('Log events from %s' % (group_name))
        logger.info('')

    def get_time(self, timestamp):
        return str(dt.datetime.fromtimestamp(timestamp / 1000))

    def parse(self):
        for _, instance in self.instances.items():
            for name, streams in instance['logGroups'].items():
                self._parse(name, streams)

    def _parse(self, name, streams):
        pass
