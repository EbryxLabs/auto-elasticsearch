import logging

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

            for engine in engines:
                if engine in instance['Engine']:
                    self.instances[name] = instance

        logger.debug('Instances to parse:', self.instances.keys())
