import json
import time
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)

session = boto3.session.Session()


def get_rds_instances():

    rds_instances = dict()
    rds = session.client('rds')

    logger.info('Getting all db instances of RDS...')
    instances = rds.describe_db_instances()
    for instance in instances.get('DBInstances'):
        if instance.get('DBInstanceIdentifier'):
            rds_instances[instance['DBInstanceIdentifier']] = {
                key: value for (key, value) in instance.items()
                if key in [
                    'Engine', 'EngineVersion', 'DBInstanceClass',
                    'DBInstanceStatus', 'DBInstanceArn',
                    'Endpoint', 'EnabledCloudwatchLogsExports'
                ]
            }

    logger.info('[%d] RDS instances fetched.' % (len(rds_instances.keys())))
    return rds_instances


def include_log_groups(rds_instances):

    logs = session.client('logs')

    logger.info('Getting all log groups of RDS from Cloudwatch...')
    groups = logs.describe_log_groups(logGroupNamePrefix='/aws/rds/')

    group_count = 0
    stream_count = 0

    for gr in groups.get('logGroups'):
        logger.debug('Getting streams from log group: %s' % (
            gr.get('logGroupName')))

        if not gr.get('logGroupName'):
            continue

        log_group_name = gr['logGroupName']
        group_count += 1

        streams = logs.describe_log_streams(logGroupName=log_group_name)
        if not streams.get('logStreams'):
            continue

        log_streams = streams['logStreams']
        stream_count += len(log_streams)

        instance_name = log_group_name.split('/instance/')[-1].split('/')[0]

        for iname, instance in rds_instances.items():
            if iname == instance_name and not instance.get('logGroups'):
                instance['logGroups'] = {log_group_name: log_streams}
            elif iname == instance_name and instance.get('logGroups'):
                instance['logGroups'][log_group_name] = log_streams

    logger.info('[%d] Log groups fetched.' % (group_count))
    logger.info('[%d] Log streams fetched.' % (stream_count))


def get_time_range(minutes=5):
    ctime = str(time.time())
    end_time = ctime.split('.')[0] + ctime.split('.')[-1][:3]
    start_time = str(int(ctime.split('.')[0]) - (
        60 * minutes)) + ctime.split('.')[-1][:3]

    return (int(start_time), int(end_time))


def reduce_event(event):
    return {
        key: value for (key, value) in event.items()
        if key not in ['logStreamName']
    }


def add_events(log_events, streams, counter):

    if not log_events.get('events'):
        return

    for event in log_events['events']:
        if not event.get('logStreamName'):
            continue

        for stream in streams:
            if stream.get('logStreamName') == event['logStreamName'] \
                    and not stream.get('events'):
                stream['events'] = [reduce_event(event)]
            elif stream.get('logStreamName') == event['logStreamName'] \
                    and stream.get('events'):
                stream['events'].append(reduce_event(event))

        counter['_'] += 1


def include_log_events(instances):

    logs = session.client('logs')
    logger.info('Getting all log events...')

    event_count = {'_': 0}
    for name, instance in instances.items():
        if not instance.get('logGroups'):
            continue

        log_groups = instance['logGroups']
        for group_name, streams in log_groups.items():
            log_events = logs.filter_log_events(
                logGroupName=group_name,
                startTime=get_time_range(5)[0]
            )

            add_events(log_events, streams, event_count)

    logger.info('[%d] Log events fetched.' % (event_count['_']))
    logger.info(json.dumps(instances, indent=2))


if __name__ == "__main__":
    instances = get_rds_instances()
    include_log_groups(instances)
    include_log_events(instances)
