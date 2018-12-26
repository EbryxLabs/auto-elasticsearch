import os
import json
import time
import logging
import argparse
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handle = logging.StreamHandler()
handle.setLevel(logging.INFO)
handle.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logger.addHandler(handle)

QUERY = '''
{
  "size": 0,
  "query": {
    "bool" : {
      "must": {
        "range": {
          "timestamp": {
            "gte": "now-{duration}/m"
          }
        }
      }
    }
  },
  "aggs": {
    "unique_hosts": {
      "terms": {
        "field": "request_host",
        "size": 99999
      }
    }
  }
}'''

def define_params():

  parser = argparse.ArgumentParser()
  parser.add_argument('--path', help='path to config folder having at ' +
                      'least global.conf file. [default: ./configs]', default='configs')
  return parser.parse_args()

def read_config(foldername):
  if not os.path.isdir(foldername):
    exit('No config folder exists: %s' % (foldername))

  conf_files = [os.path.join(foldername, x) for x in os.listdir(foldername)
                if x.endswith(('.conf','.cnf')) and not x.startswith('.')]

  conf_data = {}
  for conf in conf_files:
    conf_data[conf.strip('.conf').strip('.cnf').split('/')[-1]] = json.loads(open(conf, 'r').read())
          
  return conf_data

def  make_query(data):
  duration = data['global']['duration'] if data['global'].get('duration') else '15m'
  query = QUERY.replace('{duration}', duration)
  return json.loads(query)

def make_request(data, query):

  url = 'http://' + data['global']['endpoint'].strip('/').strip('http://') + '/' + data['global']['index'] + '/_search'
  print(url); print(json.dumps(query, indent=2)); print('#' * 60)

  response, _count = (None, 0)
  while not response and _count < 5:
    try:
      response = requests.get(url, json=query)
    except:
      logger.info('Could not send elasticsearch request. Retrying after 10 secs...')
      time.sleep(10)
      _count += 1

  if not response:
    exit('Exiting program.')

  if response.status_code == 200:
    rdata = response.json()
    if not(rdata.get('aggregations') and rdata['aggregations'].get(
        'unique_hosts') and rdata['aggregations']['unique_hosts'].get('buckets')):
      exit('[%d] Unexpected data returned.' % (response.status_code))

    hosts = [entry.get('key') for entry in rdata['aggregations']['unique_hosts']['buckets']]

    if not os.path.isfile(data['global']['host_file']):
      open(data['global']['host_file'], 'w')

    hosts_on_disk = [entry.strip('\n') for entry in open(
      data['global']['host_file'], 'r').readlines()]

    new_hosts = list(set(hosts) - set(hosts_on_disk))
    
    hfile = open(data['global']['host_file'], 'w')
    hfile.writelines([entry + '\n' for entry in hosts])
    hfile.close()

    if not new_hosts:
      logger.info('No new host detected :)')

      logger.info('Following are the new hosts...') if new_hosts else None
      for index, nhost in enumerate(new_hosts):
        logger.info('%02d: %s' % (index + 1, nhost))

    return new_hosts

def post_on_slack(data, result):

  if not result:
    return

  for url in data['global'].get('slack_urls', list()):
    logger.info('Sending to slack: %s' % url)
    text = 'Following new hosts detected:\n```'
    for entry in result:
      text += entry + '\n'

    response, _count = (None, 0)
    while not response and _count < 5:
      try:
        response = requests.post(url, json={'text': text + '```'})
      except:
        logger.info('Could not send slack request. Retrying after 10 secs...')
        time.sleep(10)
        _count += 1

    if not response:
      exit('Exiting program.')
    
    if response.status_code == 200:
      logger.info('Pushed successfully.')
    else:
      logger.info('Could not push message: <(%s) %s>' % (
        response.status_code, response.content.decode('utf8')))

if __name__ == '__main__':

  params = define_params()
  confs = read_config(params.path)
  query = make_query(confs)
  result = make_request(confs, query)
  post_on_slack(confs, result)
