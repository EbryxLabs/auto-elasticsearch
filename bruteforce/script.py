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
      },
      "must_not" : [{
        "regexp" : {
          "client" : {
            "value": "10.*|192.*|52.*"
          }
        }
      },{_extra_must_not}]
    }
  },
  "aggs": {
    "hosts": {
      "terms": {
        "field": "request_host",
        "size": 1000
      },
      "aggs": {
        "clients": {
          "terms": {
            "field": "client",
            "size": 1000,
            "min_doc_count": {min_count}
          } 
        }
      }
    }
  }
}
'''

def define_params():

  parser = argparse.ArgumentParser()
  parser.add_argument('--path', help='path to config folder having at ' +
                      'least global.conf file. [default: ./configs]', default='configs')
  return parser.parse_args()

def read_config(foldername):
  if not os.path.isdir(foldername):
    exit('No config folder exists: %s' % (foldername))

  conf_files = [os.path.join(foldername, x) for x in os.listdir(foldername) if x.endswith(('.conf','.cnf')) and not x.startswith('.')]
  conf_data = {}
  for conf in conf_files:
    conf_data[conf.strip('.conf').strip('.cnf').split('/')[-1]] = json.loads(open(conf, 'r').read())
          
  return conf_data

def make_query(data):
  duration = data['global']['duration'] if data['global'].get('duration') else '5m'
  min_count = str(data['global']['min_count']) if data['global'].get('min_count') else '500'
  
  global_whitelist_hosts = str()
  global_whitelist_ips = str()
  
  if data['global'].get('whitelist'):
    if data['global']['whitelist'].get('hosts'):
      for wh_host in data['global']['whitelist']['hosts']:
        global_whitelist_hosts += wh_host + '|'

    if data['global']['whitelist'].get('ips'):
      for wh_ip in data['global']['whitelist']['ips']:
        global_whitelist_ips += wh_ip + '|'

  global_whitelist_hosts = global_whitelist_hosts.strip('|')
  global_whitelist_ips = global_whitelist_ips.strip('|')

  extra_text = str()
  if global_whitelist_hosts:
    subquery = { 'regexp': { 'request_host': { 'value': global_whitelist_hosts } } }
    extra_text += json.dumps(subquery) + ','

  if global_whitelist_ips:
    subquery = { 'regexp': { 'client': { 'value': global_whitelist_ips } } }
    extra_text += json.dumps(subquery)

  query = QUERY.replace('{duration}', duration) \
               .replace('{min_count}', min_count) \
               .replace('{_extra_must_not}', extra_text)

  return json.loads(query)

def is_ip_whitelisted(data, host_name, client):

  if data.get(host_name) and data[host_name].get(
      ['whitelist']) and data[host_name]['whitelist'].get('ips'):

    for ip in data[host_name]['whitelist']['ips']:
      if ip == client['key']:
        return True
  return False

def make_request(data, query):
  url = 'http://' + data['global']['endpoint'].strip('/').strip('http://') + '/' + data['global']['index'] + '/_search'
  print(url); print(json.dumps(query, indent=2)); print('#' * 60)

  response, _count = (None, 0)
  while not response and _count < 5:
    try:
      response = requests.get(url, json=query)
    except:
      _count += 1
      logger.info('Could not send elasticsearch query request. ' +
                  'Retrying after 10 secs...')
      time.sleep(10)
  
  if not response:
    exit('Exiting progrma.')

  if response.status_code == 200:
    rdata = response.json()
    if not(rdata.get('aggregations') and rdata['aggregations'].get(
        'hosts') and rdata['aggregations']['hosts'].get('buckets')):
      exit('[%d] Unexpected data returned.' % (response.status_code))

    hosts = rdata['aggregations']['hosts']['buckets']
    filtered_data = list()
    for host in hosts:
      if not(host.get('clients') and host['clients'].get('buckets')):
        continue
      host_data = dict()
      host_data['name'] = host['key']
      host_data['clients'] = list()

      clients = host['clients']['buckets']
      for client in clients:
        if is_ip_whitelisted(data, host_data['name'], client):
          continue

        host_data['clients'].append({'ip': client['key'], 'req_count': client['doc_count']})

      if len(host_data['clients']) > 0:
        filtered_data.append(host_data)

    logger.debug(json.dumps(filtered_data, indent=2))
    return filtered_data

def post_on_slack(data, result):

  if not result:
    return

  text = 'Data for potential brute force attacks:\n'
  for entry in result:
    text += entry['name'] + '\n'
    for client in entry['clients']:
      text += '> *%s*: %d\n' % (client['ip'], client['req_count'])

  for url in data['global'].get('slack_urls', list()):
    logger.info('Sending to slack: %s' % url)
    
    response, _count = (None, 0)
    while not response and _count < 5:
      try:
        response = requests.post(url, json={'text': text})
      except:
        _count += 1
        logger.info('Could not send slack request. ' +
                    'Retrying after 10 secs...')
        time.sleep(10)

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

  