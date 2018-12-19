import requests
import os
import json

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

def read_config(foldername):
    if not os.path.isdir(foldername):
        exit('No config folder exists.')

    conf_files = [os.path.join(foldername, x) for x in os.listdir(foldername) if x.endswith(('.conf','.cnf')) and not x.startswith('.')]
    conf_data = {}
    for conf in conf_files:
        conf_data[conf.strip('.conf').strip('.cnf').split('/')[-1]] = json.loads(open(conf, 'r').read())
            
    return conf_data

def  make_query(data):
    duration = data['global']['duration'] if data['global'].get('duration') else '5m'
    min_count = str(data['global']['min_count']) if data['global'].get('min_count') else '500'
    global_whitelist_hosts = str()
    if data['global'].get('whitelist'):
        if data['global']['whitelist'].get('hosts'):
            for wh_host in data['global']['whitelist']['hosts']:
                global_whitelist_hosts += wh_host + '|'

    global_whitelist_hosts = global_whitelist_hosts.strip('|')
    if global_whitelist_hosts:
        extra = { 'regexp': { 'request_host': { 'value': global_whitelist_hosts } } }
        extra = json.dumps(extra)
    else:
        extra = str()

    query = QUERY.replace('{duration}', duration) \
                 .replace('{min_count}', min_count) \
                 .replace('{_extra_must_not}', extra)
    return json.loads(query)

def make_request(data, query):
    url = 'http://' + data['global']['endpoint'].strip('/').strip('http://') + '/' + data['global']['index'] + '/_search'
    print(url); print(json.dumps(query, indent=2)); print('#' * 60)

    response = requests.get(url, json=query)
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
                host_data['clients'].append({'ip': client['key'], 'req_count': client['doc_count']})

            if len(host_data['clients']) > 0:
                filtered_data.append(host_data)
        print(json.dumps(filtered_data, indent=2))
        return filtered_data
    
if __name__ == '__main__':
    confs = read_config('./configs')
    query = make_query(confs)
    make_request(confs, query)
    
