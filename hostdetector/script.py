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

def read_config(foldername):
    if not os.path.isdir(foldername):
        exit('No config folder exists.')

    conf_files = [os.path.join(foldername, x) for x in os.listdir(foldername) if x.endswith(('.conf','.cnf')) and not x.startswith('.')]
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

    response = requests.get(url, json=query)
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

        new_hosts = set(hosts) - set(hosts_on_disk)
        if not new_hosts:
            print('No new host detected :)')
        
        hfile = open(data['global']['host_file'], 'w')
        hfile.writelines([entry + '\n' for entry in hosts])
        hfile.close()

        print('Following are the new hosts...') if new_hosts else None
        for index, nhost in enumerate(new_hosts):
            print('%02d: %s' % (index + 1, nhost))
    
if __name__ == '__main__':
    confs = read_config('./configs')
    query = make_query(confs)
    make_request(confs, query)
    
