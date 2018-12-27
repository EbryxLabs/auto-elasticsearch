import os
import boto3
from requests_aws_sign import AWSV4Sign
from elasticsearch import Elasticsearch, RequestsHttpConnection

def get_es_client(host, port):
  # Establish credentials
  # msession = boto3.session.Session()
  msession = boto3.session.Session(aws_access_key_id=os.environ.get('ACCESS_KEY_ID'),
                                   aws_secret_access_key=os.environ.get('SECRET_ACCESS_KEY'))
  credentials = msession.get_credentials()
  region = msession.region_name or 'eu-west-1'

  # Elasticsearch settings
  service = 'es'
  auth=AWSV4Sign(credentials, region, service)
  es_client = Elasticsearch(host=host,
                            port=int(port),
                            connection_class=RequestsHttpConnection,
                            http_auth=auth,
                            use_ssl=False,
                            verify_ssl=False)

  if es_client.info() is not None:
    return es_client
  else: 
    print('ES Client couldn\'t be created successfully in get_es_client()')
    return None