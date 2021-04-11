from typing import List, Generator, Union
import json

import boto3
from botocore.exceptions import ClientError

import logging
import logging.config
import yaml


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)


class BucketReader:
    def __init__(self, resource='s3', *args, **kwargs) -> None:
        self.s3 = boto3.client(resource, *args, **kwargs)

    def list_files(self, bucket: str, key: str) -> List[str]:
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            files = response['Body'].read().decode('utf-8').split('\n')
            return files
        except ClientError as e:
            logger.debug(e)

    def get_json(self, bucket: str, items: Union[str, List[str]]) -> Generator[str, None, None]:
        items = [items] if isinstance(items, str) else items
        for item in items:
            body = self.s3.get_object(Bucket=bucket, Key=item)['Body'].read()
            yield json.dumps(json.loads(body))
