"""AWS transcribe implementation."""
import argparse
import json
import logging
import time
from io import BytesIO
from uuid import uuid4

import boto3
from botocore.client import BaseClient

from base import BaseTranscribator

AWS_ACCESS_KEY = 'todo'
AWS_SECRET_KEY = 'todo'
AWS_SESSION_TOKEN = 'dev'
TRIES_AMOUNT: int = 60
TRIES_WAIT_SECONDS: int = 5

logger = logging.getLogger(__file__)


class AwsTranscribe(BaseTranscribator):
    """AWS transcribe implementation."""

    _bucket_name: str
    _s3_client: BaseClient
    _transcribe_client: BaseClient

    def __init__(self, language: str = 'uz-UZ') -> None:
        """Create transcribe client."""
        super().__init__(language)

        self._s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='us-east-1',
        )
        self._bucket_name = f'bucket-{uuid4().hex}'

        self._transcribe_client = boto3.client(
            'transcribe',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='us-east-1',
        )
        self._s3_client.create_bucket(Bucket=self._bucket_name)

    def transcribe(self, audio_path: str, speaker_labeling: bool = False) -> list[str]:
        """Transcribe audio record by AWS Transcribe service."""
        job_name = self._start_job(audio_path)
        logger.info('start transcribe {0}'.format(job_name))

        transcribe_uri = self._await_job(job_name)
        logger.info('transcribe completed {0}'.format(transcribe_uri))

        return self._get_job_response(transcribe_uri)

    def _start_job(self, audio_path: str) -> str:
        source_name: str = f'source-{uuid4().hex}'
        job_name: str = f'job-{uuid4().hex}'
        logger.info('transcribe {0} {1} {2}'.format(audio_path, source_name, job_name))
        self._s3_client.upload_file(audio_path, self._bucket_name, source_name)

        media_uri = f's3://{self._bucket_name}/{source_name}'
        self._transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            OutputBucketName=self._bucket_name,
            LanguageCode=self._language,
        )
        return job_name

    def _await_job(self, name: str) -> str:
        logger.info('waiting for task completion {0}'.format(name))
        max_tries = TRIES_AMOUNT
        while max_tries > 0:
            max_tries -= 1
            job = self._transcribe_client.get_transcription_job(TranscriptionJobName=name)['TranscriptionJob']
            job_status = job['TranscriptionJobStatus']
            logger.info('job status is {0}'.format(job_status))

            if job_status == 'COMPLETED':
                return f'{name}.json'

            elif job_status == 'FAILED':
                raise RuntimeError('Transcribe job failed')

            time.sleep(TRIES_WAIT_SECONDS)

        raise RuntimeError('Transcribe job timeout')

    def _get_job_response(self, uri: str) -> list[str]:
        response = BytesIO()
        self._s3_client.download_fileobj(self._bucket_name, uri, response)
        response_content = response.getvalue().decode()
        logger.info('got response {0}'.format(response_content))

        response_decoded: dict = json.loads(response_content)
        logger.info('decode response as json {0}'.format(response_decoded))

        return [
            row.get('transcript', '')
            for row in response_decoded.get('results', {}).get('audio_segments', [])
            if row.get('transcript')
        ]


if __name__ == '__main__':
    # run: python aws.py path=~/Downloads/sample.mp3
    parser = argparse.ArgumentParser()
    parser.add_argument('--path')
    args = parser.parse_args()

    model = AwsTranscribe()
    print(model.transcribe(args.path))
