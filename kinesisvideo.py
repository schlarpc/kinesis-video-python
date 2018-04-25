import boto3
import botocore.exceptions
import types
try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

class KinesisVideo(object):
    """
    A wrapper around the three Kinesis Video services available in boto3/botocore,
    with automatic endpoint resolution for data plane services. Basically, you
    can call any client methods from kinesisvideo, kinesis-video-media, or
    kinesis-video-archived-media against this object and it should Just Work (TM).

    Caveats:
    * I used a lot of underscore properties, so this will probably break at some point
    * stream ARNs and endpoints are cached, so recreate the object if you delete/recreate a stream
    * PutMedia limitations:
        * model is hacked in because it's not present in botocore (yet?)
        * no per-fragment streaming response body for PutMedia (no botocore support)
        * no sigv4 signing on request body for PutMedia (no botocore support, same as Java producer)
    """

    _CONTROL_SERVICES = ('kinesisvideo', )
    _DATA_SERVICES = ('kinesis-video-media', 'kinesis-video-archived-media')

    def __init__(self, session=None):
        self._session = session or boto3.Session()
        self._methods = {}
        for service in self._CONTROL_SERVICES + self._DATA_SERVICES:
            prototype = self._get_client_for_service(service)
            for method in prototype.meta.method_to_api_mapping.keys():
                self._methods[method] = service

    @lru_cache()
    def _get_arn_for_stream_name(self, stream_name):
        response = self.describe_stream(
            StreamName=stream_name,
        )
        return response['StreamInfo']['StreamARN']

    @lru_cache()
    def _get_endpoint_for_stream_method(self, stream_arn, method):
        response = self.get_data_endpoint(
            StreamARN=stream_arn,
            APIName=method.upper(),
        )
        return response['DataEndpoint']

    @lru_cache()
    def _get_client_for_service(self, service, endpoint_url=None):
        client = self._session.client(service, endpoint_url=endpoint_url)
        if service == 'kinesis-video-media':
            client = self._patch_kinesis_video_media(client)
        return client

    @lru_cache()
    def _get_client_by_arguments(self, method, stream_name=None, stream_arn=None):
        service = self._methods[method]
        if service not in self._DATA_SERVICES:
            return self._get_client_for_service(service)
        if not (bool(stream_name) ^ bool(stream_arn)):
            raise botocore.exceptions.ParamValidationError(report=
                'One of StreamName or StreamARN must be defined ' + \
                'to determine service endpoint'
            )
        stream_arn = self._get_arn_for_stream_name(stream_name) if stream_name else stream_arn
        endpoint_url = self._get_endpoint_for_stream_method(stream_arn, method)
        return self._get_client_for_service(service, endpoint_url)

    def __getattr__(self, method):
        if method not in self._methods:
            return getattr(super(), method)
        kwarg_map = {'StreamName': 'stream_name', 'StreamARN': 'stream_arn'}
        def _api_call(**kwargs):
            filtered_kwargs = {kwarg_map[k]: v for k, v in kwargs.items() if k in kwarg_map}
            client = self._get_client_by_arguments(method, **filtered_kwargs)
            return getattr(client, method)(**kwargs)
        return _api_call

    @staticmethod
    def _patch_kinesis_video_media(client):
        client.meta.service_model._service_description['operations']['PutMedia'] = {
            'name': 'PutMedia',
            'http': {
                'method': 'POST',
                'requestUri': '/putMedia',
            },
            'input': {
                'shape': 'PutMediaInput',
            },
            'output': {
                'shape': 'PutMediaOutput',
            },
            'errors': [
                {
                    'shape': 'ResourceNotFoundException',
                },
                {
                    'shape': 'NotAuthorizedException',
                },
                {
                    'shape': 'InvalidEndpointException',
                },
                {
                    'shape': 'ClientLimitExceededException',
                },
                {
                    'shape': 'ConnectionLimitExceededException',
                },
                {
                    'shape': 'InvalidArgumentException',
                }
            ],
            'authtype': 'v4-unsigned-body',
        }
        client.meta.service_model._shape_resolver._shape_map['PutMediaInput'] = {
            'type': 'structure',
            'required': [
                'FragmentTimecodeType',
                'ProducerStartTimestamp',
            ],
            'members': {
                'FragmentTimecodeType': {
                    'shape': 'FragmentTimecodeType',
                    'location': 'header',
                    'locationName': 'x-amzn-fragment-timecode-type',
                },
                'ProducerStartTimestamp': {
                    'shape': 'Timestamp',
                    'location': 'header',
                    'locationName': 'x-amzn-producer-start-timestamp',
                },
                'StreamARN': {
                    'shape': 'ResourceARN',
                    'location': 'header',
                    'locationName': 'x-amzn-stream-arn',
                },
                'StreamName': {
                    'shape': 'StreamName',
                    'location': 'header',
                    'locationName': 'x-amzn-stream-name',
                },
                'Payload': {
                    'shape': 'Payload',
                },
            },
            'payload': 'Payload',
        }
        client.meta.service_model._shape_resolver._shape_map['PutMediaOutput'] = {
            'type': 'structure',
            'members': {
                'Payload': {
                    'shape': 'Payload',
                },
            },
            'payload': 'Payload',
        }
        client.meta.service_model._shape_resolver._shape_map['FragmentTimecodeType'] = {
            'type': 'string',
            'enum': [
                'ABSOLUTE',
                'RELATIVE',
            ],
        }
        client.put_media = types.MethodType(
            lambda self, **kwargs: self._make_api_call('PutMedia', kwargs),
            client,
        )
        client.meta.method_to_api_mapping['put_media'] = 'PutMedia'
        return client



def main():
    video = KinesisVideo()

    print(video.list_streams())

    response = video.put_media(
        StreamName='teststream',
        Payload=open('clusters.mkv', 'rb'),
        FragmentTimecodeType='ABSOLUTE',
        ProducerStartTimestamp=0,
    )

    import json
    for event in map(json.loads, response['Payload'].read().decode('utf-8').splitlines()):
        print(event)

if __name__ == '__main__':
    main()
