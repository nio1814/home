from argparse import ArgumentParser
from configparser import ConfigParser
from requests import get, post, Session
from requests_oauthlib import OAuth2Session
from google.cloud.pubsub import SubscriberClient
from cv2 import VideoCapture
from datetime import datetime
from os.path import join
from PIL import Image
from google.oauth2 import service_account
from json import loads


class DoorbellClient:
  class Operation:
    LISTEN = 'listen'
    GET_IMAGE = 'get_image'

  def _headers(self):
    return {'Content-Type': 'application/json', 
            'Authorization': f'Bearer {self._access_token}'}

  def __init__(self, configuration_file, directory='C:\\Users\\niioa\\Pictures\\arundel\\front'):
    self._directory = directory
    self._device_id = ''
    self._session = None
    # self._session = OAuth2Session(self._CLIENT_ID)
    # token = self._session.fetch_token('https://oauth2.googleapis.com/token', client_secret=self._CLIENT_SECRET, code=authorization_code)

    configuration = ConfigParser()
    configuration.read(configuration_file)
    self._project_id = configuration['doorbell']['project_id']
    self._client_id = configuration['doorbell']['client_id']

    parameters = {'client_id': self._client_id,
                  'client_secret': configuration['doorbell']['client_secret'],
                  'code': configuration['doorbell']['authorization_code'],
                  'grant_type': 'authorization_code',
                  'redirect_uri': 'https://www.google.com'}
    response = post('https://oauth2.googleapis.com/token', params=parameters)
    if response.status_code == 200:
      tokens = response.json()
      self._refresh_token = tokens['refresh_token']
      print(f'Refresh token: {self._refresh_token}')
    else:
      parameters['grant_type'] = 'refresh_token'
      parameters['refresh_token'] = configuration['doorbell']['refresh_token']
      parameters.pop('code')
      parameters.pop('redirect_uri')
      response = post('https://oauth2.googleapis.com/token', params=parameters)
      if response.status_code == 200:
        tokens = response.json()
      else:
        print('Authorization failed.')
        print(f'Go to: https://nestservices.google.com/partnerconnections/{self._project_id}/auth?redirect_uri=https://www.google.com&access_type=offline&prompt=consent&client_id={self._client_id}&response_type=code&scope=https://www.googleapis.com/auth/sdm.service')
        return

    self._access_token = tokens['access_token']
    print(f'Access token: {self._access_token}')
    # response = post('https://www.googleapis.com/oauth2/v4/token', params=parameters)
    self._session = Session()
    self._session.auth = {}

    self._cloud_project_id = configuration['cloud']['project_id']
    self._subscription_id = configuration['cloud']['subscription_id']
    self._credentials_file = configuration['cloud']['credentials_file']

    response = get(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices', headers=self._headers())

    for device in response.json()['devices']:
      if device['type'] == 'sdm.devices.types.DOORBELL':
        self._device_id = device['name'].split('/')[-1]

  def get_image(self, event_id):
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraEventImage.GenerateImage", 
                          "params" : {
                            'eventId': event_id
                          }}, headers=self._headers(), params={'width': 1152})
    if response.status_code == 200:
      response_data = response.json()
      response = get(response_data['url'], params={'Authorization': f'Basic {response_data["token"]}'})
      print(response)
    else:
      print('Generating image failed.')
    return response
    

  def get_stream(self):
    # response = post(f'https://developer-api.nest.com/devices/cameras/{self._device_id}/snapshot_url', headers=self._headers())
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraLiveStream.GenerateRtspStream", "params" : {} }, headers=self._headers())
    results = response.json()['results']
    stream = VideoCapture(results['streamUrls']['rtspUrl'])

  def save_image(self):
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraLiveStream.GenerateRtspStream", "params" : {} }, headers=self._headers())
    results = response.json()['results']
    stream = VideoCapture(results['streamUrls']['rtspUrl'])
    image = stream.read()[1]
    file_name = (datetime.now().isoformat() + '.png').replace(':', '-')
    Image.fromarray(image[..., ::-1]).save(join(self._directory, file_name))

    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraLiveStream.StopRtspStream", 
                          "params" : {'streamExtensionToken': results['streamExtensionToken']} }, headers=self._headers())

  def listen(self):
    def process_message(message):
      print(f'Received {message.data!r}')

      message = loads(message.data.decode())
      for event_type, event_info in message['resourceUpdate']['events'].items():
        self.get_image(event_info['eventId'])

    with SubscriberClient(credentials=self._credentials_file) as subscriber:
      # name = f'projects/{self._project_id}/subscriptions/{self._subscription_id}'
      # subscriber.create_subscription(name=name, topic=self._TOPIC_NAME)
      path = subscriber.subscription_path(self._cloud_project_id, self._subscription_id)
      print(f'Listening to messages for {path}')
      future = subscriber.subscribe(path, process_message)

      try:
        future.result()
      except TimeoutError:
        print('timeout')
        future.cancel()
        future.result()
      except KeyboardInterrupt:
        print('Canceled')


if __name__ == "__main__":
  parser = ArgumentParser()
  parser.add_argument('config_file', type=str)
  parser.add_argument('--operation', type=str, default=DoorbellClient.Operation.GET_IMAGE)
  arguments = parser.parse_args()

  doorbell = DoorbellClient(arguments.config_file)
  if arguments.operation == doorbell.Operation.GET_IMAGE:
    doorbell.save_image()
  elif arguments.operation == doorbell.Operation.LISTEN:
    doorbell.listen()