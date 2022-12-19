from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
from requests import get, post, Session
from google.cloud.pubsub_v1 import SubscriberClient
from cv2 import VideoCapture
from datetime import datetime
from os import environ
from os.path import join
from PIL import Image
from json import loads
from google.oauth2.credentials import Credentials
import sqlite3
from pandas import DataFrame
from time import time

class DoorbellClient:
  class Operation:
    LISTEN = 'listen'
    GET_IMAGE = 'get_image'

  def _headers(self):
    return {'Content-Type': 'application/json', 
            'Authorization': f'Bearer {self._access_token}'}

  def __init__(self, configuration_file):
    self._device_id = ''
    self._session = None

    configuration = ConfigParser()
    configuration.read(configuration_file)
    self._project_id = configuration['doorbell']['project_id']
    self._client_id = configuration['doorbell']['client_id']
    self._client_secret = configuration['doorbell']['client_secret']
    self._refresh_token = configuration['doorbell']['refresh_token']
    self._directory = configuration['general']['directory']
    self._database = configuration['general']['database']

    self._cloud_project_id = configuration['cloud']['project_id']
    self._subscription_id = configuration['cloud']['subscription_id']
    self._credentials_file = configuration['cloud']['credentials_file']

    self._authorize()
    response = get(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices', headers=self._headers())

    for device in response.json()['devices']:
      if device['type'] == 'sdm.devices.types.DOORBELL':
        self._device_id = device['name'].split('/')[-1]

  def _authorize(self):
    parameters = {'client_id': self._client_id,
                  'client_secret': self._client_secret,
                  'grant_type': 'refresh_token', 
                  'refresh_token': self._refresh_token}
    response = post('https://oauth2.googleapis.com/token', params=parameters)
    if response.status_code == 200:
      tokens = response.json()
    else:
      print('Authorization failed.')
      print(f'Go to: https://nestservices.google.com/partnerconnections/{self._project_id}/auth?redirect_uri=https://www.google.com&access_type=offline&prompt=consent&client_id={self._client_id}&response_type=code&scope=https://www.googleapis.com/auth/sdm.service')
      return

    self._access_token = tokens['access_token']
    self._expiration_time = time() + tokens['expires_in']

  def _get_file_path(self, image_time, extension):
    file_name = f'{image_time.isoformat()}'.replace(':', '-')
    file_name += '.' + extension

    return join(self._directory, file_name)

  def get_image(self, event_id):
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraEventImage.GenerateImage", 
                          "params" : {
                            'eventId': event_id
                          }}, headers=self._headers())
    if response.status_code == 200:
      response_data = response.json()['results']
      response = get(response_data['url'], headers={'Authorization': f'Basic {response_data["token"]}'}, params={'width': 1152})
      print(response)
      if response.status_code == 200:
        file_path = self._get_file_path(datetime.strptime(response.headers['Date'], '%a, %d %b %Y %H:%M:%S %Z'), 'jpg')
        with open(file_path, 'wb') as file:
          file.write(response.content)
        return file_path
      else:
        return self.save_image()
    else:
      print('Generating image failed.')
      print(response.content.decode())

  def get_stream(self):
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraLiveStream.GenerateRtspStream", "params" : {} }, headers=self._headers())
    results = response.json()['results']
    stream = VideoCapture(results['streamUrls']['rtspUrl'])

  def save_image(self) -> str:
    response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                    json={"command" : "sdm.devices.commands.CameraLiveStream.GenerateRtspStream", "params" : {} }, headers=self._headers())
    results = response.json()['results']
    stream = VideoCapture(results['streamUrls']['rtspUrl'])
    if stream.isOpened():
      image = stream.read()[1]
      file_name = (datetime.now().isoformat() + '.png').replace(':', '-')
      file_path = join(self._directory, file_name)
      Image.fromarray(image[..., ::-1]).save(file_path)

      response = post(f'https://smartdevicemanagement.googleapis.com/v1/enterprises/{self._project_id}/devices/{self._device_id}:executeCommand', 
                      json={"command" : "sdm.devices.commands.CameraLiveStream.StopRtspStream", 
                            "params" : {'streamExtensionToken': results['streamExtensionToken']} }, headers=self._headers())

      return file_path
    else:
      print('Failed to open stream')

  def listen(self):
    def process_message(message):
      delay = (datetime.now().astimezone() - message.publish_time).seconds
      print(f'{delay} {message.publish_time.strftime("%c")}')
      if delay <= 30:
        if time() > self._expiration_time:
          self._authorize()
        message_data = loads(message.data.decode())
        for event_type, event_info in message_data['resourceUpdate']['events'].items():  
          file_path = self.get_image(event_info['eventId'])
          if file_path:
            database = sqlite3.connect(self._database)
            DataFrame([{'event': event_type.split('.')[-1],
                        'time': message.publish_time.isoformat(),
                        'image': file_path}]).to_sql('event', database, if_exists='append')
      message.ack()

    environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._credentials_file
    subscriber = SubscriberClient()
    path = subscriber.subscription_path(self._cloud_project_id, self._subscription_id)
    print(f'Listening to messages for {path}')
    future = subscriber.subscribe(path, process_message)
    with subscriber:
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
