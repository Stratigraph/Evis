import httplib2
from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

# Google Developer parameters
PROJECT_NUMBER = 'evis-904'
FLOW = flow_from_clientsecrets('client_secrets.json',
                             scope='https://www.googleapis.com/auth/bigquery')
storage = Storage('bigquery_credentials.dat')
credentials = storage.get()

if credentials is None or credentials.invalid:
  # Run oauth2 flow with default arguments.
  credentials = tools.run_flow(FLOW, storage, tools.argparser.parse_args([]))

http = httplib2.Http()
http = credentials.authorize(http)

bigquery_service = build('bigquery', 'v2', http=http)