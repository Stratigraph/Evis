import httplib2
import pprint
import sys

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

def main():
  storage = Storage('bigquery_credentials.dat')
  credentials = storage.get()

  if credentials is None or credentials.invalid:
    # Run oauth2 flow with default arguments.
    credentials = tools.run_flow(FLOW, storage, tools.argparser.parse_args([]))

  http = httplib2.Http()
  http = credentials.authorize(http)

  bigquery_service = build('bigquery', 'v2', http=http)

  try:
    query_request = bigquery_service.jobs()
    #query_data = {'query':'SELECT TOP( title, 10) as title, COUNT(*) as revision_count FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;'}
    #query_data = {'query':'SELECT MonthYear, Actor1Code, Actor2Code FROM [gdelt-bq:full.events] WHERE Actor1Code IS NOT NULL LIMIT 5'}
    query_data = {'query':'SELECT Year, Actor1Name, Actor2Name, Count FROM ( SELECT Actor1Name, Actor2Name, Year, COUNT(*) Count, RANK() OVER(PARTITION BY YEAR ORDER BY Count DESC) rank FROM (SELECT Actor1Name, Actor2Name, Year FROM [gdelt-bq:full.events] WHERE Actor1Name < Actor2Name and Actor1CountryCode != \'\' and Actor2CountryCode != \'\' and Actor1CountryCode!=Actor2CountryCode), (SELECT Actor2Name Actor1Name, Actor1Name Actor2Name, Year FROM [gdelt-bq:full.events] WHERE Actor1Name > Actor2Name and Actor1CountryCode != \'\' and Actor2CountryCode != \'\' and Actor1CountryCode!=Actor2CountryCode), WHERE Actor1Name IS NOT null AND Actor2Name IS NOT null GROUP EACH BY 1, 2, 3 HAVING Count > 100 ) WHERE rank=1 ORDER BY Year'}
    query_response = query_request.query(projectId=PROJECT_NUMBER,
                                         body=query_data).execute()
    #pretty printer
    print 'Query Results:'
    for row in query_response['rows']:
      result_row = []
      for field in row['f']:
        result_row.append(field['v'])
      print ('\t').join(result_row)

  except HttpError as err:
    print 'Error:', pprint.pprint(err.content)

  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")

if __name__ == '__main__':
  main()