import pandas as pd
import sys
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Determine carrier from commmand line
carrier = sys.argv[1]
if carrier == 'uhc':
    url = 'https://public.fhir.flex.optum.com/R4/Location?_count=100&address-state=CA'
elif carrier == 'anthem':
    url = 'https://cmsmanapi.anthem.com/fhir/cms_mandate/mcd/Location?_count=100&address-state=CA'

# Pull data from carrier API
session = requests.Session()
retry = Retry(connect=5, backoff_factor=2)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
request = session.get(url)
json_object = request.json()

# API Calls
providers = pd.json_normalize(json_object.get('entry'))
print('Initial Request Successful! Hang tight this will take quite some time...')
page_number = 1

while True:
    try:
        if page_number % 100 == 0:
            print(f'Completed: {page_number}')
        next_url = json_object.get('link')[1].get('url')
        next_request = session.get(next_url)
        next_json = next_request.json()
        providers = pd.concat([providers, pd.json_normalize(next_json.get('entry'))])
        page_number += 1
    except Exception as e:
        print(e)
        print(f'Total Number of Pages: {page_number}')
        print('Done!')
        break

providers.to_csv(f'ca_{carrier}_providers.csv', index=False)