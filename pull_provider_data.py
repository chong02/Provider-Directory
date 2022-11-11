import pandas as pd
import sys
import requests

# Determine carrier from commmand line
carrier = sys.argv[1]
if carrier == 'uhc':
    url = 'https://public.fhir.flex.optum.com/R4/Location?_count=100&address-state=CA'
elif carrier == 'anthem':
    url = 'https://cmsmanapi.anthem.com/fhir/cms_mandate/mcd/Location?_count=100&address-state=CA'

# Pull data from carrier API
request = requests.get(url)
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
        next_request = requests.get(next_url)
        next_json = next_request.json()
        providers = pd.concat([providers, pd.json_normalize(next_json.get('entry'))])
        page_number += 1
    except:
        print('Done!')
        break

providers.to_csv(f'ca_{carrier}_providers.csv', index=False)