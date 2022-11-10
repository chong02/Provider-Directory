import pandas as pd
import requests

base_url = 'https://public.fhir.flex.optum.com/R4'
location_CA = '/Location?_count=100&address-state=CA'
full_url = base_url + location_CA
request = requests.get(full_url)
json_object = request.json()

uhc_providers = pd.json_normalize(json_object.get('entry'))
print('Initial Request Successful! Hang tight this will take quite some time...')
page_number = 1

while True:
    try:
        if page_number % 100 == 0:
            print(f'Completed: {page_number}')
        next_url = json_object.get('link')[1].get('url')
        next_request = requests.get(next_url)
        next_json = next_request.json()
        uhc_providers = pd.concat([uhc_providers, pd.json_normalize(next_json.get('entry'))])
        page_number += 1
    except:
        print('Done!')
        break

uhc_providers.to_csv('ca_uhc_providers.csv', index=False)