# Import libraries
import sys
import requests

# Global variables
carrier = sys.argv[1]
city = sys.argv[2]

if carrier == 'Kaiser' or carrier == 'kaiser':
    base_url = 'https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc/PractitionerRole?'
    search_param = f'location.address-city={city}&location.address-state=CA'
else:
    sys.exit(f'Error: carrier name ({carrier}) not recognized')

full_query = base_url + search_param
request = requests.get(full_query)
json_object = request.json()
total_entries = json_object.get('total')

print(f'{carrier} providers in {city}: {total_entries}')