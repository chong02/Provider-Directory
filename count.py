import sys
import requests

# Global variables
carrier = sys.argv[0]
city = sys.argv[1]

if carrier == 'Kaiser' | carrier == 'kaiser':
    base_url = 'https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc/PractitionerRole'
    search_param = f'location.address-city={city}'
else:
    sys.exit('Error: carrier name not recognized')

full_query = base_url + search_param
json_object = requests.get(full_query)
total_entries = json_object.get('total')

print(f'{carrier} providers in {city}: {total_entries}')