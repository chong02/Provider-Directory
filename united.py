import requests
import pandas as pd
import numpy as np
from cities import *
from specialties import *
import time

# KP API variables
base = 'https://public.fhir.flex.optum.com/R4/'
hcs = 'HealthcareService'
loc = 'Location'
org = 'Organization'
dr = 'Practitioner'
role = 'PractitionerRole'

# Bay Area cities
bay_area = np.array([east_bay])
bay_area = np.append(bay_area, south_bay).flatten()
bay_area = np.append(bay_area, peninsula).flatten()
bay_area = np.append(bay_area, north_bay).flatten()

### Location DataFrame ###

# Initialize arrays to hold information
loc_refs = np.array([])
loc_names = np.array([])
loc_addresses = np.array([])
loc_cities = np.array([])
loc_zips = np.array([])
loc_lats = np.array([])
loc_longs = np.array([])
loc_statuses = np.array([])

# Fill arrays
for city in bay_area:
    city_loc_bundle = requests.get(base + loc + f'?address-city={city}&address-state=CA&_count=100').json()
    
    # Check if city has any Location resources
    try:
        city_loc_bundle_total = city_loc_bundle['total']
    except KeyError: # If rate limited, sleep for one second and retry
        print('Retrying...')
        time.sleep(1)
        city_loc_bundle = requests.get(base + loc + f'?address-city={city}&address-state=CA&_count=100').json()
        city_loc_bundle_total = city_loc_bundle['total']
    if not city_loc_bundle_total:
        continue
    else: 
        # Create useful variables
        total_items = city_loc_bundle['total']
        total_iterations = (total_items // 100) + 1
        obj = city_loc_bundle['entry']
        iteration = 1

        # While loop for pagination
        while iteration <= total_iterations:
            # Check if no pagination required
            if total_items <= 100:
                next_url = None
            else:
                next_url = city_loc_bundle['link'][1]['url']
            for item in obj:
                resource = item['resource']
                # Find values in Location resource item
                ref = resource['id']
                try:
                    name = resource['name']
                except KeyError:
                    name = 'N/A'
                address = resource['address']['text']
                city_name = resource['address']['city']
                zip_code = resource['address']['postalCode']
                lat = resource['position']['latitude']
                long = resource['position']['latitude']
                status = resource['status']

                # Append to array
                loc_refs = np.append(loc_refs, ref)
                loc_names = np.append(loc_names, name)
                loc_addresses = np.append(loc_addresses, address)
                loc_cities = np.append(loc_cities, city_name)
                loc_zips = np.append(loc_zips, zip_code)
                loc_statuses = np.append(loc_statuses, status)

            # Finished with items in obj; reinitialize variables
            if total_items > 100:
                city_loc_bundle = requests.get(next_url).json()
                try:
                    obj = city_loc_bundle['entry']
                except KeyError: # If rate limited, sleep for one second and retry
                    print('Retrying...')
                    time.sleep(1)
                    city_loc_bundle = requests.get(next_url).json()
                    obj = city_loc_bundle['entry']
            iteration += 1

# Create Location DataFrame
loc_df = pd.DataFrame(data={'id':loc_refs,
                            'name':loc_names,
                            'address':loc_addresses,
                            'city':loc_cities,
                            'zip':loc_zips,
                            'status':loc_statuses})

# Print status
print('Finished with Location DataFrame, moving on to Providers')

### Providers DataFrame ###

# Initialize arrays to hold information
hcs_refs = np.array([])
hcs_loc_refs = np.array([])
hcs_names = np.array([])
hcs_specialties = np.array([])
hcs_statuses = np.array([])
hcs_phones = np.array([])

# Fill arrays
for specialty in mental_health: # For loop to loop through specialties
    city_hcs_bundle = requests.get(base + hcs + f'?service-category=prov&location.address-state=CA&specialty={specialty}&_count=100').json()

    # Check if city has any HealthcareService resources
    try:
        city_hcs_bundle_total = city_hcs_bundle['total']
        print(city_hcs_bundle_total)
    except KeyError: # If rate limited, sleep for one second and retry
        print('Retrying...')
        time.sleep(1)
        city_hcs_bundle = requests.get(base + hcs + f'?service-category=prov&location.address-state=CA&specialty={specialty}&_count=100').json()
        city_hcs_bundle_total = city_hcs_bundle['total']
    if not city_hcs_bundle_total:
        continue
    else:
        # Create useful variables
        total_items = city_hcs_bundle['total']
        total_iterations = (total_items // 100) + 1
        obj = city_hcs_bundle['entry']
        iteration = 1

        # For loop
        while iteration <= total_iterations:
            # Check if no pagination required
            if total_items <= 100:
                next_url = None
            else:
                next_url = city_hcs_bundle['link'][1]['url']
            for item in obj:
                resource = item['resource']
                # Find values in HealthcareService resource item
                ref = resource['id']
                loc_ref = resource['location'][0]['reference'][9:]
                name = resource['name']
                try:
                    specialty = resource['specialty'][0]['coding'][0]['display']
                except KeyError:
                    specialty = 'N/A'
                status = resource['active']
                phone = resource['telecom'][0]['value']

                # Append to array
                hcs_refs = np.append(hcs_refs, ref)
                hcs_loc_refs = np.append(hcs_loc_refs, loc_ref)
                hcs_names = np.append(hcs_names, name)
                hcs_specialties = np.append(hcs_specialties, specialty)
                hcs_statuses = np.append(hcs_statuses, status)
                hcs_phones = np.append(hcs_phones, phone)

            # Finished with items in obj; reinitialize variables
            if total_items > 100:
                city_hcs_bundle = requests.get(next_url).json()
                try:
                    obj = city_hcs_bundle['entry']
                except KeyError: # If rate limited, sleep for one second and retry
                    print('Retrying...')
                    time.sleep(1)
                    city_hcs_bundle = requests.get(next_url).json()
                    obj = city_hcs_bundle['entry']
            iteration += 1

# Create Providers DataFrame
providers_df = pd.DataFrame(data={'id':hcs_refs,
                                  'location':hcs_loc_refs,
                                  'name':hcs_names,
                                  'specialty':hcs_specialties,
                                  'status':hcs_statuses,
                                  'phone':hcs_phones})

# Print status
print('Finished with Providers DataFrame, moving on to merging')

### Merge ###

# Combine DataFrames
united = providers_df.merge(right=loc_df,
                            how='left',
                            left_on='location',
                            right_on='id')
united = united.rename(columns={'id_x':'provider_id',
                                'name_x':'name',
                                'status_x':'provider_status',
                                'id_y':'location_id', # Drop
                                'name_y':'location_name', # Drop
                                'status_y':'location_status'}) \
               .drop(['location_id', 'location_name'], axis=1)

# Export to csv
united.to_csv('united_resource.csv')