# Import necessary libraries
import os.path
import sys
import requests
import re
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter("ignore", UserWarning)
from geopy.geocoders import Nominatim
import json
import psycopg2

# Define global variables
file_exists = os.path.exists('providers.csv')
insurance_carrier = sys.argv[1]
if insurance_carrier == 'United':
    base_url = 'https://public.fhir.flex.optum.com/R4/'
    supplement = '&_count=100'
elif insurance_carrier == 'Kaiser':
    base_url = 'https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc/'
practitionerRole = 'PractitionerRole?'
city = sys.argv[2]
search_param = f'location.address-city={city}&location.address-state=CA'
supplement = ''
full_query = base_url + practitionerRole + search_param + supplement
kaiser_networks = {'Exclusive_Provider_Organization_(EPO)_CN': 'Kaiser EPO Network',
                   'HMO_CN': 'Kaiser HMO Network',
                   'Medi-Cal_Managed_Care_CN': 'Kaiser Medi-Cal Network',
                   'Point-of-Service_Plan_(POS)_CN': 'Kaiser Point-of-Service Network',
                   'Senior_Advantage_CN': 'Kaiser Senior Advantage Network'}

# Define necessary functions
def get_healthcareService_refs(provider_list):
    '''
    Function that extracts the HealthcareService resource reference from a list of
    provider entries (taken from the PractitionerRole resource)
    -----
    Input:
    
    provider_list (list) - List of json objects each referring to a single provider
    -----
    Output:
    
    healthcareService_refs (list) - List of healthcareService resource references
                                    for the corresponding providers
    '''
    healthcareService_refs = []
    for provider in provider_list:
        provider_entry = provider.get('resource')
        try:
            provider_service = provider_entry.get('healthcareService')[0]
            healthcareService_ref = provider_service.get('reference')
            healthcareService_refs.append(healthcareService_ref)
        except TypeError:
            healthcareService_refs.append('None')
    return healthcareService_refs

def extract_specialty_and_number(healthcareServiceUrls):
    '''
    Function that takes a HealthcareService API call and extracts the provider taxonomy code
    and the corresponding plain English specialty
    -----
    Input:
    
    healthcareServiceUrls (list) - List of strings corresponding to HealthcareService resource API call
    -----
    Outputs:
    
    specialties (list) - 2D list of provider taxonomy codes and corresponding plain English
                         specialty
                         
            [[taxonomy_code, specialty],
             [taxonomy_code, specialty],
                   ...          ...
             [taxonomy_code, specialty]]
    
    numbers (list) - 1D list of provider phone numbers
    '''
    specialties = []
    numbers = []
    for url in healthcareServiceUrls:
        request = requests.get(url)
        json_object = request.json()
        specialty_object = json_object.get('specialty')
        contact_object = json_object.get('telecom')[0]
        try:
            specialty_dict = specialty_object[0].get('coding')[0]
            code = specialty_dict.get('code')
            display_name = specialty_dict.get('display')
            specialty = [code, display_name]
        except TypeError: # Catch TypeError raised if no specialty listed
            specialty = [None, None]
        number = contact_object.get('value')
        specialties.append(specialty)
        numbers.append(number)
    return specialties, numbers

def extract_healthcareservice_resource(healthcareServiceUrls, carrier):
    '''
    Function that takes a HealthcareService API call and extracts the provider taxonomy code
    and the corresponding plain English specialty
    -----
    Input:
    
    healthcareServiceUrls (list) - List of strings corresponding to HealthcareService resource API call

    carrier (str)
    -----
    Output(s):
    
    **************************
    *  If carrier != United  *
    **************************
    specialties (list) - 2D list of provider taxonomy codes and corresponding plain English
                         specialty
                         
            [[taxonomy_code, specialty],
             [taxonomy_code, specialty],
                   ...          ...
             [taxonomy_code, specialty]]

    **************************
    *  If carrier == United  *
    **************************
    specialties (list) - 2D list of provider taxonomy codes and corresponding plain English
                         specialty
                         
            [[taxonomy_code, specialty],
             [taxonomy_code, specialty],
                   ...          ...
             [taxonomy_code, specialty]]

    numbers (list) - See extract_specialty_and_position
    '''
    if carrier == 'United':
        return extract_specialty_and_number(healthcareServiceUrls)        
    else:
        specialties = []
    for url in healthcareServiceUrls:
        
        request = requests.get(url)
        json_object = request.json()
        specialty_object = json_object.get('specialty')
        try:
            specialty_dict = specialty_object[0].get('coding')[0]
            code = specialty_dict.get('code')
            display_name = specialty_dict.get('display')
            specialty = [code, display_name]
        except TypeError: # Catch TypeError raised if no specialty listed
            specialty = ['Not listed', 'Not listed']
        specialties.append(specialty)
    return specialties

def extract_name_address_coordinates(location_urls):
    '''
    Function that extracts all information from Location resource API calls
    United variant
    -----
    Input:

    location_urls (list) - List of strings corresponding to Location resrouce API calls
    -----
    Outputs:
    names (list) - List of provider names as strings
    
    addresses (list) - List of street addresses as strings
        
    cities (list) - List of provider cities as strings
    
    states (list) - List of provider state as strings (postal code)
    
    zips (list) - List of provider zip codes as strings (postal code)

    coordinates (list) - 2D list of coordinates

            [[latitude, longitude],
             [latitude, longitude],
                 ...       ...
             [latitude, longitude]]
    '''
    names = []
    addresses = []
    cities = []
    states = []
    zips = []
    for url in location_urls:
        request = requests.get(url)
        json_object = request.json()
        name = json_object.get('name')
        address_object = json_object.get('address')
        coordinate_object = json_object.get('position')
        street_address = address_object.get('text')
        city = address_object.get('city')
        state = address_object.get('state')
        zip_code = address_object.get('postalCode')[:5]
        lat = coordinate_object.get('latitude')
        lon = coordinate_object.get('longitude')
        coordinate = [lat, lon]
        names.append(name)
        addresses.append(street_address)
        cities.append(city)
        states.append(state)
        zips.append(zip_code)
        coordinates.append(coordinate)
    return names, addresses, cities, states, zips, coordinates

def extract_location_resource(location_urls, carrier):
    '''
    Function that extracts all information from Location resource API calls
    Calls helper function for certain carriers (see extract_name_address)
    -----
    Inputs:
    
    location_urls (list) - List of strings corresponding to Location resource API calls

    carrier (str) - String designating insurance carrier
    -----
    Outputs:
    
    **************************
    *  If carrier != United  *
    **************************
    names (list) - List of provider names as strings
    
    addresses (list) - List of street addresses as strings
    
    numbers (list) - List of provider phone numbers as strings
    
    cities (list) - List of provider cities as strings
    
    states (list) - List of provider state as strings (postal code)
    
    zips (list) - List of provider zip codes as strings (postal code)

    **************************
    *  If carrier == United  *
    **************************
    names (list) - See extract_name_address_coordinates
    
    addresses (list) - See extract_name_address_coordinates
        
    cities (list) - See extract_name_address_coordinates
    
    states (list) - See extract_name_address_coordinates
    
    zips (list) - See extract_name_address_coordinates

    coordinates (list) - See extract_name_address_coordinates
    '''
    if carrier == 'United':
        return extract_name_address_coordinates(location_urls)
    else:
        names = []
        addresses = []
        numbers = []
        cities = []
        states = []
        zips = []
        for url in location_urls:
            request = requests.get(url)
            json_object = request.json()
            name = json_object.get('name')
            address_object = json_object.get('address')
            contact_object = json_object.get('telecom')[0]
            street_address = address_object.get('text')
            number = contact_object.get('value')
            city = address_object.get('city')
            state = address_object.get('state')
            zip_code = address_object.get('postalCode')[:5]
            names.append(name)
            addresses.append(street_address)
            numbers.append(number)
            cities.append(city)
            states.append(state)
            zips.append(zip_code)
        return names, addresses, numbers, cities, states, zips

def clean_address(address):
    '''
    Helper function that cleans street address to allow for Nominatim 
    seach engine API call
    -----
    Input:
    
    address (string) - Street address (includes whitespaces and suite, 
                       unit, or floor)
    -----
    Output:
    
    cleaned_address (string) - Street address prepared for Nominatim
                               search engine API call
    '''
    unit_pattern = 'Fl\s[\w\d]+\s|Ste\s[\w\d]+\s|Unit\s[\w\d]+\s|Unit\s[\w\d]+\s|Rm\s[\w\d]+\s'
    try:
        clean_w_spaces = re.sub(unit_pattern, '', address)
        cleaned_address = re.sub('\s', '%20', clean_w_spaces)
        return cleaned_address
    except TypeError:
        return None

def nominatim_lookup(address):
    '''
    Helper function that makes a call to Nominatim search engine API
    -----
    Input:
    
    address (string) - Street address
    -----
    Output:
    
    coordinate (list) - List of strings corresponding to coordinates ([latitude, longitude])
    '''
    nominatim_search = 'https://nominatim.openstreetmap.org/search/'
    set_json_format = '?format=json'
    cleaned_address = clean_address(address)
    try:
        nominatim_call = nominatim_search + cleaned_address + set_json_format
        try:
            search_object = requests.get(nominatim_call).json()[0]
            latitude = search_object.get('lat')
            longitude = search_object.get('lon')
            coordinate = [latitude, longitude]
            return coordinate
        except IndexError:
            print(f'Unable to find coordinates for {address}')
            return [None, None]
    except TypeError:
        return [None, None]

def get_coordinates(addresses):
    '''
    Function that uses Nominatim search engine API to extract coordinates from
    a list of street addresses
    -----
    Input:
    
    addresses (list) - List of street addresses as strings
    -----
    Output:
    
    coordinates (list) - 2D list of coordinates (latitude, longitude)
    
            [[latitude, longitude],
             [latitude, longitude],
                ...        ...
             [latitude, longitude]]
    '''
    coordinates = []
    for address in addresses:
        try:
            coordinate = nominatim_lookup(address)
        except json.decoder.JSONDecodeError: # Catch unable to find coordinates error
            coordinate = ['100.0', '200.0'] # Invalid coordinates to be entered in database
        coordinates.append(coordinate)
    return coordinates


def extract_network_name(providers_networks):
    '''
    Function that extracts the names of accepted networks given a list
    of json objects detailing the Kaiser networks that a single provider
    belongs to
    -----
    Input:
    
    providers_networks (list) - List of json objects detailing the
                                Kaiser networks that that provider
                                belongs to
    -----
    Output:
    
    translated_networks (list) - List of strings with the Kaiser networks
                                 that that provider belongs to in plain
                                 English
    '''
    translated_networks = []
    for network_object in providers_networks:
        try:
            networks_raw = [network.get('valueReference').get('identifier').get('value')
                            for network in network_object]
            networks = [kaiser_networks[raw_network] for raw_network in networks_raw]
            translated_networks.append(networks)
        except AttributeError: # Catch not accepting patients
            translated_networks.append(['Not accepting patients'])
        except KeyError: # Catch unrecognized networks
            print('Unrecognized network. Please manually review and edit network (see below)')
            print(f'{networks}')
            translated_networks.append(['Unrecognized network'])
    return translated_networks

def network_finder(network_token, df):
    '''
    Function that takes a string identifying which network and returns
    a list identifying whether each provider in df is a part of the specified
    network
    -----
    Inputs:
    
    network_token (str) - String idetifying the network to look for
    
    df (DataFrame) - Pandas Dataframe with one provider per row to sear in
    -----
    Output:
    
    in_network_list (list) - List specifying whether each provider in df is in
                             the network specified by network_token
    '''
    in_network_list = []
    providers_networks = df['Networks'].astype(str).values
    if network_token == 'Medicare':
        token = 'Senior Advantage'
    elif network_token == 'POS':
        token = 'Point-of-Service'
    else:
        token = network_token
    for network_set in providers_networks:
        if token in network_set:
            in_network = 1
        else:
            in_network = 0
        in_network_list.append(in_network)
    return in_network_list

def city_lookup(df_kaiser):
    '''
    Function that makes a Nominatum API call to look up the city
    from a longitude, latitude pair for an entire dataframe.
    ** A Nominatim object, `geolocator`, must be created and initialized! **
    -----
    Input:
    
    df_kaiser (DataFrame) - Pandas DataFrame of Kaiser providers
                            including columns 'Latitude' and 'Longitude'
    -----
    Output:
    
    cities (list) - List of cities as strings
    '''
    cities = []
    latitudes = df_kaiser['Latitude'].values
    longitudes = df_kaiser['Longitude'].values
    geolocator = Nominatim(user_agent="geoapiExercises")
    for i in range(len(latitudes)):
        lat = latitudes[i]
        lon = longitudes[i]
        try:
            location = geolocator.reverse(f'{lat},{lon}')
            address = location.raw['address']
            city = address.get('city')
        except ValueError:
            city = None
        cities.append(city)
    return cities

def extract_state_zip(address):
    '''
    Function that extracts city postal code and the zip code from
    an address, address, as a string
    -----
    Input:
    
    address (str) - Singular address as a string
    -----
    Output:
    
    state_zip (list) - Python list of length one containing state, zip
                       match
    -----
    Example:
    
    >>> address = "5601 Arnold Rd Dublin CA 94568"
    >>> extract_state_zip(address)
    ['CA 94568']
    '''
    try:
        state_zip = re.findall(r'\w{2}\s\d+$', address)
    except TypeError: # Catch error thrown when no address found in database
        state_zip = ['XX 00000']
        print('No address found in database')
    return state_zip

def add_carriers(df):
    '''
    Creates a list of insurance carrier for each listing in df
    List of insurance carriers to be included:
        Aetna
        Anthem
        Blue Shield of California
        Cigna
        Kaiser Permanente
        Oscar Health (more research needed)
        UnitedHealthcare
    -----
    Input:
    
    df (DataFrame) - Pandas DataFrame with provider listings
    -----
    Output:
    
    carriers (list) - Python list with insurance carrier for each
                      provider listing; each provider listing
                      should have only one designated carrier value
    '''
    carriers = []
    at = 'Aetna'
    an = 'Anthem'
    bs = 'Blue Shield'
    cg = 'Cigna'
    kp = 'Kaiser'
    os = 'Oscar Health'
    un = 'UnitedHealthcare'
    for provider in df['Networks'].values:
        provider_listing = str(provider)
        if at in provider_listing:
            carrier = at
        elif an in provider_listing:
            carrier = an
        elif bs in provider_listing:
            carrier = bs
        elif cg in provider_listing:
            carrier = cg
        elif kp in provider_listing:
            carrier = kp
        elif os in provider_listing:
            carrier = os
        elif un in provider_listing:
            carrier = un
        else:
            carrier = None
        carriers.append(carrier)
    return carriers

def add_accepting_patients_status(df):
    '''
    Add accepting patients status for each provider listing in df
    -----
    Input:
    
    df (DataFrame) - Pandas DataFrame with provider listings
    -----
    Output:
    
    active_status (list) - Python list with binary indicators
                           of active status for each provider
                           listing in df
    '''
    active_status = []
    for provider_listing in df['Carrier'].values:
        if provider_listing is None:
            status = 0
        else:
            status = 1
        active_status.append(status)
    return active_status

# Make API calls
request = requests.get(full_query)
json_object = request.json()
total = json_object.get('total')
entries_per_page = len(json_object.get('entry'))
total_pages = total // 50 + 1
print(f'Total Entries: {total}')
print(f'Entries per Page: {entries_per_page}')
page = 1
print(f'Working on Page {page}...')

# First page
provider_list = json_object.get('entry')
healthcareService_refs = get_healthcareService_refs(provider_list)
healthcareServiceUrls = [base_url + ref for ref in healthcareService_refs]
location_refs = [provider.get('resource').get('location')[0].get('reference')
                 for provider in provider_list]
location_urls = [base_url + ref for ref in location_refs]
network_objects = [providers_networks.get('resource').get('extension')[1:]
                   for providers_networks in json_object.get('entry')]

id_codes = [provider.get('resource').get('id') for provider in provider_list]
if insurance_carrier == 'United':
    specialties, numbers = extract_healthcareservice_resource(healthcareServiceUrls,
                                                              insurance_carrier)
    names, addresses, cities, states, zip_codes, coordinates = extract_location_resource(location_urls,
                                                                                         insurance_carrier)
else:
    specialties = extract_healthcareservice_resource(healthcareServiceUrls,
                                                     insurance_carrier)
    names, numbers, addresses, cities, states, zip_codes = extract_location_resource(location_urls,
                                                                                     insurance_carrier)
    coordinates = get_coordinates(addresses)
codes = [specialty[0] for specialty in specialties]
specialty_names = [specialty[1] for specialty in specialties]
latitudes = [coordinate[0] for coordinate in coordinates]
longitudes = [coordinate[1] for coordinate in coordinates]
networks = extract_network_name(network_objects)
last_updated = [provider.get('resource').get('meta').get('lastUpdated')
                for provider in provider_list]

# Create dataframe
kaiser_providers = pd.DataFrame(data={'id_code': id_codes,
                                      'Name': names, 
                                      'Address': addresses,
                                      'Phone Number': numbers,
                                      'Latitude': latitudes, 
                                      'Longitude': longitudes,
                                      'Provider Taxonomy Code': codes,
                                      'Specialty': specialty_names,
                                      'Networks': networks,
                                      'Last Updated': last_updated})

try:
    next_dict = json_object.get('link')[1]
    next_url = next_dict.get('url')
except IndexError: # If only one page, continue on
    pass
page += 1

# Repeat above to account for pagination
while page <= total_pages:
    next_json_object = requests.get(next_url).json()
    print(f'Working on Page {page}...')
    provider_list = next_json_object.get('entry')
    try:
        healthcareService_refs = get_healthcareService_refs(provider_list)
    except TypeError:
        page += 1
        continue
    healthcareServiceUrls = [base_url + ref for ref in healthcareService_refs]
    location_refs = [provider.get('resource').get('location')[0].get('reference')
                     for provider in provider_list]
    location_urls = [base_url + ref for ref in location_refs]
    network_objects = [providers_networks.get('resource').get('extension')[1:]
                       for providers_networks in next_json_object.get('entry')]
    
    id_codes = [provider.get('resource').get('id') for provider in provider_list]
    specialties = get_specialties(healthcareServiceUrls)
    codes = [specialty[0] for specialty in specialties]
    specialty_names = [specialty[1] for specialty in specialties]
    names, addresses, numbers = get_names_addresses_and_numbers(location_urls)
    coordinates = get_coordinates(addresses)
    latitudes = [coordinate[0] for coordinate in coordinates]
    longitudes = [coordinate[1] for coordinate in coordinates]
    networks = extract_network_name(network_objects)
    last_updated = [provider.get('resource').get('meta').get('lastUpdated')
                for provider in provider_list]

    kaiser_df = pd.DataFrame(data={'id_code': id_codes,
                                   'Name': names, 
                                   'Address': addresses,
                                   'Phone Number': numbers,
                                   'Latitude': latitudes, 
                                   'Longitude': longitudes,
                                   'Provider Taxonomy Code': codes,
                                   'Specialty': specialty_names,
                                   'Networks': networks,
                                   'Last Updated': last_updated})
    
    kaiser_providers = pd.concat([kaiser_providers, kaiser_df])

    try:
        next_dict = next_json_object.get('link')[1]
        next_url = next_dict.get('url')
    except IndexError: # Catch IndexError thrown if only one page of data exists
        pass
    page += 1
    
# Fix index
kaiser_providers = kaiser_providers.reset_index() \
                                   .drop('index', axis=1)

# Add networks
kaiser_providers['Kaiser EPO Network'] = network_finder('EPO', kaiser_providers)
kaiser_providers['Kaiser HMO Network'] = network_finder('HMO', kaiser_providers)
kaiser_providers['Kaiser Medi-Cal Network'] = network_finder('Medi-Cal', kaiser_providers)
kaiser_providers['Kaiser Point-of-Service Network'] = network_finder('POS', kaiser_providers)
kaiser_providers['Kaiser Senior Advantage Network'] = network_finder('Medicare', kaiser_providers)

# Add state and zip code
cities = city_lookup(kaiser_providers)
state_zip = [extract_state_zip(address) for address in kaiser_providers['Address'].values]
state_zip_split = [re.split(r'\s', state_w_zip[0]) for state_w_zip in state_zip]
states = [split[0] for split in state_zip_split]
zip_codes = [split[1] for split in state_zip_split]
kaiser_providers['City'] = cities
kaiser_providers['State'] = states
kaiser_providers['Zip Code'] = zip_codes

# Add carrier
kaiser_providers['Carrier'] = add_carriers(kaiser_providers)

# Add patient status
kaiser_providers['Accepting Patients'] = add_accepting_patients_status(kaiser_providers)

# Add ids
full_ids = kaiser_providers['id_code'].values
kaiser_providers.insert(loc=0,
                        column='id',
                        value=full_ids)
kaiser_providers = kaiser_providers.drop(labels=['id_code'],
                                         axis=1)

# Create csv or append to existing file and push changes to database
conn = psycopg2.connect(database="provider_directory",
                        user='postgres', 
                        password='',
                        host='localhost',
                        port='5432')
cursor = conn.cursor()

if file_exists:
    print('Writing to providers.csv')
    og_kaiser_providers = pd.read_csv('providers.csv')
    new_kaiser_providers = pd.concat([og_kaiser_providers,
                                      kaiser_providers])
    # Check for duplicates
    print('Checking for duplicates...')
    length = new_kaiser_providers.shape[0]
    updated_kaiser_providers = new_kaiser_providers.drop_duplicates(subset='id')
    updated_kaiser_providers.to_csv('providers.csv',
                                    index=False)
    updated_length = updated_kaiser_providers.shape[0]
    if length != updated_length:
        diff = length - updated_length
        print(f'{diff} duplicate(s) found')
    else:
        print('No duplicates found!')
    print('Updating providers table...')
    update_providers_table_file = open('update_providers_table.sql', 'r')
    cursor.execute(update_providers_table_file.read())
    conn.commit()
else:
    print('Creating providers.csv...')
    kaiser_providers.to_csv('providers.csv',
                            index=False)
    print('Writing to database...')
    create_providers_sql_file = open('create_providers_table.sql', 'r')
    cursor.execute(create_providers_sql_file.read())
    conn.commit()
conn.close()