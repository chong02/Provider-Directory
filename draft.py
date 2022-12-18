from fhirclient import client

settings = {'api_base': 'https://fhir-open-api-dstu2.smarthealthit.org'}
smart = client.FHIRClient(settings=settings)

import fhirclient.models.location as l
#location = l.Location.read(, smart.server)