-- Update toy_providers to include updated providers (new providers not included)
UPDATE toy_providers
SET id = toy_to_add.id,
	name = toy_to_add.name,
	address = toy_to_add.address,
	number = toy_to_add.number,
	latitude = toy_to_add.latitude,
	longitude = toy_to_add.longitude,
	code = toy_to_add.code,
	specialty = toy_to_add.specialty,
	networks = toy_to_add.networks,
	lastUpdated = toy_to_add.lastUpdated,
	kaiserEPO = toy_to_add.kaiserEPO,
	kaiserHMO = toy_to_add.kaiserHMO,
	kaiserMediCal = toy_to_add.kaiserMediCal,
	kaiserPOS = toy_to_add.kaiserPOS,
	kaiserMediAdv = toy_to_add.kaiserMediAdv,
	city = toy_to_add.city,
	state = toy_to_add.state,
	zip = toy_to_add.zip,
	carrier = toy_to_add.carrier,
	accepting = toy_to_add.accepting
FROM toy_to_add
WHERE toy_providers.id = toy_to_add.id;

-- Create temporary table with all new providers
CREATE TABLE updated_toy_providers AS (
    SELECT toy_to_add.*
    FROM toy_providers
    RIGHT JOIN toy_to_add
    ON toy_providers.id = toy_to_add.id
);

-- Drop toy_providers and rename updated_toy_providers
DROP TABLE toy_providers;
ALTER TABLE updated_toy_providers RENAME TO toy_providers;