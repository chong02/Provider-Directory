-- Create to_add table
CREATE TEMP TABLE to_add (
    id varchar(50) NOT NULL PRIMARY KEY,
    name varchar(100) NOT NULL,
    address varchar(100),
    number varchar(14) NOT NULL,
    latitude real,
    longitude real,
    code varchar(10),
    specialty varchar(1000),
    networks varchar(1000),
    lastUpdated date,
    kaiserEPO boolean,
    kaiserHMO boolean,
    kaiserMediCal boolean,
    kaiserPOS boolean,
    kaiserMediAdv boolean,
    city varchar(50),
    state varchar(2),
    zip varchar(5),
    carrier carriers,
    accepting boolean
);

-- Copy data from providers.csv
COPY to_add (
    id,
    name,
    address,
    number,
    latitude,
    longitude,
    code,
    specialty,
    networks,
    lastUpdated,
    kaiserEPO,
    kaiserHMO,
    kaiserMediCal,
    kaiserPOS,
    kaiserMediAdv,
    city,
    state,
    zip,
    carrier,
    accepting
)
FROM '/Users/chong02/Desktop/Provider-Directory/providers.csv'
CSV HEADER;

-- Update providers with new information
UPDATE providers
SET id = to_add.id,
	name = to_add.name,
	address = to_add.address,
	number = to_add.number,
	latitude = to_add.latitude,
	longitude = to_add.longitude,
	code = to_add.code,
	specialty = to_add.specialty,
	networks = to_add.networks,
	lastUpdated = to_add.lastUpdated,
	kaiserEPO = to_add.kaiserEPO,
	kaiserHMO = to_add.kaiserHMO,
	kaiserMediCal = to_add.kaiserMediCal,
	kaiserPOS = to_add.kaiserPOS,
	kaiserMediAdv = to_add.kaiserMediAdv,
	city = to_add.city,
	state = to_add.state,
	zip = to_add.zip,
	carrier = to_add.carrier,
	accepting = to_add.accepting
FROM to_add
WHERE providers.id = to_add.id;

-- Create temporary table with all new providers
CREATE TABLE updated_providers AS (
    SELECT to_add.*
    FROM providers
    RIGHT JOIN to_add
    ON providers.id = to_add.id
);

-- Drop toy_providers and rename updated_toy_providers
DROP TABLE providers;
ALTER TABLE updated_providers RENAME TO providers;