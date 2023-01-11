-- Create enum type for providers table
CREATE TYPE carriers AS ENUM(
    'Aetna',
    'Anthem',
    'Bue Shield',
    'Cigna',
    'Kaiser',
    'Oscar',
    'United',
    'Unknown/Other'
);

-- Create providers table
CREATE TABLE providers (
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

-- Copy data from providers.csv file
COPY providers (
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