-- Create a table to store golf driver data
CREATE OR REPLACE TABLE GOLF_DRIVERS (
    DRIVER_ID VARCHAR(50),  -- Hash of brand + model + condition + dexterity + loft + flex + shaft
    BRAND VARCHAR(100),
    MODEL VARCHAR(200),
    PRICE DECIMAL(10,2),
    CONDITION VARCHAR(50),
    DEXTERITY VARCHAR(20),
    LOFT VARCHAR(20),
    FLEX VARCHAR(50),
    SHAFT VARCHAR(200),
    PRODUCT_URL VARCHAR(500),
    SOURCE_URL VARCHAR(500),  -- Added field for the source website URL
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FIRST_SEEN TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (DRIVER_ID)
);

-- Test
SELECT * FROM GOLF_DRIVERS;
SELECT COUNT(*) FROM GOLF_DRIVERS;
--TRUNCATE GOLF_DRIVERS;

-- Create a network rule
CREATE OR REPLACE NETWORK RULE golf_websites_access
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('2ndswing.com', 'www.2ndswing.com');

-- Create an external access integration that uses the network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION golf_web_access
  ALLOWED_NETWORK_RULES = (golf_websites_access)
  ENABLED = true;

-- Grant usage on the integration to necessary roles
GRANT USAGE ON INTEGRATION golf_web_access TO ROLE accountadmin;

-- To add more websites later, you can alter the network rule:
/*
ALTER NETWORK RULE golf_websites_access
SET VALUE_LIST = ('2ndswing.com', 'www.2ndswing.com', 
                  'golfsite.com');
*/