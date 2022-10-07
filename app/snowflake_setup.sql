/*
This file contains all the sql statments 
for the snowflake setup, including the rules
*/

create  database if not exists DH_DATABASE;

create  schema if not exists DH_RAW;

create  schema if not exists dh_stage;

create  schema if not exists dh_prod;

/*
Table to insert the data from the input file
*/
create  table if not exists DH_DATABASE.DH_RAW.OLD_VOUCHER_DATA
(
timestamp timestamp,
    country_code text,
    last_order_ts timestamp,
    first_order_ts timestamp,
    total_orders text,
    voucher_amount text
    
);

/*
table to store the details of the segements
*/
CREATE OR REPLACE TABLE DH_DATABASE.dh_stage.rules_for_segments
(
    segment_type VARCHAR(30),
    segment_name VARCHAR(30),
    lower_value  INT,
    upper_value  INT
);

/*
table to create the rules for each segement
*/
INSERT INTO DH_DATABASE.dh_stage.rules_for_segments VALUES   ('ferquency_segment', '1-4', 1, 4),
                                            ('ferquency_segment', '5-13',5, 13),
                                            ('ferquency_segment', '14-37',14, 37),
                                            ('recency_segment', '30-60',30, 60),
                                            ('recency_segment', '61-90',61, 90),
                                            ('recency_segment', '91-120',91, 120),
                                            ('recency_segment', '121-180',121, 180),
                                            ('recency_segment', '180+',180, null);
