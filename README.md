# Customer Segmentation: ELT Pipeline & API

## Problem Statement

The idea behind the project is to reactivate customers who left the platform and resume their order frequency by sending vouchers to customers based on specific rules and customer attributes.

The data provided in the assignment is the historical data of voucher assignments for customers. Each row is a voucher assigned to a customer.

## Requirements

### General Requirements
1. Simple but scalable solution.
2. Easy to understand and customize.
3. Easy to use.

### Technical Requirements
* Conduct data analysis to explore and prepare the data.
* Create a data pipeline to generate customer segments, including data cleaning, and optimization.
* Create a REST API that will expose the most used voucher value for a particular customer segment.


## Segments to be used
|   Segment      | Segment Name | Measure |
| ------------ | ------------------------- | ------------------------ |
| Frequent    | "1-4"                | total orders                |
| Frequent    | "5-13"                | total orders               |
| Frequent    | "14-37"              | total orders                |
| Recency     | "30-60"              | days since last order       |
| Recency     | "61-90"              | days since last order       |
| Recency     | "91-120"             | days since last order       |
| Recency     | "121-180"            | days since last order       |
| Recency     | "180+"               | days since last order       |


## Solution Details

### Data Cleaning
* The dataset was filtered and cleaned according to the following criteria:
* Only entries holding the `Peru` country code are included
* Entries with `total_orders` of 0 but with `last_order_ts` or `first_order_ts` are removed.
* Entries with `last_order_ts` that took place before `first_order_ts` are removed.
* Entries with blank `total_orders` or `voucher_amount` were discarded.

### Project Requirements (tools & related)
In order to run this project, 

* Docker needs to be installed
* Snowflake account to be created (credentials of my personal account, are shared for POC purposes)
* FastAPI will be used for the creation of the API purposes
* Snowflake connectivity will be made using the official python connector for snowflake.

### Project Composition
* This project is containerized and runs via Docker. There is one container created from the python image of Docker. 
* The Trail Snowflake account is created for the purposes of this project. 
* The database(`dh_database`) consists of 3 schemas: `dh_raw` (imported data from file), `dh_stage` (we load the rules into this for transformation), and  `dh_prod` (the last step in the process and this data will be read from the API).
* Pipeline execution can be done via python script execution in the container or using an endpoint(`runPipeline`)t for the same. 

### Creation of docker image
1. Run the command in the terminal, `docker build -t myimage .` where the Dockerfile of the project is located.
2. After successful completion of step 1, run the command  `docker run -d --name mycontainer -p 80:80 myimage`
3. After this step, the server will start and we can use the API calls.
4. Post completion of step2, run the command and `docker ps` and make note of the container_id for the next steps.
5. Use the container ID from the above step and replace it in the next command `docker exec -it <container id> /bin/bash`, this will help us to log in to the container.

### ELT Steps

1. Snowflake Setup will be done as the first step in pipeline, where we create all DB objects.
2. Using [gdown](https://pypi.org/project/gdown/) library, the file will be downloaded and saved on disk locally.
3. The data from the file is loaded into RAW schema table without any changes. To maintain untouched data.
4. Using the Raw data and segment rules, segments will be calculated and stored in PROD schema for consumption of API.

### API Endpoint Details

The FAST API library is connected with snowflake and provides us with 2 endpoints. One for ELT Pipeline and the other for Voucher Selection

* `/runPipeline/` - This endpoint will run the pipeline for data processing
* `/getVoucher/` - This endpoint will get the Voucher data for the input request(curl/ Python requests) and returns a JSON object

In order to populate the database with the output, run the `runPipeline` API before interacting with the `getVoucher` API.

### Sending a Request to the API

As we used FastAPI, there is additional interactive documentation provided. 
You can go to http://192.168.99.100/docs or http://127.0.0.1/docs (or equivalent, using your Docker host).

#### runPipeline Example

```
import requests

url = 'http://127.0.0.1/runPipeline/'
myobj = '' // we don't need any inputs for pipeline

response = requests.post(url, json=myobj)

print(response.text)
```

Or an alternative would be to send a curl request:

```
curl -X 'POST' \
  'http://127.0.0.1/runPipeline/' \
  -H 'accept: application/json' \
  -d ''
```

#### getVoucher Example

```
import requests

url = 'http://127.0.0.1/getVoucher/'
myobj = 
  {
    "customer_id": 10, // customer id
    "total_orders": 20, // total orders placed by a customer
  "country_code": "Peru",  // customer’s country
  "last_order_ts": "2020-08-29 12:34:56",  // ts of the last order placed by a customer
  "first_order_ts": "2019-08-29 12:34:56", // ts of the first order placed by a customer
  "segment_name": "recency_segment" // which segment a customer belongs to
  }

response = requests.post(url, json=myobj)

print(response.text)
```

Or an alternative would be to send a curl request:

```curl -X 'POST' \
  'http://127.0.0.1/getVoucher/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "customer_id": 10,
  "total_orders": 20,
  "country_code": "Peru",
  "last_order_ts": "2020-08-29 12:34:56",
  "first_order_ts": "2019-08-29 12:34:56",
  "segment_name": "recency_segment"
}'
```

### Checking in Database
1. Login into snowflake using https://du59886.ap-south-1.aws.snowflakecomputing.com/console/login#/
2. Use the Credentials username: `saianil58` and password: `Use the shared password`
3. Please use the warehouse `sai_wh`
4. To check the raw data, run `select * from dh_database.dh_raw.old_voucher_data;`
5. To check the Segements rules loaded, run `select * from dh_database.dh_stage.RULES_FOR_SEGMENTS;`
6. To check the prod data, where segments are calculated, run `select * from dh_database.dh_prod.SEGMENTS_DERIVED;`

## Tests: Simple ones developed
* This project uses the pytest framework for unit-testing. 
* The `tests` directory contains only a simple test as this is a POC.
* The first test will check if we can connect to a dummy URL. The expected output of this is a failure.
* The second test will make sure the connection to snowflake is established and closed. The expected output of this is Pass.

In order to do so you would need to exec into the container, into the tests directory and execute below:

```
# run the tests
python -m pytest -v
```

## Remarks & Scaling Thoughts
1. All the credentials and the ways they are handled in this project are meant to be simple on purpose.
2. There are no security measures taken, usually, we can use with AWS Parameter store.
3. In real-world situations, we can use better tools for SQL execution like [DBT](https://www.getdbt.com/).
4. We can also use the orchestration tools like Airflow.
5. There can be more logging details for different steps in the pipeline.

## Conclusion
Analysis of the data revealed that the amount of vouchers is the same for most of the customers(2640). 
After segmenting the customers the same result was for both recency and frequency.