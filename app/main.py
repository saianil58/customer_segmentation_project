from typing import Union
from datetime import datetime
from fastapi import FastAPI, Request
from pydantic import BaseModel
import logging
from utils import get_snowflake_conn_cursor, close_snowflake_conection
from pipeline import run_pipeline

logging.basicConfig(level=logging.INFO)

# create a custom class to handle the request
class Customer(BaseModel):
    customer_id: int
    total_orders: int
    country_code: str
    last_order_ts: str
    first_order_ts: str
    segment_name: str


# declare the fast api app library
app = FastAPI()

# endpoint to run the pipeline
@app.post("/runPipeline/")
def run_pipe_from_api():
    try:
        run_pipeline()
        return (
                {
                    "pipeline_status": "Success"
                },
                200,
            )
    except:
        return (
            {
                "pipeline_status": "Failure"
            },
            400,
        )

# getVoucher api call mehod, takes input of customer
# runs the Voucher selection sql query
# process the request and share the response
@app.post("/getVoucher/")
async def create_item(customer: Customer):

    # read the request data as a dictionary
    request_data = customer.dict()

    # the query to bring voucher_amount from the table
    query = """
                SELECT voucher_amount d
                FROM DH_DATABASE.DH_PROD.SEGMENTS_DERIVED 
                WHERE 1=1 
                AND segment_type='{segment_name}' 
                AND lower_value <= {dimension} 
                AND (upper_value >= {dimension} OR upper_value IS NULL) 
                ;
            """

    # Find the segment name from the request data
    segment_name = request_data.get("segment_name")

    # if the segment is recency_segment, process the query accordingly
    if segment_name == "recency_segment":

        # find the last order from the customer
        last_order_ts = request_data.get("last_order_ts")

        # assumption: today is the 15th of June 2020
        datediff = abs(
            (
                datetime.strptime((last_order_ts), "%Y-%m-%d %H:%M:%S")
                - datetime.strptime(("2020-06-15 00:00:00"), "%Y-%m-%d %H:%M:%S")
            ).days
        )

        # Prepare the query for the exection
        query = query.format(segment_name=segment_name, dimension=datediff)

    # If the segment is ferquency_segment, process the query accordingly
    elif segment_name == "ferquency_segment":

        # Get the total orders from request
        total_orders = request_data.get("total_orders")

        # prepare the query for exection
        query = query.format(segment_name=segment_name, dimension=total_orders)

    # get the snowflake connection and fetch the cusor object
    ret_val = get_snowflake_conn_cursor()
    cur = ret_val[0]

    # The app should only work for Peru, validate that
    country_code = request_data.get("country_code")
    if not country_code.lower() == "peru":
        return (
            {
                "invalid_request": "This API only works for Peru, Please check the Request"
            },
            400,
        )

    try:
        # execute the query and fetch the results
        data = cur.execute(query).fetchall()

        # close the connection
        close_snowflake_conection(ret_val)

        # return according to requirements
        return {"voucher_amount": data[0][0]}
    except:
        return (
            {
                "invalid_request": "request format is incorrect, ensure request format adheres to documentation"
            },
            400,
        )
