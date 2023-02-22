# You need this to use FastAPI, work with statuses and be able to end HTTPExceptions
from fastapi import FastAPI, status, HTTPException, Request, responses

# You need this to be able to turn classes into JSONs and return
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# Needed for json.dumps
import json

# Both used for BaseModel
from pydantic import BaseModel
from typing import Optional, List

from decimal import Decimal

from datetime import datetime
from kafka import KafkaProducer, producer

import sys
sys.path.append('..')

# custom module to connect and query PostgreSQL database
import postgresql.postgres_connection, postgresql.tables
import psycopg2

# custom Python script to compute optimized route
import app.optiway as optiway

#import requests
from starlette.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND,HTTP_303_SEE_OTHER

# Create class (schema) for the JSON which will contain user information
class User(BaseModel):
    nom: str
    prenom: str
    email: str
    numero: int
    rep: str
    adresse: str
    code_postal: int
    ville: str
    a_livrer: str

    def __init__(self, *args):
        
        # Get a "list" of field names (or key view)
        field_names = self.__fields__.keys()
        
        # Combine the field names and args to a dict
        # using the positions.
        kwargs = dict(zip(field_names, args))
        
        super().__init__(**kwargs)

# class used to partially update user fields
# fields marked as optional in order to be able to do so
class UserUpdate(BaseModel):
    nom: Optional[str] = None
    prenom: Optional[str] = None
    email: str
    numero: Optional[int] = None
    rep: Optional[str] = None
    adresse: Optional[str] = None
    code_postal: Optional[int] = None
    ville: Optional[str] = None
    a_livrer: Optional[str] = None

class UsersUpdateList(BaseModel):
    __root__: List[UserUpdate]
    

# Class exception EmptyQueryResultException
class EmptyQueryResultException(Exception):
    "Raised when the Postgres query returns an empty result"
    pass

def produce_kafka_string(json_as_string, topic):
    # Create producer
    producer = KafkaProducer(bootstrap_servers='kafka:9092',acks=1)

    # Write the string as bytes because Kafka needs it this way
    producer.send(topic, bytes(json_as_string, 'utf-8'))
    producer.flush()

# This is important for general execution and the docker container
app = FastAPI()

# Base URL
@app.get("/")
async def root_test():
    return {"message": "Hello World"}

# Get a user by email
@app.get("/users")
async def get_user(email: str):
    conn = postgresql.postgres_connection.connect()
    query_result = postgresql.tables.select_from_table(
        conn=conn,
        columns="*",
        table="users",
        query="""where (email='{}')""".format(email))
    
    postgresql.postgres_connection.close_connection(conn=conn)
    
    try:
        # for empty qurery results
        if query_result == []:
            raise EmptyQueryResultException
        else:
            #filter None values (empty columns)
            query_res_filtered = ["" if item==None else item for item in query_result[0]]
            
            my_user=User(*query_res_filtered)

            # encode into JSON
            json_compatible_user_data = jsonable_encoder(my_user)
            return JSONResponse(content=json_compatible_user_data)

    except ValueError:
        return JSONResponse(content=query_result, status_code=400)

# Add new user
@app.post("/users")
async def create_new_user(item: User): #body awaits a json with invoice item information
    #TO DO : modify to process a list of users as input

    try:
        # We parse the item as a json
        json_of_item = jsonable_encoder(item)

        # We then dump the json out as string
        json_as_string = json.dumps(json_of_item)
        print(json_as_string)

        # we produce the string to kafka
        produce_kafka_string(json_as_string,'add-user-topic')

        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content="New user {} added".format(json_as_string), status_code=201)
    
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)

# Update users
@app.patch("/update_users")
async def update_user(my_user_update_list: UsersUpdateList):
    try:
        conn = postgresql.postgres_connection.connect()
        for user in my_user_update_list.__root__:
            # update only the fields which are set
            # exclude None values, which correspond to unset fields
            # also exclude the PK email (not supposed to be modified)
            email_list =[]
            for key in user.dict(exclude_none=True,exclude={'email'}):
                email_list.append(user.email)
                postgresql.tables.update_table(
                    conn=conn,
                    col_val="""{} = '{}'""".format(key,user.dict(exclude_none=True)[key]),
                    table="users",
                    query="""where (email='{}')""".format(user.email))
                    
        postgresql.postgres_connection.close_connection(conn=conn)

        return JSONResponse(content='Users {} have been updated'.format(str(email_list)), status_code=201)

    except ValueError:
        return JSONResponse(content=my_user_update_list, status_code=400)

# Get the optimal way for the delivery of the concerned users
@app.get("/route_planner")
async def find_optimal_way():
    try:
        #load the adresses to deliver from the PostgreSQL view delivery_adresses
        conn = postgresql.postgres_connection.connect()
        query_result = postgresql.tables.select_from_table(
            conn=conn,
            table="delivery_adresses")
        
        postgresql.postgres_connection.close_connection(conn=conn)

        # for empty qurery results
        if query_result == []:
            raise EmptyQueryResultException
        else:

            # process the adress list to compute the optimized delivery route
            my_query = optiway.give_me_my_deliver_route(query_result)

            # we parse the query to create a list made of the users email
            # since these users have been taking into account for delivery, we can update the users table and set a_livrer = 'non'
            my_users_to_update_list = [str(UserUpdate(email=t[0]).dict(exclude_unset=True)) for t in my_query]

            ######################
            # TO DO : redirect the result query my_query to the API patch/partial_update_user
            # first trials not working (error 405 METHOD NOT ALLOWED , ...)
            ######################

            ######################
            # for now, no redirection to patch API
            # here, the email list is sent directly to kafka, using the topic "get-delivery-adresses-topic"
            for user in my_users_to_update_list:
                produce_kafka_string(user,'get-delivery-adresses-topic')
            
            # load the table tables_to_update, which contains the name of temp tables for users to update
            # this way, rows in tables_to_update (created through Spark job) can be deleted as soon as users table is updated
            conn = postgresql.postgres_connection.connect()
            query_result = postgresql.tables.select_from_table(
                conn=conn,
                columns="name",
                table="tables_to_update"
            )
            postgresql.postgres_connection.close_connection(conn=conn)

            return JSONResponse(content=my_users_to_update_list, status_code=201)

    except ValueError:
        return JSONResponse(content=my_users_to_update_list, status_code=400)