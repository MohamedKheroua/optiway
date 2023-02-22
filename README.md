# optiway
Route optimization app

This project aims to create a route optimization app, which computes the shortest route between a set of adresses.
Similar tools are used by companies, in order to optimize their daily deliveries.

The data are stored in a local PostgreSQL database:
- a table adresses, created from a sample of open data accessible at [Adresse Data Gouv Repo](https://adresse.data.gouv.fr/data/)
- a user table, which contains generic data generated randomly with online tools such as [Random Name Generator](https://fossbytes.com/tools/random-name-generator)

The app is built with FastAPI. It interacts with :
- Kafka to be able to handle data, as in operational context, the data would be streamed 
- Spark to process the data

Docker-compose is used here to run this whole data processing tool.

To simulate a delivery route computation :
1. we should have a list of users to deliver (field "a_livrer" equals "oui"). If needed, the PATCH request "update_users" can be used to set the field "a_livrer" to "oui" for the chosen users
2. the GET request "route_planner" is used to compute our delivery route
    * the list of delivery adresses to process is loaded
    * the delivery route is computed
    * the list of delivered users is sent to Kafka
    * Spark processes the data from Kafka, in order to update the delivered users

The app is a functional POC but there are still ongoing updates !

Please feel free to contact me if you have any questions : [Contact me](mailto:mohamedrkheroua@gmail.com)