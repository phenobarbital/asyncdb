import pymongo
import sys

##Create a MongoDB client, open a connection to Amazon DocumentDB as a replica set and specify the read preference as secondary preferred
client = pymongo.MongoClient('mongodb://troc:trocdocudb123456@troc-docdb-tf.cluster-cp0qwiwsxfog.us-east-2.docdb.amazonaws.com:27017/navigator?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false&tls=true&tlsCAFile=/home/jesuslara/proyectos/navigator/asyncdb/env/global-bundle.pem')

##Specify the database to be used
db = client.sample_database

##Specify the collection to be used
col = db.sample_collection

##Insert a single document
col.insert_one({'hello':'Amazon DocumentDB'})

##Find the document that was previously written
x = col.find_one({'hello':'Amazon DocumentDB'})

##Print the result to the screen
print(x)

##Close the connection
client.close()
