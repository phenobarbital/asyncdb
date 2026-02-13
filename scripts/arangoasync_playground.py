
import asyncio
from arangoasync import ArangoClient
from arangoasync.auth import Auth

async def main():
    print("Connecting to ArangoDB...")
    hosts = "http://localhost:8529"
    username = "root"
    password = "12345678" 
    
    client = ArangoClient(hosts=hosts)
    auth = Auth(username=username, password=password)
    
    try:
        print(f"Client created: {client}")
        
        sys_db = await client.db("_system", auth=auth)
        print(f"Connected to _system db: {sys_db.name}")
        
        version = await sys_db.version()
        print(f"ArangoDB version: {version}")
        
        test_db_name = "test_arangoasync_db"
        if await sys_db.has_database(test_db_name):
            print(f"Database {test_db_name} exists, dropping...")
            await sys_db.delete_database(test_db_name)
            
        print(f"Creating database {test_db_name}...")
        await sys_db.create_database(test_db_name)
        
        db = await client.db(test_db_name, auth=auth)
        print(f"Connected to {test_db_name}")
        
        col_name = "users"
        if await db.has_collection(col_name):
            await db.delete_collection(col_name)
            
        print(f"Creating collection {col_name}...")
        await db.create_collection(col_name)
        col = db.collection(col_name)
        
        doc = {"name": "Alice", "age": 30}
        print(f"Inserting document: {doc}")
        meta = await col.insert(doc)
        print(f"Inserted: {meta}")
        
        key = meta["_key"]
        retrieved = await col.get(key)
        print(f"Retrieved: {retrieved}")
        
        aql = f"FOR u IN {col_name} RETURN u"
        print(f"Running AQL: {aql}")
        cursor = await db.aql.execute(aql)
        async for doc in cursor:
            print(f"Query Result: {doc}")
            
        print("Cleaning up...")
        await sys_db.delete_database(test_db_name)
        print("Done.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
