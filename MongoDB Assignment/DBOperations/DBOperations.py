# create the class for the Database Operations for MongoDB



from asyncio.log import logger
from Logger.FileLogging import FileLogger;
import pymongo;

class Databaseoperations:
    def __init__(self, DatabaseName, CollectionName):
        self.DatabaseName = DatabaseName
        self.collectionName = CollectionName
        self.logger = FileLogger()
        self.__Connection_URL = "mongodb+srv://root:root@cluster0.qkv4r.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        self.client = pymongo.MongoClient(self.__Connection_URL)
        self.logger.info("Database Operations Class Initialized")
    
      
            
            
    def checkExistence_DB(self):
        """It verifies the existence of DB"""
        DBlist = self.client.list_database_names()
        if self.DatabaseName in DBlist:
            self.logger.info(f"DB: '{self.DatabaseName}' exists")
            return True
        self.logger.info(f"DB: '{self.DatabaseName}' not yet present OR no collection is present in the DB")
        return False
    
    def checkExistence_COL(self):
        """It verifies the existence of collection name in a database"""
        collection_list = self.client[self.DatabaseName].list_collection_names()
        
        if self.collectionName in collection_list:
            print(f"Collection:'{self.collectionName}' in Database:'{self.DatabaseName}' exists")
            return True
        
        self.logger.info(f"Collection:'{self.collectionName}' in Database:'{self.DatabaseName}' does not exists OR \n\
        no documents are present in the collection")
        return False
    
    def insert_many(self, collection_of_records):
        try:
            self.client[self.DatabaseName][self.collectionName].insert_many(collection_of_records)
            self.logger.info("bulk Records inserted successfully")
        except Exception as e:
            self.logger.error("Error in inserting the records in the DB {}".format(e))
            
            
    def update_many(self, id, value):
        try:
            self.client[self.DatabaseName][self.collectionName].update_many({"Chiral indice n": str(id)}, {"$set": {"Chiral indice n": str(value)}})
            self.logger.info("bulk Records updated successfully")
        except Exception as e:
            self.logger.error("Error in updating the records in the DB {}".format(e))
            
    def delete_many(self, id):
        try:
            self.client[self.DatabaseName][self.collectionName].delete_many({"Chiral indice n": str(id)})
            self.logger.info("bulk Records deleted successfully")
        except Exception as e:
            self.logger.error("Error in deleting the records in the DB {}".format(e))
            
    def find_many(self,id):
        try:
            collection_of_records =self.client[self.DatabaseName][self.collectionName].find({"Chiral indice n": str(id)})
            self.logger.info("bulk Records found successfully")
            return collection_of_records
        except Exception as e:
            self.logger.info("Error in finding the records in the DB {}".format(e))
            
    def filter_records(self,id):
        try:
            filter_records = self.client[self.DatabaseName][self.collectionName].find({"$and":[
                {'Chiral indice n': str(id)},{'Initial atomic coordinate v': {'$ne': "45"}}
            ]})
            self.logger.info("bulk Records filtered successfully")
            return filter_records
        except Exception as e:
            self.logger.info("Error in filtering the records in the DB {}".format(e))
            
            
            