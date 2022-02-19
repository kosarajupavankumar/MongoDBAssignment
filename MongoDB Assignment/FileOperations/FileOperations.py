
from asyncio.log import logger
from msilib.schema import File

from numpy import append
from Logger.FileLogging import FileLogger;
from DBOperations.DBOperations import Databaseoperations

class FileOperations:
     
    def __init__(self, fileName,DataBaseName, CollectionName):
         self.fileName = fileName;
         self.logger = FileLogger();
         self.DB = Databaseoperations(DatabaseName=DataBaseName,CollectionName=CollectionName);
         self.final_dataset = "";
         logger.info("File Operations Class Initialized");
         
         
    def FileValidation(self):
        try:
            import os
            if os.path.isfile(self.fileName):
                logger.info("File is present in the Directory");
                return True;
            else:
                logger.error("File Validation Failed");
                return False;
        except Exception as e:
            logger.error("Error in File Validation {}".format(e));
            return False;
        
    def insert_BulkRecords(self):
        try:
            Filepresent = self.FileValidation();
            if(Filepresent):
                import pandas as pd   
                df = pd.read_csv(self.fileName, sep=';');
                columns = df.columns.tolist();
                for i in range(2, len(columns)):
                    if(i !=7):
                        df[columns[i]] = df[columns[i]].apply(lambda x: x.replace("0,",''))
                    else :
                        df[columns[i]] = df[columns[i]].apply(lambda x: x.replace("0,0",''))
                        df[columns[i]] = df[columns[i]].apply(lambda x: x.replace("0,",''))
                
                df.to_csv("final_dataset.csv", index=False);  
                logger.info("File EDA Successful and converted to CSV file");
                
                self.final_dataset = r"C:\Users\pavankumar.kosaraju\Desktop\MongoDB Assignment\final_dataset.csv";
                records_collection =self.read_csv_Convert_Dict();
                logger.info("Checking the collection Present in the DB");
                notcreated = self.DB.checkExistence_DB()
                if(not notcreated):
                    logger.info("checking the collection present in the DB");
                    notcreated = self.DB.checkExistence_COL();
                    if(not notcreated):
                        logger.info("inserting the records in the DB");
                        self.DB.insert_many(records_collection);
        except Exception as e:
            logger.error("Error in EDA {}".format(e));
            return False;
        
    
    def read_csv_Convert_Dict(self):
        try:
            id=0;
            import csv;
            with open(self.final_dataset, 'r') as csvfile:
                reader = csv.DictReader(csvfile);
                records=[]
                for row in reader:
                    id = id+1;
                    row['_id'] = id;
                    records.append(row);
                logger.info("Records converted successfully");
            return records;
        except Exception as e:
            logger.error("Error in converting csv file to dictionary {}".format(e));
        
    def update_bulkRecords(self, id, value):
        try:
            self.logger.info("starting the update flow");
            self.DB.update_many(id, value);
        except Exception as e:
            self.logger.error("Error in updating the records {}".format(e));
            
    def delete_many_records(self, id):
        try:
            self.logger.info("starting the delete flow");            
            self.DB.delete_many(id);
        except Exception as e:
            self.logger.error("Error in deleting the records {}".format(e));
            
    def find_many_records(self, id):
        try:
            self.logger.info("starting the find flow");
            many_records = self.DB.find_many(id);
            for id in enumerate(many_records):
                self.logger.info("Bulk records found --> {}".format(id));
        except Exception as e:
            self.logger.error("Error in finding the records {}".format(e));
                
    def filter_records(self,id):
        try:
            self.logger.info("starting the filter flow");
            filter_records =self.DB.filter_records(id);
            for record in filter_records:
                self.logger.info("Filtered records found --> {}".format(record));
            self.logger.info("Filter records successful");
        except Exception as e:
            self.logger.error("Error in filtering the records {}".format(e));
        