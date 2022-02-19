from FileOperations.FileOperations import FileOperations



class Test:
    
    def __init__(self, DataBaseName, CollectionName):
        self. __filename = r"C:\Users\pavankumar.kosaraju\Desktop\DataSets\CarbonNanotubes\carbon_nanotubes.csv"
        self.fileOperations = FileOperations(self.__filename, DataBaseName, CollectionName);
        
    def insert(self):
        self.fileOperations.insert_BulkRecords();
        
    def update(self):
        self.fileOperations.update_bulkRecords(2,10);

    def find(self):
        self.fileOperations.find_many_records(4);
        
    def filter(self):
        self.fileOperations.filter_records(4);
        
    def delete(self):
        self.fileOperations.delete_many_records(2);


test = Test(DataBaseName="Accenture", CollectionName  = "CarbonNanotubes");
test.insert();
test.update()
test.find();
test.filter();
test.delete();