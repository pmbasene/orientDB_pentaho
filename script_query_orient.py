#!/home/insyco/env-dev-insyco/bin/python
from pprint import pprint
import pyorient
from pyorient import exceptions

# client ==== cursor

class Orientdb():

    def __init__(self):
        self.username= 'root'
        self.password= 'root'
        self.hostname='localhost'
        self.client = pyorient.OrientDB(self.hostname, 2424)
        self.session_id = self.client.connect(self.username, self.password)

    def __repr__(self):
        # return self.session_id
        print(f"Vous etes Connecte avec la session_id {self.session_id} !!!")

    def close_connection(self):
        print('Connexion closed')
        return self.client.db_close()

    
    # ------------------ decorateurs -------------------------------------#
    # # definir decorateur qui verifie si une base de donnee est creee ou pas !
    
    def checkIfDB_exists(func):
        def wrapper(self,*args, **kwargs):
            if self.client.db_exists(*args, pyorient.STORAGE_TYPE_MEMORY ):
                return func(self, *args, **kwargs)
        return wrapper

    def checkIfDB_opens(func):
        def wrapper(self,*args, **kwargs):
            self.client.db_open(*args, self.username, self.password)
            return func(self, *args, **kwargs)
        return wrapper

    def listdb(self):
        return self.client.db_list()

    def createdb(self, databaseName):
        """ retturn None"""
        try:
            # print(f'creation de la base donnee: {databaseName}')
            return self.client.db_create(databaseName, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )
        except exceptions.PyOrientDatabaseException:
            print(f"<{databaseName}> a deja ete cree ! ")
      
    @checkIfDB_exists
    def dropdb(self, databaseName):
        self.client.db_drop(databaseName) # if self.client.db_exists( databaseName, pyorient.STORAGE_TYPE_MEMORY ):
        print(f'<{databaseName}> supprimee')

    # def _openAdb(self, databaseName):    
    #     return self.client.db_open(databaseName, self.username, self.password)

    @checkIfDB_opens
    def createClass(self, tableName):
        # assert databaseName exists before
        # class === table
        try:
            # self._open×Adb(databaseName)    # self.client.db_open( databaseName, self.username, self.password)
            cluster_id = self.client.command( f"create class {tableName} extends V")
            print(f"table: <{tableName}> est creee !")
            print(cluster_id)
            return cluster_id
        except exceptions.PyOrientSchemaException:
            print(f'Class <{tableName}> already exists')



    #  def createClass(self, tableName, databaseName):
    #     # assert databaseName exists before
    #     # class === table
    #     try:
    #         self._openAdb(databaseName)    # self.client.db_open( databaseName, self.username, self.password)
    #         cluster_id = self.client.command( f"create class {tableName} extends V")
    #         print(f"table: <{tableName}> est creee !")
    #         print(cluster_id)
    #         return cluster_id
    #     except exceptions.PyOrientSchemaException:
    #         print(f'Class <{tableName}> already exists')
    
    def createProprety(self, tableName, propreties, types, databaseName):
        try:
            self._openAdb(databaseName)
            cluster_id = self.client.command(f"create property {tableName}.{propreties} {types}" )
            print(f"creation d'une nouvelle prop: {propreties}")
            return cluster_id
        except exceptions.PyOrientCommandException:
            print(f'Property <{propreties}> already exists')
    


    def insertData(self, tableName, databaseName):
        self._openAdb(databaseName)      
        self.client.command(f"insert into {tableName} ('accommodation', 'work', 'holiday') values('BB', 'garage', 'mountain')")
        # self.client.command(f"insert into {tableName}  CONTENT {"accommodation":"BB", "work":"garage", "holiday": "mountain"} ")
        print('ajoute')



# connexion
# client = pyorient.OrientDB("localhost", 2424)
# session_id = client.connect("root", "root")
o = Orientdb()

print(o.listdb())

# main 
res = o.createdb('db_name')


# res2 = o.dropdb('db_name')
# print(res2)


# create class
res3 = o.createClass('employe', 'db_name' )

## create operty
# o.createProprety('employe','accommodation', 'String', 'db_name')

# o.insertData('employe','db_name')


o.close_connection()
