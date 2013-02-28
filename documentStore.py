import pymongo
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint

class Document:
    """
    Document
    """
    
    def __init__(self, doc, uri, language, user, groups):
        self.__json = doc
        self.uri = uri
        self.__metadata = dict(createdate=datetime.utcnow(), creator=user, active=True, uri=self.uri, language=language, groups=groups)
        
    @property
    def document(self):
        return self.__json
        
    @property
    def metadata(self):
        return self.__metadata
        
class User:
    """
    User object
    """
    
    def __init__(self, user):
        if type(user) == str:
            self.__username = user
            self.__groups = list()
        else:
            self.__username = user["username"]
            self.__groups = user["groups"]
    
    @property
    def username(self):
        return self.__username
        
    @property
    def groups(self):
        return self.__groups
        
    @property
    def document(self):
        return dict(groups=self.__groups, username=self.__username)
        
    def addGroup(self, group, permissions):
        current = {"groupname":group, "permissions":permissions}
        self.__groups.append(current)
    
class DocumentStore:
    """
    Class that handles all communication with the MongoDB
    It handles things like versioning, security and multiligualism
    The "CMS"
    """
    
    def __init__(self, host, port):
        connection = MongoClient(host,port )
        self.db = connection.test_database
        self.collection = self.db.cms.nodes
        self.ctxCollection = self.db.csm.context
        
    def getAllByURI(self, uri, language):
        results = self.collection.find({"metadata.uri":uri}).sort('metadata.version', pymongo.ASCENDING)
        l = list(results)
        return l
        
    def getLastByURI(self, uri, language):
        result = self.collection.find_one({"metadata.uri":uri, "metadata.active":True})
        return result
    
    def showAll(self):
        cur = self.collection.find()
        for item in cur:
            pprint(item)
        
    def getBySearch(self, username, search=None, language=None):
        #We are only interested in active data
        searchString = [{"metadata.active":True}]
        
        #Making sure the result contains only security groups where user has read access too
        roles = self.getRoles(username, 'r')
        if len(roles) == 0:
            roles.append('public')
        roleSearch = {"metadata.groups": {'$in': roles}}
        searchString.append(roleSearch)
        
        #If a language is specified we only want results in that language
        if language != None:
            searchString.append({"metadata.language":language})
        #Adding the userspecified search
        if search != None:
            searchString.append(search)
        
        #All searches added hear should validate to true so we need "and"
        searchString = {"$and":searchString}
        cur = self.collection.find(searchString)
        return cur
        
    def getRoles(self, username, permission):
        user = self.getUser(username)
        curGroups = list()
        for group in user.groups:
            if permission in group["permissions"]:
                curGroups.append(group["groupname"])
        
        return curGroups
    
    def save(self, document):
        """
        Saves the document Document"
        if you are saving a document with a uri that already exists in the selected lagnuage it will create a new version of that document
        """
        uri = document.metadata["uri"]
        
        #Getting the latest version for this document
        version = 0
        self.__previousVersion = self.getLastByURI(uri, document.metadata["language"])
        if self.__previousVersion != None:
            version = int(self.__previousVersion["metadata"]["version"])+1
        
        #Update the current active version to be inactive as our new version should be the active one
        docs = self.getAllByURI(uri, document.metadata["language"])
        for doc in docs:
            self.collection.update({"metadata.uri":uri, "metadata.active":True},{'$set': {"metadata.active":False}})
        
        metadata = document.metadata
        metadata.update({"version":version})
        id = self.collection.insert({"document":document.document, "metadata":metadata})
        return id
        
    def createUser(self, user):
        existingUser = self.getUser(user.username)
        id = None
        if existingUser == None:
            id = self.ctxCollection.insert(user.document)
        else:
            id = self.updateUser(user)
        return id
    
    def getUser(self, username):
        user = None
        document = self.ctxCollection.find_one({"username":username})
        if document != None:
            user = User(document)
        return user
    
    def updateUser(self, user):
        id = None
        self.ctxCollection.update({"username":user.username}, {"$set": {"groups":user.groups}})
        
        

#conv = parse.convertor(html)
#dct = conv.convert()

#json1 = {"attributes": list({"green", "square", "legacay"})}
#json2 = {"attributes": list({"red", "round", "new"})}
#json3 = {"attributes": list({"red", "square", "legacay"})}

#json1 = {"attributes": [{"tag":"green"}, {"tag":"square"}, {"tag":"legacay"}]}
#json2 = {"attributes": [{"tag":"red"}, {"tag":"round"}, {"tag":"legacay"}]}
#json3 = {"attributes": [{"tag":"red"}, {"tag":"square"}, {"tag":"new"}]}

ds = DocumentStore('localhost', 27017)
#doc = Document(json1, '/research/tagR.html', "nl", "Frank")
#ds.save(doc)
#doc = Document(json2, '/research/tagS.html', "nl", "Frank")
#ds.save(doc)
#doc = Document(json3, '/research/tagT.html', "nl", "Frank")
#ds.save(doc)
#ds.showAll()
#ds.getBySearch({"document.attributes": {'$all': ['red', 'square']}})
#ds.getBySearch({"document.attributes": {'$elemMatch' : {'tag':"red", 'tag':"round"}}})


#start = datetime.utcnow()
#for i in range(1000):
#    for j in range(100):
#        json = {"attributes": list({"green"+str(j), "square"+str(j), "legacay"+str(j)})}
#        doc = Document(json, '/research/tag'+str(i)+str(j)+'.html', "nl", "Frank")
#        ds.save(doc)
        
#end = datetime.utcnow()
#print(end-start) #7:36.926135

#ds.collection.ensure_index([("metadata.uri",pymongo.ASCENDING), ("metadata.active", pymongo.ASCENDING)])
#ds.db.create_collection("cms.context")
#user = User('Frank')
#user.addGroup("public", ["r","w","d"])

#for i in range(100):
#    for j in range(10):
#        json = {"attributes": "Testing itteration "+str(i)+str(j)}
#        groups = list()
#        if j%2 == 0:
#            groups.append("public")
#            if i%2 == 0:
#                groups.append("private")
#            else:
#                groups.append("secure")
#        else:
#        groups.append("confidential")
#            if i%2 == 0:
#                groups.append("private")
#            else:
#                groups.append("public")
#        doc = Document(json, '/research/tag'+str(i)+str(j)+'.html', "nl", "Frank", groups)
#        ds.save(doc)