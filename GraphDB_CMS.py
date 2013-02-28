from py2neo import neo4j, cypher, geoff
import datetime

class DocumentStore:
    """
    The document management system
    """
    
    def __init__(self, connectionData): 
        """
        @param connectionData: http://localhost:7474/db/data/
        """
        self.__graphDB = neo4j.GraphDatabaseService(connectionData)
    
    @property
    def connection(self):
        """
        @return the connection
        """
        return self.__graphDB
        
    @property
    def reference(self):
        """
        @return the 0 node
        """
        return self.connection.get_reference_node()
        
    def createDocument(self, name, language):
        """
        Create a new document (if it exists the existing document is loaded
        @param name the documentName
        @param language the language your document is in
        """
        doc = Document(self, name, language)
        return doc
        
    @property
    def valueRoot(self):
        """
        @return the root for the values
        """
        return self.reference.get_related_nodes(0, "VAL_ROOT")[0]
        
    @property
    def documentRoot(self):
        """
        @return the root for the documents
        """
        return self.reference.get_related_nodes(0, "DOC_ROOT")[0]
        
    @property
    def userRoot(self):
        """
        @return the root for the users
        """
        return self.reference.get_related_nodes(0, "USER_ROOT")[0]
        
    @property
    def valueIndex(self):
        """
        @return the index for the values
        """
        return self.connection.get_index(neo4j.Node, "values")
        
    @property
    def userIndex(self):
        """
        @return the index for the users
        """
        return self.connection.get_index(neo4j.Node, "users")
        
    @property
    def documentIndex(self):
        """
        @return the index for the documents
        """
        return self.connection.get_index(neo4j.Node, "documents")
        
    def getDocumentContent(self, documentName, language):
        """
        Get document in latest version
        """
        query = "start n=node:documents(documentName={DOC}) match (n)-[:"+language+"]->(l)-[:CURRENT]->(c) return c.content"
        result = cypher.execute(self.connection, query, {"DOC": documentName})
        return result[0][0][0]
    
class Document:
    
    def __init__(self, store, name, language):
        """
        @param store the document store
        @param name the unique name of the document (not language specific)
        @param language the language of this document
        """
        self.__store = store
        self.__name = name
        self.__attributes = list()
        self.__language = language     
        self.__documentNode = None
        try:   
            self.__documentNode = self.__store.documentIndex.get("documentName", name)[0]
        except Exception:
            print("new document")
        if self.__documentNode is not None:
            self.__translationDoc = self.__getCurrentLanguage()
            self.__loadAttributes()
        else:
            self.__translationDoc = None
        
        # Simple idea of the graph
        # document -[LANGUAGE]-> version
        # document -[<ATTRIBUTE>]-> value
        
    @property
    def name(self):
        """
        @return name of the document
        """
        return self.__name
        
    @property
    def content(self):
        """
        @return content of the document set by the user
        """
        return self.__content
        
    @property
    def documentNode(self):
        """
        @return the node that represents the document (the master for all translations, all versions)
        """
        return self.__documentNode
        
    @property
    def version(self):
        """
        @return the current version of the document
        """
        version = self.__getVersion()
        
    def updateAttribute(self, key, value):
        """
        change an attribute, doesn't work for lists
        """
        att = self.__getAttribute(key)
        att['value'] = value
        
    def addAttribute(self, key, value, translatable=None):  
        """
        Add attribute in memory
        @param key the name of the attribute
        @param value can be a lsit for multiple value fields
        @param translatable if None then the key/value is used for all translations, otherwise it's only used for 1 translation
        """
        existing = self.__getAttribute(key)
        if existing is not None:
            val = existing['value']
            if type(val) == list:
                val.append(value)
            else:
                existing['value'] = list({val, value})
        else:
            att = dict(key=key, value=value, translatable=translatable)
            self.__attributes.append(att)
        
    def addContent(self, content):
        """
        Add content to the document in memory
        """
        self.__content = content
        
    def saveFile(self, user):
        """
        Save the file to Neo4J (saves attributes and content effectivly to Neo4J)
        Saving an object 2 times creates new revisions for the content
        MISSING: Removed attributes will not really be removed, only new ones added
        """
        docIndex = self.__store.documentIndex
        self.__documentNode = docIndex.get_or_create("documentName", self.__name, dict(documentName=self.__name))        
        self.__store.documentRoot.create_relationship_to(self.__documentNode, "DOCUMENT")
        self.__saveAttributes()
        if self.__translationDoc is None:
            self.__translationDoc = self.__store.connection.create(dict())[0]
            self.__documentNode.create_relationship_to(self.__translationDoc, self.__language.upper())
        if self.__content is not None:
            content = self.__store.connection.create(dict(content=self.__content))[0]
            self.__translationDoc.create_relationship_to(content, 'HAS_CONTENT', dict(version=self.version))
            self.__setCurrentVersion(content)
        self.addPermission(user)
            
    def addPermission(self, username):
        """
        add permission for usernam to the current document
        """
        if self.__documentNode is not None:        
            user = self.__store.userIndex.get("username", username)
            if user is None or len(user) == 0:
                user = self.__store.userIndex.get_or_create("username", username, dict(username=username))
                rel = user.create_relationship_to(self.__store.userRoot, 'IS_USER')
            else:
                user = user[0]
            rel = user.create_relationship_to(self.__documentNode, 'ALLOWED')
        else:
            print("not yet in DB")
    
    def removePermission(self, username):
        """
        remove permission for username to the current document
        """
        if self.__documentNode is not None:        
            query = "start n=node({ID}) match (n)<-[c:ALLOWED]-(u) where u.username={USER} delete c"
            result = cypher.execute(self.__store.connection, query, {"ID": self.__documentNode.id, "USER": username})
        else:
            print("not yet in DB")
    
    def checkPermission(self, username):
        """
        @return true when username has access to the configuration
        """
        allowed = False
        if self.__documentNode is not None:        
            query = "start n=node({ID}) match (n)<-[:ALLOWED]-(u) where u.username={USER} return u.username"
            result = cypher.execute(self.__store.connection, query, {"ID": self.__documentNode.id, "USER": username})
            if len(result[0]) > 0 and result[0][0][0] == username:
                allowed = True
        else:
            print("not yet in DB")
            
        return allowed
        

    def __setCurrentVersion(self, content):
        """
        Make sure that content is the current version for the current document and translation
        """
        query = "start n=node({ID}) match (n)-[r:CURRENT]->(c) delete r"
        result = cypher.execute(self.__store.connection, query, {"ID": self.__translationDoc.id})
        self.__translationDoc.create_relationship_to(content, 'CURRENT')
        
    def __loadAttributes(self):
        """
        Load attributes from Neo4j to memory
        """
        query = "start d=node({A}) match (d)-[r]->(v) where has(v.value) return type(r) as att, v.value as value"
        attributes = version = cypher.execute(self.__store.connection, query, {"A": self.documentNode.id})[0]
        for att in attributes:
            self.addAttribute(key=att[0].lower(), value=att[1])
        
    def __saveAttributes(self):
        """
        Persists the attributes, can't delete attributes
        """
        con = self.__store.connection
        valRoot = self.__store.valueRoot
        valueIndex = self.__store.valueIndex
        for att in self.__attributes:
            relation = att['key'].upper()
            if type(att['value']) == list:
                for value in att['value']:
                    existing = valueIndex.get("value", value)
                    val = None
                    if existing is None or len(existing) == 0:
                        val = valueIndex.get_or_create("value", value, dict(value=value))
                        rel = valRoot.create_relationship_to(val, "VALUE")
                    else:
                        val = existing[0]
                    if att['translatable'] != None:
                        rel = self.__translationDoc.create_relationship_to(val, relation)
                    else:
                        rel = self.__documentNode.create_relationship_to(val, relation)
            else:
                val = valueIndex.get_or_create("value", att['value'], dict(value=att['value']))
                rel = valRoot.create_relationship_to(val, "VALUE")
                if att['translatable'] != None:
                    rel = self.__translationDoc.create_relationship_to(val, relation)
                else:
                    rel = self.__documentNode.create_relationship_to(val, relation)
                
                
    def __getVersion(self):
        """
        @return the current version
        """
        version = 0
        query = "start n=node({A}) match (n)-[:"+self.__language.upper()+"]->(l)-[r:HAS_CONTENT]->(c) return max(r.version)  as max_version"
        try:
            version = cypher.execute(self.__store.connection, query, {"A": self.__documentNode.id})[0][0][0]
            version +=1
        except Exception:
            print ("first version")

        if version is None:
                version = 0
        return version
        
    def __getAttribute(self, key):
        """
        @return attribute from memory
        """
        attribute = None
        for att in self.__attributes:
            if key == att['key']:
                attribute = att
                break
                
        return attribute
                
    def __getCurrentLanguage(self):
        """
        @return the current language node
        """
        query = "start n=node({A}) match (n)-[:"+self.__language.upper()+"]->(k) return k"
        node = cypher.execute(self.__store.connection, query, {"A": self.__documentNode.id, "L": self.__language})[0][0][0]
        return node
    
            
## SETUP ##
def setup():
    root = graph_db.get_node(0)
    valRoot = graph_db.create(dict(), (root, 'VAL_ROOT', 0))
    valRoot = graph_db.create(dict(), (root, 'DOC_ROOT', 0))
    valRoot = graph_db.create(dict(), (root, 'USER_ROOT', 0))
    indexes()

## INDEXES  ###
def indexes():
    graph_db.get_or_create_index(neo4j.Node, "values")
    graph_db.get_or_create_index(neo4j.Node, "documents")
    graph_db.get_or_create_index(neo4j.Node, "users")
    
## TEAR DOWN ##
def tearDown():
    query = 'start r=relationship(*) delete r'
    cypher.execute(graph_db, query)
    query = 'start n=node(*) where id(n) > 0 delete n'
    cypher.execute(graph_db, query)

## RUN ##
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
tearDown()
setup()
store = DocumentStore("http://localhost:7474/db/data/")
doc = store.createDocument("doc_000001", 'EN')
doc.addAttribute("organization", list({"Moonfish", "Neo Technology"}))
doc.addAttribute("title", "First Doc")
doc.addContent("This is the us text")
doc.saveFile("me")
doc2 = store.createDocument("doc_000002", 'EN')
doc2.addAttribute("organization", list({"Sun", "Oracle"}))
doc2.addAttribute("title", "Second Doc")
doc2.addContent("This is another text in US")
doc2.saveFile("me")
doc3 = store.createDocument("doc_000003", 'FR')
doc3.addAttribute("organization", "oracle")
doc3.addAttribute("title", "Another document")
doc3.addContent("This is a completely different document")
doc3.saveFile("jos")
doc4 = store.createDocument("doc_000004", 'FR')
doc4.addAttribute("organization", list({"Oracle", "Microsoft"}))
doc4.addAttribute("title", "Document 4")
doc4.addContent("This is a completely different document")
doc4.saveFile("jos")
docm = store.createDocument("doc_000004", 'FR')
docm.addContent("This is the second version")
docm.saveFile("mark")
print(store.getDocumentContent("doc_000004", 'FR'))
print("Access allowed: "+str(doc.checkPermission("me")))
print("Access allowed: "+str(doc2.checkPermission("me")))
print("Access allowed: "+str(doc3.checkPermission("me")))
print("Access allowed: "+str(doc4.checkPermission("me")))
print("Access allowed: "+str(docm.checkPermission("me")))
doc.removePermission("me")
print("Access allowed: "+str(doc.checkPermission("me")))