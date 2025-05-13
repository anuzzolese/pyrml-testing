from pyrml import PyRML
import unittest
from unittest import TestCase

from typing import Optional

import os
import re
from pyrml.pyrml_mapper import RMLConverter
from rdflib.graph import Graph, Dataset
from rdflib.namespace import Namespace, RDF, DCTERMS
from rdflib.term import URIRef, BNode, Literal

from rdflib.parser import StringInputSource
from rdflib.namespace import FOAF
from rdflib.plugins.parsers.notation3 import TurtleParser, RDFSink, SinkParser

from rdflib.plugin import register, Parser
import requests
import zipfile
import shutil
import pyodbc
import psycopg2
import pymssql
import time

#SUPPORTED_FORMATS = ['CSV', 'JSON', 'XML', 'SPARQL', 'MySQL', 'PostgreSQL', 'SQLServer']
#SUPPORTED_FORMATS = ['CSV', 'JSON', 'XML']
SUPPORTED_FORMATS = ['CSV']

TRIPLESTORE_ADDRESS = 'http://localhost:3030'

D2RQ = Namespace('http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#')
TC = Namespace('http://rml.io/ns/test-case/')

EXPLAIN = True

class Benchmark():
    
    MASTER = 'https://github.com/kg-construct/rml-test-cases/archive/refs/heads/master.zip'
    
    def __init__(self, fldr: str):
        self.__fldr = fldr
        
    @property
    def testsuite_folder(self):
        return os.path.join(self.__fldr, 'testsuite', 'rml-test-cases-master', 'test-cases')
    
    def __download_tests(self):
        r = requests.get(Benchmark.MASTER)
        if r.status_code == 200:
            print('size:', len(r.content))
            with open('testsuite.zip', 'wb') as fh:
                fh.write(r.content)
                print(r.content[:10])  # display only some part
        else:
            print(r.text)


        with zipfile.ZipFile('testsuite.zip', 'r') as zip_ref:
            zip_ref.extractall(os.path.join(self.__fldr, 'testsuite'))
            
    def __fix(self):
        src = os.path.join(self.__fldr, 'fix', 'metadata.nt')
        dest = os.path.join(self.__fldr, 'testsuite', 'rml-test-cases-master')
        shutil.copy(src, dest)
        print(f'Copied metadata from {src} to {dest}')
        
        testcasefolders = [d for d in os.listdir(os.path.join(self.__fldr, 'fix')) if os.path.isdir(os.path.join(self.__fldr, 'fix', d))]
        
        for testcasefldr in testcasefolders:
            fldr = os.path.join(self.__fldr, 'fix', testcasefldr)
            
            target = os.path.join(dest, 'test-cases', testcasefldr)
            
            files = [os.path.join(fldr, f) for f in os.listdir(fldr) if os.path.isfile(os.path.join(fldr, f))]
            for f in files:
                shutil.copy(f, target)
                print(f'Copied file from {f} to {target}')
            
    def create(self):
        self.__download_tests()
        self.__fix()

class PyRMLTest(TestCase):
    
    def __init__(self, _id, description, ignore_fail: bool = False):
        unittest.TestCase.__init__(self, 'test')
        self.__id = _id
        self.__description = description
        self.__ignore_fail = ignore_fail
        
    @property
    def id(self):
        return self.__id
    
    @property
    def description(self):
        return self.__description
    
    @property
    def ignore_fail(self):
        return self.__ignore_fail
        
    @classmethod
    def setUpClass(cls):
        PyRML.IRIFY = False
        
        cls.metadata = Graph()
        cls.metadata.parse('metadata.nt')
        
        earl = Namespace('http://www.w3.org/ns/earl#')
        
        cls.test_cases = [test_case for test_case in cls.metadata.subjects(RDF.type, earl.TestCase)]
        
        print(cls.test_cases)
            
    
    def setUp(self):
        self.__mapper: RMLConverter = PyRML.get_mapper()
        
    def is_isomorphic(self, d1: Dataset, d2: Dataset):
        ctxs1 = [ctx.identifier for ctx in d1.contexts()]
        ctxs2 = [ctx.identifier for ctx in d2.contexts()]
        for ctx1 in ctxs1:
            print(f'CTX {ctx1}: {type(ctx1)}')
            g1 = d1.get_graph(ctx1)
            if ctx1 in ctxs2:
                
                g2 = d2.get_graph(ctx1)
                morph = g1.isomorphic(g2)
                print(f'MORPH {morph}')
                if not morph:
                    return False
            else:
                print(f'{ctxs1} - {ctxs2}')
                return False
        return True
        
    def test(self):
        
        PyRML.INFER_LITERAL_DATATYPES = False
        
        root = os.getcwd()
        
        cwd = str(self.id)
        os.chdir(cwd)
        
        endpoint = self.id
        
        mapping_file = 'mapping.ttl'
        
        graphs_meta = []
        
        sql = False
        
        if self.id.endswith('-SPARQL'):
            print(f'Executing {os.getcwd()}')
            
            def parse_g(f: str):
                data = Dataset()
                try:
                    data.parse(f, format='ttl')
                
                except FileNotFoundError as e:
                    assert(False)
                    
                return data
            
            
            dl = DataLoader()
            
            graphs = [{'uri': f'{self.id}_{f}', 'file': f} for f in os.listdir('.') if f.startswith('resource')]
            
            for g in graphs:
                
                n = g['file'].replace('resource', '').replace('.ttl', '')
                
                if n == '':
                    graphs_meta.insert(0, g)
                else:
                    graphs_meta.insert(int(n)-1, g)
                
                dl.load_rdf(g['uri'], parse_g(g['file']))
            
            print(f'Graph meta {graphs_meta}.')
            
            mapping_graph = Graph()
            mapping_graph.parse('mapping.ttl', format='ttl')
            tps = mapping_graph.triples((None, URIRef('http://www.w3.org/ns/sparql-service-description#endpoint'), None))
            for s, p, o in tps:
                
                ds = str(o).replace('http://localhost:PORT/', '').replace('/sparql', '')
                
                n = ds.replace('ds', '')
                
                n = 0 if n=='' else int(n)-1
                
                print(f'N is {n} from {o}')
                
                mapping_graph.remove((s, p, o))
                mapping_graph.add((s, URIRef('http://www.w3.org/ns/sparql-service-description#endpoint'), URIRef(f'{TRIPLESTORE_ADDRESS}/{graphs_meta[n]["uri"]}')))
                
            mapping_graph.serialize(destination='mapping-sparql.ttl', format='ttl', encoding='UTF-8')
            
            mapping_file = 'mapping-sparql.ttl'
        elif self.id.endswith('-MySQL') or self.id.endswith('-PostgreSQL') or self.id.endswith('-SQLServer'):
            print('Got SQL.')
            dbtype = self.id.split('-')[1].lower()
            mapping_file = self.__manage_sql(dbtype)
            PyRML.INFER_LITERAL_DATATYPES = True
            sql = True
            
        try:
            start = time.time()
            g_1: Dataset = self.__mapper.convert(mapping_file)
            exec_time = time.time() - start
            
            print(f'Exec time: {exec_time}')
            
        except Exception as e:
            print(e)
            g_1 = Dataset()
            
        ctxs = [ctx for ctx in g_1.contexts()]
        
        if len(ctxs) > 0:
            g_1.serialize('output_pyrml.nq', 'nquads')
        else:
            g_1.serialize('output_pyrml.nt', 'nt')
            
        g_2 = Dataset()
        try:
            g_2.parse('output.nq', format='nquads')
        except FileNotFoundError as e:
            pass
            
        _isomorphic = self.is_isomorphic(g_1, g_2)
        print(f'ISOMORPHIC {_isomorphic}')
        
        if EXPLAIN:
            if not _isomorphic and not self.ignore_fail:
                print(f'Error test: {self.id}')
                g_diff = g_1 - g_2
                print('Diffs g1-g2')
                for t in g_diff:
                    print(f'\t {t}')
                    
                g_diff = g_2 - g_1
                print('Diffs g2-g1')
                for t in g_diff:
                    print(f'\t {t}')
                    
                print('Triples in g1')
                for t in g_1:
                    print(f'\t {t}')
                    
                print('Triples in g2')
                for t in g_2:
                    print(f'\t {t}')
                
        for gm in graphs_meta:
            dl = DataLoader()
            #dl.remove_graph(gm['uri'])
            
        if sql:
            dl = DataLoader()
            #dl.drop_database()
            
        
        self.assertTrue(_isomorphic or self.ignore_fail)
        
        os.chdir(root)
        
        
    def tearDown(self):
        self.__mapper.reset()
        
        PyRML.delete_mapper()
        
    def __manage_sql(self, type:str):
        
        print(f'Executing SQL {os.getcwd()}')
            
        def parse_g(f: str):
            data = Dataset()
            try:
                data.parse(f, format='ttl')
            
            except FileNotFoundError as e:
                assert(False)
                
            return data
    
        
        dl = DataLoader()
        type = type.lower()
        if type == 'mysql':
            dl.load_mysql('resource.sql')
        elif type == 'postgresql':
            dl.load_postgresql('resource.sql')
        elif type == 'sqlserver':
            dl.load_sqlserver('resource.sql')
        
        mapping_graph = Graph()
        mapping_graph.parse('mapping.ttl', format='ttl')
        databases = mapping_graph.subjects(RDF.type, D2RQ.Database, True)
        
        for database in databases:
            driver = mapping_graph.value(database, D2RQ.jdbcDriver, None)
            if driver == Literal('com.mysql.cj.jdbc.Driver'):
                mapping_graph.remove((database, D2RQ.jdbcDSN, Literal('CONNECTIONDSN')))
                mapping_graph.add((database, D2RQ.jdbcDSN, Literal('mysql+mysqlconnector://localhost:3306/test?charset=utf8mb4')))
                
                mapping_graph.remove((database, D2RQ.password, Literal('')))
                mapping_graph.add((database, D2RQ.password, Literal('pyrml')))
                
            if driver == Literal('org.postgresql.Driver'):
                mapping_graph.remove((database, D2RQ.jdbcDSN, Literal('CONNECTIONDSN')))
                mapping_graph.add((database, D2RQ.jdbcDSN, Literal('postgresql+psycopg2://localhost/test')))
                
                mapping_graph.remove((database, D2RQ.password, Literal('')))
                mapping_graph.remove((database, D2RQ.password, Literal('password')))
                mapping_graph.add((database, D2RQ.password, Literal('pyrml')))
            if driver == Literal('com.microsoft.sqlserver.jdbc.SQLServerDriver'):
                mapping_graph.remove((database, D2RQ.jdbcDSN, Literal('CONNECTIONDSN')))
                mapping_graph.add((database, D2RQ.jdbcDSN, Literal('mssql+pymssql://localhost:1433/TestDB')))
                
                pwd_triples = [triple for triple in mapping_graph.triples((database, D2RQ.password, None))]
                for t in pwd_triples: 
                    mapping_graph.remove(t)
                mapping_graph.add((database, D2RQ.password, Literal('_pyRML_admin')))
                
                
        mapping_graph.serialize(destination='mapping-sql.ttl', format='ttl', encoding='UTF-8')
        
        return 'mapping-sql.ttl'
        
        
class MyTurtleParser(TurtleParser):

    def parse(
        self,
        source: "InputSource",
        graph: Graph,
        encoding: Optional[str] = "utf-8",
        turtle: bool = True,
    ):
        if encoding not in [None, "utf-8"]:
            raise ParserError(
                "N3/Turtle files are always utf-8 encoded, I was passed: %s" % encoding
            )

        sink = RDFSink(graph)

        baseURI = graph.absolutize(source.getPublicId() or source.getSystemId() or "")
        p = SinkParser(sink, baseURI=baseURI, turtle=turtle)
        # N3 parser prefers str stream
        stream = source.getCharacterStream()
        if not stream:
            stream = source.getByteStream()
        p.loadStream(stream)

        for prefix, namespace in p._bindings.items():
            graph.bind(prefix, namespace)
            
        if p._baseURI:
            graph.base = p._baseURI 


class DataLoader:
    
    def load_rdf(self, graph_name: str, graph: Graph):
        
        
        print(f'Loading {graph_name} into Fuseki.')
        data = {'dbType': 'tdb', 'dbName': graph_name}
        x = requests.post(f'{TRIPLESTORE_ADDRESS}/$/datasets', data=data, auth=('admin', 'pyrml'))
        
        headers = {'Content-type': 'text/turtle'}
        r = requests.post(f'{TRIPLESTORE_ADDRESS}/{graph_name}', auth=('admin', 'pyrml'), data=graph.serialize(format='turtle', encoding='UTF-8'), verify=False, headers=headers)
        
    def remove_graph(self, graph_name):
        r = requests.delete(f'{TRIPLESTORE_ADDRESS}/$/datasets/{graph_name}', auth=('admin', 'pyrml'))
        
    def load_mysql(self, sql_script: str):
        
        connection_string = (
            'DSN=MySQL_;'
            'UID=root;'
            'PWD=pyrml;'
            'Port=3306;'
            'charset=utf8;'
        )
                
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()
        try:
            cursor.execute('CREATE DATABASE test;')
        except pyodbc.Error as e:
            cursor.execute('DROP DATABASE test;')
            cursor.execute('CREATE DATABASE test;')
            
            
            
        with open(sql_script, 'r') as inserts:
            query_string = inserts.read()
    
            stmts = query_string.split(';')
            for stmt in stmts:
                stmt = stmt.replace('\n', ' ')
                if stmt: 
                    stmt += ';'
                    try:
                        cursor.execute(stmt)
                    except pyodbc.Error as e:
                        print(e)
        
        cursor.commit()
        cursor.commit()
        cursor.close()
        cnxn.close()
        
    def load_postgresql(self, sql_script: str):
        
        cnxn = psycopg2.connect(
            host = 'localhost',
            user = 'postgres',
            password = 'pyrml',
            port = 5432
        )
        cnxn.autocommit = True
        
        cursor = cnxn.cursor()
        
        cursor.execute('DROP DATABASE IF EXISTS test')
        cursor.execute('CREATE DATABASE test;')
        
            
        cursor.close()
        cnxn.close()
        
        
        cnxn = psycopg2.connect(
            host = 'localhost',
            user = 'postgres',
            database = 'test',
            password = 'pyrml',
            port = 5432
        )
        cnxn.autocommit = True
        
        cursor = cnxn.cursor()
        
        
        with open(sql_script, 'r') as inserts:
            query_string = inserts.read()
    
            stmts = query_string.split(';')
            for stmt in stmts:
                stmt = stmt.replace('\n', ' ').strip()
                if stmt: 
                    stmt += ';'
                    print(f'Stmt: {stmt}')
                    try:
                        cursor.execute(stmt)
                    except pyodbc.Error as e:
                        print(e)
        
        cursor.close()
        cnxn.close()
        
    def load_sqlserver(self, sql_script: str):
        
        cnxn = pymssql.connect(
            host = 'localhost',
            user = 'sa',
            password = '_pyRML_admin',
            port = 1433,
            autocommit = True
        )
        
        cursor = cnxn.cursor()
        
        cursor.execute('DROP DATABASE IF EXISTS TestDB')
        cursor.execute('CREATE DATABASE TestDB;')
        
            
        cursor.close()
        cnxn.commit()
        cnxn.close()
        
        
        cnxn = pymssql.connect(
            host = 'localhost',
            user = 'sa',
            database = 'TestDB',
            password = '_pyRML_admin',
            port = 1433
        )
        
        cursor = cnxn.cursor()
        
        
        with open(sql_script, 'r') as inserts:
            query_string = inserts.read()
    
            stmts = query_string.split(';')
            for stmt in stmts:
                stmt = stmt.replace('\n', ' ').strip()
                if stmt: 
                    stmt += ';'
                    print(f'Stmt: {stmt}')
                    try:
                        cursor.execute(stmt)
                    except pyodbc.Error as e:
                        print(e)
        
        cursor.close()
        cnxn.commit()
        cnxn.close()
        
        
            
    def drop_database(self):
        connection_string = (
            'DSN=MySQL;'
            'UID=root;'
            'PWD=pyrml;'
            'charset=utf8;'
        )
                
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()
        cursor.execute('DROP DATABASE test;')
        cursor.commit()
        cursor.close()
        cnxn.close()
        

if __name__ == '__main__':
    cwd = os.getcwd()
    print(cwd)
    
    benchmark = Benchmark('benchmark')
    benchmark.create()
    
    try:
        PyRML.IRIFY = False
        PyRML.RML_STRICT = True
            
        os.chdir(benchmark.testsuite_folder)

        metadata = Graph()
        metadata.parse(os.path.join('..', 'metadata.nt'))
            
        earl = Namespace('http://www.w3.org/ns/earl#')
            
        test_cases = [test_case for test_case in metadata.subjects(RDF.type, earl.TestCase)]
        
        suite = unittest.TestSuite()    
        for test_case in test_cases:        
            _id = metadata.value(test_case, DCTERMS.identifier)
            if str(_id).endswith(tuple(SUPPORTED_FORMATS)):
                print(f'TEST {str(_id)}')
                description = metadata.value(test_case, DCTERMS.description)
                
                ignore_fail = metadata.value(test_case, TC.ignoreFail)
                ignore_fail = bool(ignore_fail) if ignore_fail else False
                #if os.path.exists(_id) and str(_id) == 'RMLTC0019b-CSV':
                if os.path.exists(_id):
                    suite.addTest(PyRMLTest(_id, description, ignore_fail))
                    print(f'Added test {_id}: {description}')
                    
                    '''
                    TEST not based on unittest
                    
                    __mapper: RMLConverter = PyRML.get_mapper()
                    os.chdir(_id)
                    g_1: Dataset = __mapper.convert(os.path.join(_id, 'mapping.ttl'))
                    '''
                    
                    

        # call my test
        unittest.TextTestRunner(verbosity=1).run(suite)
    finally:
        os.chdir(cwd)
    
