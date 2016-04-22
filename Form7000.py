
# coding: utf-8

# In[359]:

import requests
import json
from pandas.io.json import json_normalize
import pandas as pd
import inflection
import os.path
import base64
import yaml
from sodapy import Socrata
import itertools
import numpy as np


# In[360]:

class ConfigItems:
    '''
    util class to grab config params from a .yaml file
    '''
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile
        
    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems

class SocrataClient:
    '''
    util class to make a socrata client from a .yaml config file
    '''
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems
        
    def connectToSocrata(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                client_items = yaml.load(stream)
                client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
                return client
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    


# In[361]:

class form700:
    '''
    class to grab form 700 data off the API
    '''
    def __init__(self, configItems):
        self.username = configItems['form700_username']
        self.password = configItems['form700_password']
        self.authUrl = configItems['authUrl']
        self.url_cover = configItems['url_cover']
        self.url_schedule = configItems['url_schedule']
        self.agency_prefix = configItems['agency_prefix']
        self.page_size = 1000
        self.headers = {'content-type': 'application/json'}
        self.cookies = self.grabCookies()
        self.schedules = ['scheduleA1', 'scheduleA2', 'scheduleB', 'scheduleC', 'scheduleD', 'scheduleE']
    
    def getSchedules(self):
        return self.schedules

    def grabCookies(self):
        '''
        gets session cookie; then sets cookie property in the object
        '''
        authUrl = self.authUrl + '?UserName=' + self.username + '&Password='+ base64.b64decode(self.password)
        r = requests.post(authUrl)
        return r.cookies
    
    def makeRequest(self, url, current_page):
        '''
        method to make API calls to form 700 API
        '''
        rContent = None
        responseJson = None
        payload = {'AgencyPrefix': self.agency_prefix, 'CurrentPageIndex': current_page, 'PageSize': self.page_size}
        rContent = requests.post(url, params=payload, headers = self.headers, cookies=self.cookies)
        try:
            responseJson =  json.loads(rContent.content)
        except:
            print "could not load content as json"
        return responseJson
    
    def getJsonData(self, url, keyToPluck=None):
        '''
        method to grab json data from API response
        '''
        total_pages = 0
        current_page = 0
        filing_data = []
        while current_page <= total_pages:
            current_page = current_page + 1
            responseJson = self.makeRequest(url, current_page)
            if keyToPluck:
                filing_data = filing_data + responseJson[keyToPluck]
            else:
                filing_data = filing_data + [responseJson]
            total_pages = responseJson['totalMatchingPages']
        return filing_data
    
    def getCoverData(self):
        '''
        returns cover sheet data
        '''
        cover_data = self.getJsonData(self.url_cover, 'filings')
        cover_data = json_normalize(cover_data)
        cover_data = {'cover':cover_data}
        return cover_data
    
    def getScheduleData(self):
        '''
        returns schedules A1, A2, B, C, D and E
        '''
        parsed_schedulesDict = {}
        schedule_data = self.getJsonData(self.url_schedule)
        parsed_schedules = [  self.parseScheduleData(schedule, schedule_data) for schedule in self.schedules ]
        for i in range(len(self.schedules)):
            parsed_schedulesDict[self.schedules[i]] = parsed_schedules[i][self.schedules[i]]
        return parsed_schedulesDict
    
    @staticmethod
    def parseScheduleData(schedule, schedule_data):
        '''
        normalizes json data into python pandas dataframe; saves it as a dict objec
        '''
        single_schedule_list = [ data[schedule] for data in schedule_data]
        #chain the list together
        single_schedule_list = list(itertools.chain(*single_schedule_list ))
        return  {schedule: json_normalize( single_schedule_list) }


# In[362]:

class prepareDataSetSchema:
    '''
    class to prepare csv files of the dataset schema
    '''
    def __init__(self, configItems=None):
        self.schema_dir = configItems['schema_dir']
        
    @staticmethod
    def make_columns(df):
        '''
        makes column schemas
        '''
        schema = []
        columnNames = list(df.columns)
        names = [ inflection.titleize(k) for k in columnNames]
        for i in range(len(columnNames)):
            col = {}
            col['fieldName'] = columnNames[i]
            col['name'] = names[i]
            col['dataTypeName'] = None
            schema.append(col)
        schema = pd.DataFrame(schema)
        return schema
    
    def makeSchemaCsv(self, df, dfName):
        '''
        outputs schema into csv 
        '''
        fname = self.schema_dir + "/" + dfName+ ".csv"
        schema = self.make_columns(df)
        if(not(os.path.isfile(fname))): 
            schema.to_csv(fname)
            return "created" + fname
        else:
            return fname + "exists! No file created"
       
    def makeSchemaSchedules(self,  schedule_data, schedules):
        '''outputs schedules as a csvs'''
        return [ self.makeSchemaCsv(schedule_data[schedule], "form700_"+ schedule+ "_schema") for schedule in schedules]
    
    def makeSchemaOutFiles(self, schedule_data,cover_data, schedules ):
        '''
        method to output all the csvs files- need to fill out the column data types
        '''
        schedule_schemas = self.makeSchemaSchedules(schedule_data, schedules)
        cover_schema = self.makeSchemaCsv(cover_data, 'form700_cover_schema')


# In[363]:

class SocrataCreateInsertUpdateForm700Data:
    def __init__(self,  configItems, client=None):
        self.client = client
        self.schema_dir = configItems['schema_dir']
        self.tables = self.setTableInfo()
        self.dataset_base_url = configItems['dataset_base_url']
    
    def getTableInfo(self):
        return self.tables
        
    def setTableInfo(self):
        tables_fname = self.schema_dir + "form700_tables.csv"
        return pd.read_csv(tables_fname)
    
    def makeDataSet(self, dfname):
        dataset = {}
        dataset['columns'] = self.getColumns(dfname)
        dataset = self.parseTableInfo(dataset, dfname)
        return dataset
    
    def getColumns(self, dfname):
        '''
        creates columns as defined in schema csvs
        '''
        schema_fname = self.schema_dir + "form700_"+ dfname + "_schema.csv"
        fields = pd.read_csv(schema_fname)
        fieldnames = list(fields['fieldName'])
        #note: this is changing shit into plural 
        fieldnames = [ inflection.underscore(field) for field in fieldnames]
        names = list(fields['name'])
        dataTypeName = list(fields['dataTypeName'])
        columns = zip(fieldnames, names, dataTypeName)
        columns = [{"fieldName":item[0], "name":item[1], "dataTypeName": item[2]} for item in columns ]
        return columns
    
    def parseTableInfo(self, socrata_dataset, dfname):
        self.tables['FourByFour']= self.tables['FourByFour'].fillna(0)
        table = self.tables[self.tables['df_name'] == dfname].iloc[0]  #just want the first row
        socrata_dataset['description'] = table['description']
        socrata_dataset['tags'] = table['tags']
        socrata_dataset['category'] = table['category']
        socrata_dataset['dataset_name'] = table['dataset_name']
        socrata_dataset['FourByFour'] = table['FourByFour']
        return socrata_dataset
    
    def createDataSet(self, dfname):
        dataset = self.makeDataSet(dfname)
        if dataset['FourByFour']== 0:
            try:
                socrata_dataset = self.client.create(dataset['dataset_name'], description=dataset['description'], columns=dataset['columns'], category=dataset['category'], new_backend=False)
                FourXFour = str(socrata_dataset['id'])
                dataset['Dataset URL'] = self.dataset_base_url + FourXFour
                dataset['FourByFour'] = FourXFour
                print "4x4 "+dataset['FourByFour']
            except:
                print "*****ERROR*******"
                dataset['Dataset URL'] = ''
                dataset['FourByFour'] = 'Error: did not create dataset'
                print "4x4 "+ dataset['FourByFour']
                print "***************"
        return dataset
    
    def insertDataSet(self, dataset, dataset_dict, dfname):
        insertDataSet = []
        #keep track of the rows we are inserting
        dataset['rowsInserted'] = 0
        print dataset['FourByFour']
        ##need to rename all the columns to fit socrata- need to use titlize
        fieldnames = list(dataset_dict[dfname].columns)
        df_fields = [ inflection.underscore(field) for field in fieldnames]
        columndict = dict(zip(fieldnames,df_fields ))
        dataset_dict[dfname]= dataset_dict[dfname].rename(columns=columndict)
        try:
            insertDataSet = dataset_dict[dfname].to_dict('records')

        except:
            result = 'Error: could not get data'
            return dataset
        #need to chunk up dataset so we dont get Read timed out errors
        if len(insertDataSet) > 1000 and (not(insertDataSet is None)):
            #chunk it
            insertChunks=[insertDataSet[x:x+1000] for x in xrange(0, len(insertDataSet), 1000)]
            #overwrite the dataset on the first insert
            print insertChunks[1][230]
            result = self.client.replace(dataset['FourByFour'], insertChunks[0])
            print result
            try: 
                result = self.client.replace(dataset['FourByFour'], insertChunks[0])
                print "First Chunk: Rows inserted: " + str(dataset['rowsInserted'])
                dataset['rowsInserted'] =  int(result['Rows Created'])
            except:
                result = 'Error: did not insert dataset chunk'
            for chunk in insertChunks[1:]:
                try:
                    result = self.client.upsert(dataset['FourByFour'], chunk)
                    dataset['rowsInserted'] = dataset['rowsInserted'] + int(result['Rows Created'])
                    print "Additional Chunks: Rows inserted: " + str(dataset['rowsInserted'])
                    time.sleep(1)
                except:
                    result = 'Error: did not insert dataset chunk'
        elif len(insertDataSet) < 1000 and (not(insertDataSet is None)):
            #print insertDataSet[0]
            try:
                result = self.client.replace(dataset['FourByFour'], insertDataSet) 
                dataset['rowsInserted'] = dataset['rowsInserted'] + int(result['Rows Created'])
                print "Rows inserted: " + str(dataset['rowsInserted'])
            except:
                print 'Error: did not insert dataset'
        return dataset
    
    def postDataToSocrata(self, dfname, dataset_dict ):
        dataset = self.createDataSet(dfname)
        if dataset['FourByFour']!= 0:
            dataset = self.insertDataSet( dataset, dataset_dict, dfname)
        else: 
            print "dataset does not exist"
        return dataset


# In[364]:

class dataSetPrep:
    '''
    class to clean up dataframe objects and cast columns based on defined schema
    '''
    
    def __init__(self, configItems):
        self.schema_dir = configItems['schema_dir']

    
    def cleanDataSet(self, df_dict, dfname, tables):
        schema_fname = self.schema_dir + "form700_"+ dfname + "_schema.csv"
        df = self.checkForListColumns(tables, df_dict, dfname)
        fields = pd.read_csv(schema_fname)
        fieldnames = list(fields['fieldName'])
        dataTypeName = list(fields['dataTypeName'])
        fieldTypesDict = dict(zip(fieldnames, dataTypeName))
        df = df[fieldnames]
        df = self.removeNewLines(df)
        df = self.castFields(df, fieldTypesDict)
        return df
    
    @staticmethod
    def castFields(df, fieldTypesDict):
        for field, ftype in fieldTypesDict.iteritems():
            if ftype == 'number':
                df[field] = df[field].fillna(0)
                df[field]= df[field].astype(int)
            elif ftype == 'text':
                df[field] = df[field].fillna(value="")
                df[field]= df[field].astype(str)
                df[field] = df.apply(lambda row: checkForListRow(row,field),axis=1)
            elif ftype == 'checkbox':
                df[field] = df[field].fillna(value=False)
                df[field]= df[field].astype(bool)
            elif ftype == 'date':
                df[field] = df[field].fillna(value="")
                df[field] = pd.to_datetime(df[field], errors='coerce' , format='%Y%m%d')
                #df[field]= df[field].astype('datetime64[ns]')
        return df
    
    @staticmethod
    def removeNewLines(df):
        return df.replace({'\n': ''}, regex=True)
    
    def cleanDataSetDict(self, dataset_dict, tables):
        for df_key,df_val in dataset_dict.iteritems():
            cleaned_df = self.cleanDataSet( dataset_dict, df_key, tables)
        return dataset_dict

    @staticmethod
    def flatten_json(row, field):
        jsonField = row[field]
        allItems = []
        for item in jsonField:
            item_kv = []
            for k,v in item.iteritems():
                if str(v) == "":
                    v = None
                item_str = str(k) + ":" + str(v)
                item_kv.append(item_str)
            line_item =  ",".join(item_kv)
            allItems.append(line_item)
        allItems = "|".join(allItems)
        return allItems
    
    def checkForListColumns(self, tables, df_dict, dfname):
        table = tables[tables['df_name'] == dfname]
        list_columns = table[['list_columns']]
        list_columns = list_columns.iloc[0]['list_columns']
        list_columns = list_columns.split(",")
        df = df_dict[dfname]
        for col in list_columns:
            df[col] = df.apply(lambda row: self.flatten_json(row, col), axis=1)
        return df


# In[365]:

inputdir = "/home/ubuntu/workspace/configFiles/"
fieldConfigFile = 'fieldConfig.yaml'
cI =  ConfigItems(inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(inputdir, configItems)
client = sc.connectToSocrata()
#class objects
scICU = SocrataCreateInsertUpdateForm700Data(configItems,client)
dsp = dataSetPrep(configItems)


# In[366]:

f700 = form700(configItems)
schedules =  f700.getSchedules()
tables = scICU.getTableInfo()


# In[367]:

cover_data = f700.getCoverData()


# In[368]:

#schedule_data = f700.getScheduleData()


# In[369]:

#needed to create schema csv files 
#cds= createDataSets(config_dir, configItems)
#schemas = cds.makeSchemaOutFiles(schedule_data,cover_data, schedules )


# In[371]:

scICU = SocrataCreateInsertUpdateForm700Data(configItems,client)
datasetnew = scICU.postDataToSocrata('cover', cover_data)#socrata_dataset = scICU.createDataSet('cover')


# In[40]:

def flattenColumn(input, column):
    column_flat = pd.DataFrame([[i, c_flattened] for i, y in input[column].apply(list).iteritems() for c_flattened in y], columns=['I', column])
    column_flat = column_flat.set_index('I')
    return input.drop(column, 1).merge(column_flat, left_index=True, right_index=True)
#len(cover_data['cover'])
df = cover_data['cover']
new_df = flattenColumn(df , 'offices')
new_df[new_df['filerName'] == 'Mosser, Neveo']
#offices = df['offices'].apply(pd.Series)  

