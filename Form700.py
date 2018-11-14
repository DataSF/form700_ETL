
# coding: utf-8

# In[149]:

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
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
import csv
import time
import datetime
import logging
from retry import retry


# In[150]:

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

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn

    def setConfig(self):
        #open a file to clear log
        fo = open(self.logfn, "w")
        fo.close
        logging.basicConfig(level=logging.DEBUG, filename=self.logfn, format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger=logging.getLogger(__name__)
            #self.logfile_fullpath )


# In[151]:

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
        self.schedules = ['scheduleA1', 'scheduleA2', 'scheduleB', 'scheduleC', 'scheduleD', 'scheduleE', 'comments']
        self.schedules_redacted = ['scheduleA1_redacted', 'scheduleA2_redacted', 'scheduleB_redacted', 'scheduleC_redacted', 'scheduleD_redacted', 'scheduleE_redacted', 'comments_redacted']
    
    def getSchedules(self):
        return self.schedules
    
    def getSchedulesRedacted(self):
        return self.schedules_redacted

    def grabCookies(self):
        '''
        gets session cookie; then sets cookie property in the object
        '''
        payload = {"UserName":self.username, "Password":base64.b64decode(self.password)}
        r = requests.post(self.authUrl, data=payload)
        return r.cookies
    
    def makeRequest(self, url, current_page, isRedacted):
        '''
        method to make API calls to form 700 API
        '''
        rContent = None
        responseJson = None
        payload = {'AgencyPrefix': self.agency_prefix, 'CurrentPageIndex': current_page, 'PageSize': self.page_size, 'IsRedacted': isRedacted}
        rContent = requests.post(url, params=payload, headers = self.headers, cookies=self.cookies)
        try:
            responseJson =  json.loads(rContent.content)
        except:
            print "could not load content as json"
        return responseJson
    
    def getJsonData(self, url, isRedacted, keyToPluck=None):
        '''
        method to grab json data from API response
        '''
        total_pages = 0
        current_page = 0
        filing_data = []
        while current_page <= total_pages:
            current_page = current_page + 1
            responseJson = self.makeRequest(url, current_page, isRedacted)
            if keyToPluck:
                filing_data = filing_data + responseJson[keyToPluck]
            else:
                filing_data = filing_data + [responseJson]
            total_pages = responseJson['totalMatchingPages']
        return filing_data
    
    def getCoverData(self, isRedacted=False):
        '''
        returns cover sheet data
        '''
        cover_data = None
        cover_data = self.getJsonData(self.url_cover, isRedacted, 'filings')
        if cover_data:
            cover_data = json_normalize(cover_data)
        if isRedacted:
            cover_data = {'cover_redacted':cover_data}
        else:
            cover_data = {'cover':cover_data}
        return cover_data
    
    def getScheduleData(self, isRedacted=False):
        '''
        returns schedules A1, A2, B, C, D and E
        '''
        parsed_schedulesDict = {}
        schedule_data = self.getJsonData(self.url_schedule, isRedacted)
        parsed_schedules = [  self.parseScheduleData(schedule, schedule_data) for schedule in self.schedules ]
        for i in range(len(self.schedules)):
            parsed_schedulesDict[self.schedules[i]] = parsed_schedules[i][self.schedules[i]]
        if isRedacted:
            parsed_schedulesDict = { k+"_redacted":v for k,v in parsed_schedulesDict.iteritems()}
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
    


# In[152]:

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


# In[153]:

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
    
  
    def castFields(self, df, fieldTypesDict):
        for field, ftype in fieldTypesDict.iteritems():
            if ftype == 'number':
                try:
                    df[field]= df[field].astype(str)
                except:
                    df[field]= df.apply(lambda row: self.castAscii(row,field),axis=1)
                
                df[field] = df[field].replace({'[a-zA-Z%]': 0}, regex=True)
                try: 
                    df[field] = df[field].fillna(value=0)
                    df[field] = df[field].astype(int)
                except:
                    df[field] = df[field].fillna(value=0.0)
                    df[field] = df[field].apply(pd.to_numeric, errors='coerce').fillna(value=0.0)
            elif ftype == 'text':
                df[field] = df[field].fillna(value="")
                try:
                    df[field]= df[field].astype(str)
                except:
                    df[field]= df.apply(lambda row: self.castAscii(row,field),axis=1)
            elif ftype == 'checkbox':
                #df[field] = df[field].replace({'False': False}, regex=True)
                #df[field] = df[field].replace({'True': True}, regex=True)
                df[field] = df[field].fillna(value=False)
                #df[field]= df[field].astype(bool)
            #elif ftype == 'date':
               # df[field] = df[field].fillna(value="")
                #df[field] = pd.to_datetime(df[field], errors='coerce' , format='%Y%m%d')
                #df[field]= df[field].astype('datetime64[ns]')
        return df
    
    @staticmethod
    def castAscii(row, field):
        item =  row[field].encode('ascii','backslashreplace')
        return item
    
    @staticmethod
    def removeNewLines(df):
        return df.replace({'\n': ''}, regex=True)
    
    def cleanDataSetDict(self, dataset_dict, tables):
        for df_key,df_val in dataset_dict.iteritems():
            cleaned_df = self.cleanDataSet( dataset_dict, df_key, tables)
            dataset_dict[df_key] = cleaned_df
        return dataset_dict

    @staticmethod
    def flatten_json(row, field):
        jsonField = row[field]
        allItems = []
        for item in jsonField:
            item_kv = []
            for k,v in item.iteritems():
                try:
                    if str(v) == "":
                        v = None
                    item_str = str(k) + ":" + str(v)
                except:
                    item_str = k.encode('ascii','backslashreplace')
                item_kv.append(item_str)
            line_item =  ",".join(item_kv)
            allItems.append(line_item)
        allItems = "|".join(allItems)
        return allItems
    
    def checkForListColumns(self, tables, df_dict, dfname):
        table = tables[tables['df_name'] == dfname]
        list_columns = table[['list_columns']]
        list_columns = list_columns.iloc[0]['list_columns']
        df = df_dict[dfname]
        if list_columns != 0:
            list_columns = list_columns.split(":")
            #df = df_dict[dfname]
            for col in list_columns:
                #print col
                if(not(col == 'gifts' or col == 'realProperties')):
                    df[col] = df.apply(lambda row: self.flatten_json(row, col), axis=1)
            itemToExplode = False
            if "gifts" in list_columns:
                itemToExplode = 'gifts'
            if "realProperties" in list_columns:
                itemToExplode = 'realProperties'
            if itemToExplode:
                df = self.explodeGiftsAndProperties(df, itemToExplode)
        return df
    
    def joinFilerToSchedule(self, schedule_data, cover_data_df):
        filier_cols = ['filingId', 'filerName', 'departmentName', 'positionName', 'offices', 'periodStart', 'periodEnd', 'filingDate']
        df_filer_info = cover_data_df[filier_cols]
        schedules = schedule_data.keys()
        for schedule in schedules:
            schedule_data[schedule] = pd.merge(schedule_data[schedule],df_filer_info , on='filingId', how='left')
        return schedule_data
    
    @staticmethod
    def explodeGiftsAndProperties(df, field):
        def renameRealPropertyCols(df):
            dfcolsOrig = list(df.columns)
            dfcols = [  col[0].upper()+ col[1:]  for col in dfcolsOrig ]
            dfcolPrefix = ["realProperty" + col for col in dfcols]
            dfcolsFinal = dict(zip(dfcolsOrig,dfcolPrefix))
            df=df.rename(columns = dfcolsFinal)
            return df
        def blowupGifts(row, field, idx):
            newdf = None
            newdfList = list(row[field])
            if len(newdfList[0]) > 0:
                newdf = json_normalize(newdfList[0])
                if field == "realProperties":
                    newdf = renameRealPropertyCols(newdf)
                newdf['index_col'] = idx
            return newdf
        df['index_col'] = df.index
        dfListItems = pd.DataFrame()
        #print len(dfListItems)
        rowsidx = list(df['index_col'])
        for idx in rowsidx:
            row = df[ df['index_col'] == idx]
            rowdf = blowupGifts(row, field, idx)
            dfListItems = dfListItems.append(rowdf)
        df = df.merge(dfListItems, how='left', on='index_col')
        del df['index_col']
        del df[field]
        return df


# In[154]:

class SocrataCreateInsertUpdateForm700Data:
    '''
    creates dataset on socrata or inserts it if table exists
    '''
    def __init__(self,  configItems, client=None):
        self.client = client
        self.schema_dir = configItems['schema_dir']
        self.tables = self.setTableInfo()
        self.dataset_base_url = configItems['dataset_base_url']
        self.chunkSize = 1000
    
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
        fieldnames =  [ field.replace(".", "") for field in fieldnames]
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
        socrata_dataset['tags'] = [table['tags']]
        socrata_dataset['category'] = table['category']
        socrata_dataset['dataset_name'] = table['dataset_name']
        socrata_dataset['FourByFour'] = table['FourByFour']
        socrata_dataset['redacted'] = table['redacted']
        return socrata_dataset
    
    def createDataSet(self, dfname):
        dataset = self.makeDataSet(dfname)
        if dataset['FourByFour']== 0:
            try:
                socrata_dataset = self.client.create(dataset['dataset_name'], description=dataset['description'], columns=dataset['columns'], category=dataset['category'], tags=dataset['tags'], new_backend=False)
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
        rejectedChunks = []
        #keep track of the rows we are inserting
        dataset['rowsInserted'] = 0
        dataset['totalRecords'] = 0
        print "dataset: " + dfname + ": " + dataset['FourByFour']
        ##need to rename all the columns to fit socrata- need to use titlize
        fieldnames = list(dataset_dict[dfname].columns)
        fieldnamesRemoveDots =  [ field.replace(".", "") for field in fieldnames]
        df_fields = [ inflection.underscore(field) for field in fieldnamesRemoveDots]
        columndict = dict(zip(fieldnames,df_fields ))
        dataset_dict[dfname] = dataset_dict[dfname].rename(columns=columndict)
        #fill in all NAs just to be safe
        dataset_dict[dfname] = dataset_dict[dfname].fillna("")
        
        try:
            insertDataSet = dataset_dict[dfname].to_dict('records')
            dataset['totalRecords'] = len(insertDataSet)
        except:
            print 'Error: could not get data'
            return dataset
        
        insertChunks = self.makeChunks(insertDataSet)
        #overwrite the dataset on the first insert chunk[0]
        if dataset['rowsInserted'] == 0:
            rejectedChunk = self.replaceDataSet(dataset, insertChunks[0])
            if len(insertChunks) > 1:
                for chunk in insertChunks[1:]:
                    rejectedChunk = self.insertData(dataset, chunk)
        else:
            for chunk in insertChunk:
                rejectedChunk = self.insertData(dataset, chunk)
        return dataset
    
    @retry( tries=10, delay=1, backoff=2)
    def replaceDataSet(self, dataset, chunk):
        result = self.client.replace( dataset['FourByFour'], chunk ) 
        dataset['rowsInserted'] = dataset['rowsInserted'] + int(result['Rows Created'])
        time.sleep(0.25)
        
        
    @retry( tries=10, delay=1, backoff=2)
    def insertData(self, dataset, chunk):
        result = self.client.upsert(dataset['FourByFour'], chunk) 
        dataset['rowsInserted'] = dataset['rowsInserted'] + int(result['Rows Created'])
        time.sleep(0.25)
       

    def makeChunks(self, insertDataSet):
        return [insertDataSet[x:x+ self.chunkSize] for x in xrange(0, len(insertDataSet), self.chunkSize)]
    
    
    def postDataToSocrata(self, dfname, dataset_dict ):
        dataset = self.createDataSet(dfname)
        if dataset['FourByFour']!= 0:
            dataset = self.insertDataSet( dataset, dataset_dict, dfname)
        else: 
            print "dataset does not exist"
        return dataset


# In[155]:

class emailer():
    '''
    util class to email stuff to people.
    '''
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
	print self.inputdir
	self.configItems = configItems
        self.emailConfigs = self.getEmailerConfigs()
        print self.emailConfigs
        
    def getEmailerConfigs(self):
        emailConfigFile = self.inputdir + self.configItems['email_config_fname']
        with open(emailConfigFile,  'r') as stream:
            try:
                email_items = yaml.load(stream)
                return email_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
    def setConfigs(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.server = self.emailConfigs['server_addr']
        self.server_port = self.emailConfigs['server_port']
        self.address =  self.emailConfigs['email_addr']
        #self.password = base64.b64decode(self.emailConfigs['email_pass'])
        self.msgBody = msgBody
        self.subjectLine = subject_line
        self.fname_attachment = fname_attachment
        self.fname_attachment_fullpath = fname_attachment_fullpath
        self.recipients = self.emailConfigs['receipients']
        self.recipients =  self.recipients.split(",")
    
    def getEmailConfigs(self):
        return self.emailConfigs
    
    def sendEmails(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.setConfigs(subject_line, msgBody, fname_attachment, fname_attachment_fullpath)
        fromaddr = self.address
        toaddr = self.recipients
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = ", ".join(toaddr)
        msg['Subject'] = self.subjectLine
        body = self.msgBody 
        msg.attach(MIMEText(body, 'plain'))
          
        #Optional Email Attachment:
        if(not(self.fname_attachment is None and self.fname_attachment_fullpath is None)):
            filename = self.fname_attachment
            attachment = open(self.fname_attachment_fullpath, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
        
        #normal emails, no attachment
        server = smtplib.SMTP(self.server, self.server_port)
        #server.starttls()
        #server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()


# In[156]:

class logETLLoad:
    '''
    util class to get job status- aka check to make sure that records were inserted; also emails results to receipients
    '''
    def __init__(self, inputdir, configItems):
        self.keysToRemove = ['columns', 'tags']
        self.log_dir = configItems['log_dir']
        self.dataset_base_url = configItems['dataset_base_url']
        self.failure =  False
        self.job_name = configItems['job_name']
        self.logfile_fname = self.job_name + ".csv"
        self.logfile_fullpath = self.log_dir + self.job_name + ".csv"
        self.configItems =  configItems
        self.inputdir = inputdir
        
    def removeKeys(self, dataset):
        for key in self.keysToRemove:
            try:
                remove_columns = dataset.pop(key, None)
            except:
                noKey = True
        return dataset
    
    def sucessStatus(self, dataset):
        dataset = self.removeKeys(dataset)
        if dataset['rowsInserted'] == dataset['totalRecords']:
            dataset['jobStatus'] = "SUCCESS"
        else: 
            dataset['jobStatus'] = "FAILURE"
            self.failure =  True
        return dataset
    
    def makeJobStatusAttachment(self,  finishedDataSets ):
        with open(self.logfile_fullpath, 'w') as csvfile:
            fieldnames = finishedDataSets[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dataset in finishedDataSets:
                writer.writerow(dataset)

    def getJobStatus(self):
        if self.failure: 
            return  "FAILED: " + self.job_name
        else: 
            return  "SUCCESS: " + self.job_name

    def makeJobStatusMsg( self,  dataset  ):
        msg = dataset['jobStatus'] + ": " + dataset['dataset_name'] + "-> Total Rows:" + str(dataset['totalRecords']) + ", Rows Inserted: " + str(dataset['rowsInserted'])  + ", Link: "  + self.dataset_base_url + dataset['FourByFour'] + " \n\n " 
        return msg
    
    def sendJobStatusEmail(self, finishedDataSets):
        msgBody  = "" 
        for i in range(len(finishedDataSets)):
            #remove the column definitions, check if records where inserted
            dataset = self.sucessStatus( self.removeKeys(finishedDataSets[i]))
            msg = self.makeJobStatusMsg( dataset  )
            msgBody  = msgBody  + msg
        subject_line = self.getJobStatus()
        email_attachment = self.makeJobStatusAttachment(finishedDataSets)
        e = emailer(self.inputdir, self.configItems)
        emailconfigs = e.getEmailConfigs()
        if os.path.isfile(self.logfile_fullpath):
            e.sendEmails( subject_line, msgBody, self.logfile_fname, self.logfile_fullpath)
        else:
            e.sendEmails( subject_line, msgBody)
        print "****************JOB STATUS******************"
        print subject_line
        print "Email Sent!"


# In[157]:

#needed to create schema csv files 
#cds= createDataSets(config_dir, configItems)
#schemas = cds.makeSchemaOutFiles(schedule_data,cover_data, schedules )


# In[158]:

def getDataAndUpload(finishedDataSets, isRedacted=False):
    #get the data
    cover_data = f700.getCoverData(isRedacted)
    schedule_data = f700.getScheduleData(isRedacted) 
    #join cover data to schedules 
    if isRedacted:
        cover_key = 'cover_redacted'
    else:
        cover_key = 'cover'
    schedule_data = dsp.joinFilerToSchedule(schedule_data, cover_data[cover_key])
    #clean the cover data + schedule datasets 
    cover_data[cover_key] = dsp.cleanDataSet(cover_data,  cover_key, tables)
    schedule_data = dsp.cleanDataSetDict(schedule_data, tables)
    schedule_list = schedule_data.keys()
    #post the redacted data
    dataset = scICU.postDataToSocrata(cover_key, cover_data)
    finishedDataSets.append(dataset)
    for schedule in schedule_list: 
        dataset = scICU.postDataToSocrata(schedule, schedule_data)
        finishedDataSets.append(dataset)
    return finishedDataSets


# In[159]:

inputdir = "/home/ubuntu/form700/configFiles/"
fieldConfigFile = 'fieldConfig.yaml'
cI =  ConfigItems(inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(inputdir, configItems)
client = sc.connectToSocrata()
#class objects
scICU = SocrataCreateInsertUpdateForm700Data(configItems,client)
dsp = dataSetPrep(configItems)
lte = logETLLoad(inputdir, configItems)
f700 = form700(configItems)
tables = scICU.getTableInfo()
tables = tables.fillna(0)
lg = pyLogger(configItems)
lg.setConfig()


# In[160]:

print "Outputting Form 700 Datasets"
print datetime.datetime.now()
print 
finishedDataSets  = []
#get the private datasets
finishedDataSets = getDataAndUpload(finishedDataSets, False)
#get the redacted datasets
finishedDataSets = getDataAndUpload(finishedDataSets, True)
msg  = lte.sendJobStatusEmail(finishedDataSets)
client.close()


# In[95]:

#if __name__ == '__main__' and '__file__' in globals():
  #  main()
