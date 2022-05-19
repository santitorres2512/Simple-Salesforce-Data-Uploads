from simple_salesforce import Salesforce
import requests
import pandas as pd
import csv
import json
import re
from datetime import datetime
import time



#scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
#creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json",scope)
#client = gspread.authorize(creds)
#headers = {'Accept':'application/json'}
#sheet = client.open("Account Codes for Salesforce").sheet1
#sheet.update_cells(cellsList)
#print(sheet)
#

csvfil e = open('Input_File.csv', 'r')
csvfile2 = open('Input_File.csv', 'r')

codesfile = pd.read_csv('Codes.csv')

fieldnames = ("Company_Name__c","FirstName","LastName","Title","Email","Phone","OtherPhone","MobilePhone","Lead_LinkedIn_URL__c","MailingCity","MailingState","MailingCountry","Validation_Type__c","Email_Validation_Status__c","LeadGen_Notes__c","Phone_Validation_Outcome__c","Phone_Validation_Notes__c","Lead_Direct_Number__c","Client_Account__c","List_Name__c","BDR1__c")
reader = csv.DictReader( csvfile, fieldnames)

fieldnames2 = ("Company_Name__c","FirstName","LastName","Title","Email","Phone","OtherPhone","MobilePhone","Lead_LinkedIn_URL__c","MailingCity","MailingState","MailingCountry","Validation_Type__c","Email_Validation_Status__c","LeadGen_Notes__c","Phone_Validation_Outcome__c","Phone_Validation_Notes__c","Lead_Direct_Number__c","Client_Account__c","List_Name__c","BDR1__c")
reader2 = csv.DictReader( csvfile2, fieldnames2)

out = json.dumps( [ row for row in reader ] )
jsonF = json.loads(out)

#first = out.items[0]
DataTransfer = jsonF[1:]
AccountName = pd.DataFrame(reader2)

#print(AccountName['Client_Account__c'])
Codes =  pd.DataFrame(codesfile)
#ClientCode = Codes.loc[Codes['Account Name'] == 'AppOmni']
ClientCodes = Codes['Account Name'].str.contains(str(AccountName['Client_Account__c'][2]), na=False, case=False)
#print(AccountName['Client_Account__c'][1])
#print(ClientCode['Code'])

ClientCodeDef = str(Codes[ClientCodes]['Code'])

result = re. ,ml sub('[^0-9]','', ClientCodeDef)
StringCode = str(result[2:])


FinalCode = ' - ['+StringCode+']'
print("here here --> ", FinalCode)

AccountName['Name']=AccountName['Company_Name__c']+FinalCode #+FinalCode
AccountName['Name'][0]='Name'
AccountName.drop(AccountName.index[0], inplace=True)

print("\n Account code fetched and assigned, printing Account Name list...\n")
print(AccountName['Name'])
#print(AccountName['Client_Account__c'])

AccountNameDF = AccountName[['Name','Client_Account__c']]
#AccountNameDF.reset_index(drop=True, inplace=True)
AccountNameDF.columns = ['Name','Client_AccountContacts__c']

AccountNameDF.to_csv('AccountNameObject.csv',index=False)
#print(AccountNameDF)

csvfile3 = open('AccountNameObject.csv', 'r')
fieldnames3 = ('Name','Client_AccountContacts__c')
reader3 = csv.DictReader( csvfile3, fieldnames3)
#out2=csvfile3.to_json()
field = json.dumps( [ row for row in reader3 ] )
json2 = json.loads(field)

ActualDate =datetime.today().strftime('%m-%d-%Y')


#print(DataTransfer['Client_Account__c'])
#ListNameValue = 
#print(json2)

#csvfile3 = open('AccountNameObject.csv', 'r')
#jsonfile3 = open('file.json', 'w')

#fieldnames3 = ('Name','Client_AccountContacts__c')
#reader3 = csv.DictReader( csvfile3, fieldnames3)
#for row in reader3:
#   json.dump(row, jsonfile3)
#    jsonfile.write('\n')
#print(out2)

#AccountObject = pd.DataFrame(AccountNameDF)
#out2 = AccountNameDF.to_json()
#out2 = AccountNameDF.to_json(orient='records')[1:-1].replace('},{', '} {')
#res = json.loads(AccountNameDF.to_json(orient='records'))
#print(AccountObject)

#print(out2)
#print(AccountName['Name'])
#print(AccountName['Name'])

#print(ClientCodeDef['Code'])
#AccountName['Name']=AccountName['Company_Name__c']+' - [3]'
#AccountName['Client_AccountContacts__c']=AccountName['Client_Account__c']

#print(DataTransfer)
#print(AccountName['Name'],AccountName['Client_AccountContacts__c'])

#session_id, instance = SalesforceLogin(
#    username='jgarcia@cloudtask.com.ctsandbox',
#    password='c754ndb0x2o2o',
#    security_token='gfs61nntUu6XcFbOKUe52yuH',
#    domain='test')


#sf = Salesforce(instance_url='https://cloudtask--ctsandbox.my.salesforce.com/', session_id='', security_token='00D17000000DD8c!AQkAQJIb0._onDyDoKpKNUETKAg3JrtVEa.wnILr8Us0rpWGY5ZQrFOCsFHLa9D6.OupQ5e8EVeosiltkAms7.MdjGkaWn8W')


sf = Salesforce(username='jgarcia@cloudtask.com.ctsandbox', password='Cloudtask123', security_token='Ku5fZHeHi0Vv9Kd1mUU5bUKMR', domain='test') #### AUTHENTICATION gfs61nntUu6XcFbOKUe52yuH
'''
#print('Executed here',sf)   
             
#contact = sf.Contact.get('00317000013gtsZAAQ')  

#print(contact)


#'Client_Account__c'
#'Client_Account__c':'Testerand'

data = [
      {'LastName':'JasonMomoa','Email':'jmomoa@example.com','Account':'Nestle - [1]'},
      {'LastName':'Jones','Email':'test5@test.com'}
    ]
'''
#Test = sf.bulk.Contact.insert(data,batch_size=10000,use_serial=True)

#print(Test)

#field = [
#      {'Name':'Tester - [25]'},
#      {'Name':'Testing - [25]'},
#      {'Name':'Test - [25]'}
#    ]
    

Test = sf.bulk.Account.insert(json2[1:])

print("\n Creating account objects...\n")
print(Test)
#sf.bulk.Account.insert()
#test = sf.Account.create(field)

print("\n Accounts creating, assigning Account Name IDs, to respective contacts... \n")
AccIds=[]
for AccNumber i n range (len(Test)):
    AccIds.append(Test[AccNumber]['id'])


for ConNumber in range (len(DataTransfer)):
    DataTransfer[ConNumber]["AccountId"] = str(AccIds[ConNumber])
    DataTransfer[ConNumber]["List_Name__c"] = str(DataTransfer[ConNumber]['Client_Account__c'])+' '+str(ActualDate)
    DataTransfer[ConNumber].pop('null')
    
print("\n Account IDs assigned, creating new records on Contact object...\n")
#print(DataTransfer)
#for ConNumber in range (len(DataTransfer)):
#    DataTransfer[ConNumber].append({"AccountI":AccIds[ConNumber]})                                  
#print(DataTransfer)

Test2 = sf.bulk.Contact.insert(DataTransfer,batch_size=10000,use_serial=True)

print(Test2)

lengthTest1=len(Test)
lengthTest2=len(Test2)

print(lengthTest1)
print(lengthTest2)




for pos in range (lengthTest1):
    
    print("First Name: ",  DataTransfer[pos]["FirstName"])
    print("Last Name: ",  DataTransfer[pos]["LastName"])
    print("Email: ",  DataTransfer[pos]["Email"])
    print("Client Account: ",  DataTransfer[pos]["Client_Account__c"])
    print("Company Name: ",  DataTransfer[pos]["Company_Name__c"])
    print("Job Title: ",  DataTransfer[pos]["Title"])
      
    print("Success on Account: ", Test[pos]["success"])
    print("Success on Account: ", Test[pos]["errors"])
    print("Success on Contact: ", Test2[pos]["success"])
    print("Success on Contact: ", Test2[pos]["errors"])
    



print("\n Contacts created.\n ")
