# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 12:49:10 2021

@author: SakshiArjun
"""


import pandas as pd 
import gspread
import argparse
import datetime
import logging
import json

#create and configur logger

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = 'log_info.log', level = logging.INFO, format = LOG_FORMAT)
logger = logging.getLogger()

def read_data_from_file(Input_File,Input_Date):
    #reading csv file to a dataframe and taking expiry date as input 
    try:
#    logprintf("reading file..%s", Input_File)
        logger.info("Reading Filename and Expiry_Date")
        df = pd.read_csv(Input_File)
        xDate = Input_Date
    
    except FileNotFoundError:
            print("The entered file does not exist or not a CSV file.Please enter the correct file ")
            logger.error("The entered file doesnot exist or not a csv file .Plz enter the correct file")
            exit()
    try:
        format = "%d-%b-%Y"
        datetime.datetime.strptime(Input_Date, format)
    
    except ValueError:
            print("This is the incorrect date string format. It should be DD-Mmm-YYYY")
            logger.error("This is the incorrect date string format. It should be DD-Mmm-YYYY")      
            exit()
    return df,xDate


def filterstks(df,xDate):
    filter_criteria = (df['INSTRUMENT'] == 'FUTSTK' ) 
    filter_date = (df['EXPIRY_DT' ] == xDate )
    #filtering data for futstk and given expiry date
    newdf = (df [ filter_criteria & filter_date])
    #print(newdf)
#copying the below columns to a newdf and renaming the column close
    finalnewdf = newdf[['INSTRUMENT','SYMBOL','EXPIRY_DT', 'CLOSE']].copy()
    finalnewdf.rename(columns={'CLOSE':'Fut Close'},inplace=True)
    #print(finalnewdf)
    #filtering data for optstk and given expiry date
    filter_criteria2 = (df['INSTRUMENT'] == 'OPTSTK' ) 
    ndf = (df [ filter_criteria2 & filter_date])
    #print(ndf)
#print(type(ndf))
#finding the maximum openinterest
    maximums=ndf.loc[ndf.groupby(["SYMBOL","OPTION_TYP"])["OPEN_INT"].idxmax()] 
    #print("maximums::")
    #print(maximums)
#filtering data frames by options ce and pe 
    cedf = maximums[maximums["OPTION_TYP"] == "CE"]
    #print(cedf)
    finalcedf = cedf[['SYMBOL','STRIKE_PR','OPEN_INT', 'CHG_IN_OI']].copy()
    finalcedf.rename(columns={'STRIKE_PR':'Highest Call Strike','OPEN_INT':'Highest Call OI','CHG_IN_OI':'Highest Call Chng in OI'},inplace=True)
    #print(finalcedf)
    result = pd.merge(finalnewdf,finalcedf,on="SYMBOL")
    #print(result)
    pedf = maximums[maximums["OPTION_TYP"] == "PE"]
    finalpedf = pedf[['SYMBOL','STRIKE_PR','OPEN_INT','CHG_IN_OI']].copy()
    finalpedf.rename(columns={'STRIKE_PR':'Highest Put Strike','OPEN_INT':'Highest Put OI','CHG_IN_OI':'Highest Put Chng in OI'},inplace=True)
    #print(finalpedf)
    allresult = pd.merge(result,finalpedf,on="SYMBOL")
    #print(allresult)
#print(type(allresult))
#calculating j ang k colums
    for ind,row in allresult.iterrows():
        allresult.loc[ind,"Highest Call % from Fut"] = round((((row['Highest Call Strike']- row['Fut Close'])/row['Highest Call Strike'])*100),2)
        allresult.loc[ind,"Highest Put % from Fut"] = round((((row['Highest Put Strike']- row['Fut Close'])/row['Highest Put Strike'])*100),2)   
   # print(allresult)
   
    if allresult.empty:
        print("There is no stock data to upload to Google Sheet" )
        logger.info("there is No data filtered to write to google sheet")
        exit()
    else:
        logger.info("stock dat avilable and will be loaded to python_If google sheet") 
        return allresult


def writetogs(allresult,work_book,sheetName,secretKey):
#writing data to google sheet
    
    gc = gspread.service_account(filename = secretKey)
#display spread sheet
    logger.info("opening a google script Master")
    sh = gc.open(work_book)
#worksheet = sh.sheet1
    worksheet=sh.worksheet(sheetName)
    worksheet.update([allresult.columns.values.tolist()] + allresult.values.tolist())
    logger.info("completed loading data to python_IF sheet")
    return


def main():
    #Taking input from commandprompt
      parser = argparse.ArgumentParser()
      parser.add_argument("--FileName",'-f', required = True,help = "name of the input file" )
      parser.add_argument("--Expiry_date",'-d',required = True,help = "enter date as dd-Mmm-yyyy" )    
      parser.add_argument("--json_file",'-c',required = True,help = "name of the json file" )
      
      try:
           args = parser.parse_args()
      except SystemExit:
             print("An argument/s missing")
             logger.info('an argument/arguments missing.please enter json file,csv file and an expiry date')
             exit()
      
      else:
           Input_File = args.FileName
           Input_Date = args.Expiry_date
           Config_File = args.json_file
       
      try:
            myjsonfile = open(Config_File,'r')
            jsondata = myjsonfile.read()
            fobj = json.loads(jsondata)
            #print(fobj)
            secretKey = fobj['secretKey']
       #log_file = fobj['log_file']
            work_book = fobj['work_book']
            sheetName = fobj['sheetName']
            #print(sheetName)
      except FileNotFoundError:
               print("The entered file does not exist or not a json file.Please enter the correct file ")
               logger.error("The entered file doesnot exist or not a json file .Plz enter the correct file")
               exit()
  
      df,xDate = read_data_from_file(Input_File,Input_Date)
      
      filter_data = filterstks(df,xDate) 
      if filter_data.empty:
          print("There is no stock data for the respective expiry date" )
          logger.info("No data filtered for the expiry date given")
    
      writetogs(filter_data,work_book,sheetName,secretKey)

if __name__ == "__main__":
    main()
