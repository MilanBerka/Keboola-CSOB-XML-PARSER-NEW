import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'pydrive'])

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from io import BytesIO
import os 
import zipfile 
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import glob
from keboola import docker

""" =========================== """
"""     PARSER DEFINITON        """
""" =========================== """

# Transactions parser
def return_transactions_df(root):
    """ Takes in root of a CSOB disbursement XML file and finds transactions,
    then returns them as pandas DataFrame """
    
    transactions_list = [] # keeps the dictionaries, where one dictionary is one row
    # Iterate through the XML file
    for merchant in root.iter('merchant'):
        merchant_header_dict = {}               # keeps the info about the merchant (in merchant header)
        # Get information from the `merchant_header`
        merchant_header = merchant.findall('merchant_header')
        for merchant_header_cells in merchant_header[0].findall('./*'):
            merchant_header_dict['merchant_'+merchant_header_cells.tag] = merchant_header_cells.text
        
        # Get information from the `transaction`s
        for merchant_transaction in merchant.iter('transaction'):
            merchant_transaction_dict = {}
            for merchant_transaction_cell in merchant_transaction.findall('./*'): 
                merchant_transaction_dict['transaction_'+merchant_transaction_cell.tag] = merchant_transaction_cell.text
            
            # append one row to the final dataframe, if the row is not empty (i.e. there is at least one transaction)
            if not merchant_transaction_dict:
                pass
            else:
                transactions_list.append({**merchant_header_dict,**merchant_transaction_dict})
    return pd.DataFrame(transactions_list) 

# Firm totals parser
def return_firmtotals_df(root):
    """ Takes in root of a CSOB disbursement XML file and finds firm totals,
    which are returned as a pandas DataFrame """
    firm_total_df_list = []
    for firm_totals in root.iter('firm_total'):
        firm_totals_dict = {}
        for firm_totals_cells in firm_totals.findall('./*'):
            firm_totals_dict[firm_totals_cells.tag] = firm_totals_cells.text
        firm_total_df_list.append(firm_totals_dict)
    return pd.DataFrame(firm_total_df_list)    

""" =========================== """
"""    GOOGLE API CONNECTION    """
""" =========================== """

if __name__ == '__main__': 
    
    """ =========================== """    
    """ LOAD ALREADY PROCESSED DATA """
    """ =========================== """   
    try:
        alreadyProcessedZipfiles = pd.read_csv('in/tables/alreadyProcessedZipFiles.csv')
    except: 
        alreadyProcessedZipfiles = pd.DataFrame({'name':[]})
    
    """ =========================== """
    """ KEBOOLA STUFF """
    """ =========================== """
    
    cfg = docker.Config()
    parameters = cfg.get_parameters()
    folderNames = parameters.get('folderNames')
    gauth = GoogleAuth(settings_file='/data/in/files/253425786_settings.yaml')
    drive = GoogleDrive(gauth)
    
    """ =========================== """
    """      FILL THE DATAFRAME     """
    """ =========================== """
        
    finalDataFrame = None               # LEGACY: for storing transaction data
    finalFirmTotalsDataFrame = None     # NEW: for storing firm totals (unfortunate naming, technical debt for now)
        
    if folderNames:
        FOLDERS_TO_LOOKAT = list(folderNames)
    else:
        FOLDERS_TO_LOOKAT = ['CSOB AM 2016','CSOB AM 2017'] 
       
    for folderToLookAt in FOLDERS_TO_LOOKAT:
        """ SCAN THE `GDrive` FOLDERS FOR ZIPFILES """
        driveFilesList = drive.ListFile({'q':"mimeType='application/vnd.google-apps.folder' and title='{}' and trashed=false".format(folderToLookAt)}).GetList()                        
        folderId = driveFilesList[0]['id']
        zipfilesInFolder = drive.ListFile({'q':"'{}' in parents".format(folderId)}).GetList()
        for zf in zipfilesInFolder:
            if ('zip' in zf['title'].lower()) & (zf['title'] not in alreadyProcessedZipfiles['name'].tolist()) :
                print('title: {}'.format(zf['title']))
                alreadyProcessedZipfiles = alreadyProcessedZipfiles.append(pd.DataFrame([{'name':zf['title']}]))
                toUnzip = drive.CreateFile({'id':zf['id']})
                toUnzipStringContent = toUnzip.GetContentString(encoding='cp862')
                toUnzipBytesContent = BytesIO(toUnzipStringContent.encode('cp862'))
                readZipfile = zipfile.ZipFile(toUnzipBytesContent, "r")
                for fileInZipfileName in readZipfile.namelist():
                    if '.xml' in fileInZipfileName.lower():
                        if ('-t' in fileInZipfileName.lower()) | ('-m' in fileInZipfileName.lower()):
                            pass
                        else:
                            openedXml = readZipfile.open(fileInZipfileName).read()
                            loadedXml = ET.fromstring(openedXml.decode())
                            firmHeaderDate = loadedXml.find('firm_header').find('date').text
                            transactionDataFrame = return_transactions_df(loadedXml)  
                            firmtotalsDataFrame = return_firmtotals_df(loadedXml)
                            #firmtotalsDataFrame['date'] = '/'.join(transactionDataFrame['transaction_date'].unique()) # join: in case of faulty multiple dates in one file
                            firmtotalsDataFrame['date'] = firmHeaderDate
                            firmtotalsDataFrame['googleDriveFolderName'] = zf['title']
                            # As we are forcycling, we either start with None dataframe or we add newly extracted transactions/totals to alrady existing dataframe
                            if finalDataFrame is not None:
                                finalDataFrame = pd.concat([finalDataFrame.copy(),transactionDataFrame.copy()])
                                finalFirmTotalsDataFrame = pd.concat([finalFirmTotalsDataFrame.copy(),firmtotalsDataFrame.copy()])
                            else:
                                finalDataFrame = transactionDataFrame.copy()
                                finalFirmTotalsDataFrame = firmtotalsDataFrame.copy()
                                
                    else:
                        pass              
            else:
                pass
            
    """ =============================== """
    """  FINAL IMPROVEMENTS AND EXPORT  """
    """ =============================== """
    # Remove duplicates 
    finalDataFrame.drop_duplicates(subset=['merchant_account_currency', 'merchant_bank_account',
       'merchant_bank_code', 'merchant_firm_identificator',
       'merchant_merchant_id', 'merchant_merchant_name',
       'merchant_transaction_currency', 'merchant_type', 'transaction_AF',
       'transaction_IF', 'transaction_auth_code', 'transaction_brutto_CRDB',
       'transaction_brutto_account_currency',
       'transaction_brutto_transaction_currency', 'transaction_card_number',
       'transaction_cashback', 'transaction_cashback_CRDB', 'transaction_date',
       'transaction_fee', 'transaction_invoice_number', 'transaction_netto',
       'transaction_netto_CRDB', 'transaction_terminal_id', 'transaction_time',
       'transaction_type', 'transaction_variable_symbol'], inplace=True)
    finalFirmTotalsDataFrame.drop_duplicates(inplace=True)
    
    # Export
    finalDataFrame.to_csv('out/tables/parsedBatch.csv',index=None)
    finalFirmTotalsDataFrame.to_csv('out/tables/firmTotals.csv',index=None)
    alreadyProcessedZipfiles.to_csv('out/tables/alreadyProcessedZipfiles.csv',index=None)
