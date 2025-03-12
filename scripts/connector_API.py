
import pandas as pd
import requests
import csv
import logging

#Set logger configurations
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger()

#Retrive paper IDs from API
def retrive_papers_ids(api_key, field):
    all_papers = []
    offset = 0
    limit = 100 #limit for 100 paper for each field

    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={field}&limit={limit}&offset={offset}"
    headers = {"x-api-key": api_key}
    response = requests.get(url, headers=headers)
        
    #If sucesseful response then process data from the API    
    if response.status_code == 200:
        data = response.json()
        papers = data.get('data', [])
        all_papers.extend(papers)
    else:
        logger.error("Error: %s", response.status_code)  
        return None
    
    return all_papers


def retrive_paper_details(api_key, paper_ids):
    headers = {"x-api-key": api_key}
    response = requests.post(
        'https://api.semanticscholar.org/graph/v1/paper/batch',
        params={'fields': 'paperId,authors,title,venue,publicationVenue,year,abstract,citationCount,fieldsOfStudy,publicationTypes,publicationDate,journal'},
        json={"ids": paper_ids},
        headers=headers
    )

    #If sucesseful response then process data from the API  
    if response.status_code == 200:
        json_data = response.json()
        for idx, data in enumerate(json_data):
            for key, value in data.items():
                if isinstance(value, str):
                    #Remove line breaker chars
                    json_data[idx][key] = value.replace('\r', ' ').replace('\n', ' ')
        return json_data
    else:
        return None
    
def get_papers_details(api_key,field):
    #First retrive paper IDs from each field
    paper_ids = retrive_papers_ids(api_key,field)
    paper_ids = [paper['paperId'] for paper in paper_ids]

    if paper_ids:
        #Retrive paper data
        logger.info('Retriving paper details for field %s', field) 
        publications_data = retrive_paper_details(api_key,paper_ids)
    else:
        print("Could not retrieve paper information.")

    df = pd.DataFrame(publications_data)
    #Dump fetched paper details into CSV
    df.to_csv('data'+'/raw_data/paper_' + field + '.csv', index=False, quoting=csv.QUOTE_ALL)

#Retrive raw data from the API
def retrive_data_API(api_key,fields):

    #Retrieve paper details for each field
    for field in fields:
        get_papers_details(api_key, field)





