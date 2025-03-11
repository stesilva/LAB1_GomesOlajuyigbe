
import pandas as pd
import requests
import csv
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

def retrive_papers_ids(api_key, field):
    all_papers = []
    offset = 0
    limit = 1

    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={field}&limit={limit}&offset={offset}"
    headers = {"x-api-key": api_key}
    logger.info('Making API Call to fetch paper ids for field %s', field)  
    response = requests.get(url, headers=headers)
        
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
        params={'fields': 'paperId,authors,title,venue,publicationVenue,year,abstract,citationCount,fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,journal'},
        json={"ids": paper_ids},
        headers=headers
    )
    if response.status_code == 200:
        json_data = response.json()
        for idx, data in enumerate(json_data):
            for key, value in data.items():
                if isinstance(value, str):
                    json_data[idx][key] = value.replace('\r', ' ').replace('\n', ' ')
        
        return json_data
    else:
        return None
    
# Dump fetched publication details into CSV file
def get_papers_details(api_key,field):


    paper_ids = retrive_papers_ids(api_key,field)

    paper_ids = [paper['paperId'] for paper in paper_ids]

    print(paper_ids)

    if paper_ids:
        # Fetch publications data
        publications_data = retrive_paper_details(api_key,paper_ids)
    else:
        print("Failed to retrieve paper information.")
  

    # Convert JSON data to DataFrame
    df = pd.DataFrame(publications_data)
    # Write DataFrame to CSV file
    df.to_csv('data'+'/paper_' + field + '.csv', index=False, quoting=csv.QUOTE_ALL)


# Dump fetched publication details into CSV file
def retrive_data_API(api_key,fields):


    # Retrieve paper information
    for field in fields:
        get_papers_details(api_key, field)





