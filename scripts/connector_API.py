
import pandas as pd
import requests
import csv
import logging

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger()

#Retrive paper IDs from API by field 
def retrive_papers_ids(api_key, field):
    all_papers = []
    offset = 0
    limit = 50 #limit for 50 paper for each field

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
        #Define what fields should return
        params={'fields': 'paperId,authors,title,venue,publicationVenue,year,abstract,citationCount,fieldsOfStudy,publicationTypes,publicationDate,journal,references'},
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
    
def get_papers_details(api_key, field):
    try:
        #First retrieve paper IDs from each field
        paper_ids = retrive_papers_ids(api_key, field)
        paper_ids = list(set([paper['paperId'] for paper in paper_ids]))  #Remove duplicates

        #If paper ids are retrived then retrive each paper details
        if paper_ids:
            logger.info(f'Retrieving paper details for field {field}')
            publications_data = retrive_paper_details(api_key, paper_ids)

            reference_paper_ids = set()

            #For each paper, retrive the details for the reference papers as well
            if publications_data is not None:
                for paper in publications_data:
                    if 'references' in paper:
                        #Get up to 10 valid (not null) references for each paper
                        valid_refs = [ref['paperId'] for ref in paper['references'][:10] 
                                    if 'paperId' in ref and ref['paperId'] is not None]
                        reference_paper_ids.update(valid_refs)

                
            logger.info(f'Retrieving reference paper details for field {field}')

            #Ensure reference_paper_ids is unique
            reference_paper_ids = list(set(reference_paper_ids))

            reference_publication_data = []

            #Process the references in batches (100 IDs at a time)
            batch_size = 100
            for i in range(0, len(reference_paper_ids), batch_size):
                #Get the current batch of IDs
                batch = reference_paper_ids[i:i + batch_size]
                
                #Retrieve paper details for the current batch
                logger.info(f'Retrieving batch {i // batch_size + 1} of reference papers')
                batch_data = retrive_paper_details(api_key, batch)
                
                if batch_data is not None:
                    #Add the retrived publication data
                    reference_publication_data.extend(batch_data)

            #Combine publications data and reference publication data into a DataFrame
            df = pd.DataFrame(publications_data + reference_publication_data)

            output_path = f'data/raw_data/paper_{field}.csv'
            df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)

            logger.info(f'Data saved to {output_path}')
        else:
            logger.warning(f"No paper IDs found for field {field}")
    except Exception as e:
        logger.error(f"An error occurred while processing field {field}: {str(e)}")

#Retrive raw data from the API
def retrive_data_API(api_key,fields):

    #Retrieve paper details for each field
    for field in fields:
        get_papers_details(api_key, field)





