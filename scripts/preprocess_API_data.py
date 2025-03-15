
import pandas as pd
import os
import logging
import ast  
import json
import random
import string
from collections import Counter
import re

#Set logger configurations
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger()


#Predefined lists of fake data for generating random inputs
first_names = ["John", "Jane", "Alex", "Emily", "Chris", "Sarah", "Michael", "Jessica", "David", "Sophia"]
last_names = ["Smith", "Johnson", "Brown", "Williams", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Hernandez", "Silva", "Gomes"]
editions = list(range(1, 21))  # 1 to 20
cities = ["Barcelona", "Sao Paulo", "Abuja","New York", "London", "Paris", "Tokyo", "Sydney", "Berlin", "Beijing", "Los Angeles", "Chicago", "Houston"]
adjectives = ["Annual", "International", "Global", "Regional", "National"]
event_types = ["Conference", "Symposium", "Workshop", "Summit"]
publication_types = ["Journal", "Review", "Magazine"]
field_keywords = [
    "Analytics", "Data Mining", "SQL",
    "Threat Detection", "Intrusion Prevention", "Encryption", "Firewall", "Malware", "Phishing", "Vulnerability Assessment",
    "Machine Learning", "Neural Networks", "Deep Learning", "Natural Language Processing", "Computer Vision", "Predictive Analytics",
    "Infrastructure-as-a-Service", "Platform-as-a-Service", "Software-as-a-Service", "Virtualization", "Scalability", "Multi-cloud",
    "Network Analysis", "Graph Database", "Centrality", "Clustering", "Pathfinding", "Graph", "Big Data", "Cybersecurity", "Artificial Intelligence", "Cloud"
]
abstract_default = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exerreference ullamco laboris nisi ut aliquip ex ea commodo consequat"

def generate_random_name():
    #Generate a random name using predefined first and last names
    return f"{random.choice(first_names)} {random.choice(last_names)}"

#Function to evaluate if value can be iterated
def safe_eval(value, default=None):
    if pd.isna(value) or value == '':
        return default
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
        
def generate_fake_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

def determine_publication_type(row):

    pub_type = ''

    publication_venue = safe_eval(row.get("publicationVenue"), {})
    pub_type = publication_venue.get("type","")
    pub_type.lower()

    if "journal" in pub_type:
        return "journal"
    elif "conference" in pub_type or "workshop" in pub_type:
        return "conference"
    else:
        return "journal"  # Default to journal if not specified
    
def extract_topic(row):
    fields_of_study = safe_eval(row.get("fieldsOfStudy", "[]"))
    return fields_of_study[0] if fields_of_study else "Science"

def get_year(row):
    year = row.get("year", "")
    try:
        return int(year)
    except ValueError:
        return random.randint(2000, 2025)

def generate_venue_name(publication_type, topic, year):
    if publication_type == "conference" or publication_type == "workshop":
        return f"{random.choice(adjectives)} {random.choice(event_types)} on {topic} ({year})"
    elif publication_type == "journal":
        return f"{topic} {random.choice(publication_types)} ({year})"
    else:
        # Default to journal if publication type is missing
        return f"{topic} {random.choice(publication_types)} ({year})"

def paperDOI_format(row):
    return row.get("paperId", "")

def paperTitle_format(row):
    return row.get("title", "")

def paperAbstract_format(row):
    abstract = row.get("abstract", "")

    #Ensure the value is a string and remove leading/trailing whitespace
    if isinstance(abstract, str):  #Check if it's a string
        #Remove leading and trailing whitespace
        abstract = abstract.strip()
        
        #Split the abstract into words
        words = abstract.split()
        
        #Keep only the first 150 words
        abstract = " ".join(words[:150])
    else:
        abstract = abstract_default   
    return abstract

def paperAuthorID_format(row):
    authors = safe_eval(row.get("authors"), [])
    if not authors:
        #Generate a fake numerical ID if there are no authors
        return [str(random.randint(1000000, 9999999))]
    return [author.get("authorId", "") for author in authors]

def paperAuthorName_format(row):
    authors = safe_eval(row.get("authors"), [])
    if not authors:
        #Generate a fake name if there are no authors
        return [generate_random_name()]  

    return [author.get("name", "") for author in authors]

def conferenceWorkshopID_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "conference":
        venue = safe_eval(row.get("publicationVenue"), {})
        return venue.get("id", generate_fake_id())
    return ""

def conferenceWorkshopName_format(row):
    pub_type = determine_publication_type(row)
    topic = extract_topic(row)
    year = get_year(row)
    
    if pub_type == "conference" or pub_type == "workshop":
        venue = safe_eval(row.get("publicationVenue"), {})
        if venue.get("name"):
            return venue["name"]
        return generate_venue_name(pub_type, topic, year)
    return ""

def conferenceWorkshopType_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "conference":
        venue = safe_eval(row.get("publicationVenue"), {})
        return venue.get("type",  "workshop")
    return ""

def conferenceWorkshopEdition_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "conference":
        return random.choice(editions)
    return ""

def conferenceWorkshopYear_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "conference":
        year = row.get("year", "")
        try:
            return int(year)
        except ValueError:
            return random.randint(2000, 2025)
    return ""

def conferenceWorkshopCity_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "conference":
        return random.choice(cities)
    return ""

def journalID_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "journal":
        journal = safe_eval(row.get("journal"), {})
        return journal.get("id", generate_fake_id())
    return ""

def journalName_format(row):
    pub_type = determine_publication_type(row)
    topic = extract_topic(row)
    year = get_year(row)

    if pub_type == "journal":
        journal = safe_eval(row.get("journal"), {})
        if journal.get("name"):
            return journal["name"]
        return generate_venue_name(pub_type, topic, year)
    return ""

def jornalYear_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "journal":
        year = row.get("year", "")
        try:
            return int(year)
        except ValueError:
            return random.randint(2000, 2025)
    return ""

def jornalVolume_format(row):
    pub_type = determine_publication_type(row)
    if pub_type == "journal":
        journal = safe_eval(row.get("journal"), {})
        volume = journal.get("volume", str(random.randint(1, 100)))
        try:
            return int(volume)
        except ValueError:
            return 1
    return ""

def keywords_format(row):
    # Get existing keywords from fieldsOfStudy
    base_keywords = safe_eval(row.get("fieldsOfStudy"), [])
    
    # Extract additional keywords from the article title
    title = row.get("title", "").lower()
    title_keywords = []
    
    if title:
        #Check if any predefined keyword is in the title
         for keyword in field_keywords:
            if keyword.lower() in title:
                title_keywords.append(keyword)
        
    #Combine base, predefined, and additional keywords
    combined_keywords = list(set(base_keywords + title_keywords))

    if not combined_keywords:
        combined_keywords = ["Science"]
    
    return combined_keywords

def references_format(row, valid_paper_ids):
    # Safely evaluate the "references" field and extract references
    publication_references = safe_eval(row.get("references"), {})
    references_ids = [reference.get("paperId", "") for reference in publication_references]
    
    # Filter references based on valid_paper_ids
    if isinstance(references_ids, list):
        filtered_references = [cid for cid in references_ids if cid in valid_paper_ids]
    elif isinstance(references_ids, str):
        filtered_references = [references_ids] if references_ids in valid_paper_ids else []
    else:
        filtered_references = []
    
    # Count the number of valid references
    reference_count = len(filtered_references)
    
    return filtered_references, reference_count


def preprocess_data():

    #Make adjustments for each needed field
    logger.info('Preprocessing data from API')

    #Specify the directory containing the CSV files with raw data
    directory = 'data/raw_data'

    #Create an empty dataframe(df) to hold the combined data
    combined_data = pd.DataFrame()

    #Iterate through all files in the directory
    for file in os.listdir(directory):
        if file.endswith('.csv') and file.startswith('paper_'):
            file_path = os.path.join(directory, file)
            #Read each CSV file and append its data to the combined df
            data = pd.read_csv(file_path)
            combined_data = pd.concat([combined_data, data], ignore_index=True) 

    #Remove duplicate rows based on the 'paperID' attribute
    combined_data = combined_data.drop_duplicates(subset='paperId', keep='first')

    #Retrive list of valid paper IDs:
    valid_paper_ids = combined_data['paperId'].tolist()

    #Create new df for storing preprocessed data
    processed_data = []
    for _, row in combined_data.iterrows():
        filtered_references, reference_count = references_format(row, valid_paper_ids)
        processed_data.append({
            "paperDOI": paperDOI_format(row),
            "paperTitle": paperTitle_format(row),
            "paperAbstract": paperAbstract_format(row),
            "paperAuthorID": paperAuthorID_format(row),
            "paperAuthorName": paperAuthorName_format(row),
            "conferenceWorkshopID": conferenceWorkshopID_format(row),
            "conferenceWorkshopName": conferenceWorkshopName_format(row),
            "conferenceWorkshopType": conferenceWorkshopType_format(row),
            "conferenceWorkshopEdition": conferenceWorkshopEdition_format(row),
            "conferenceWorkshopYear": conferenceWorkshopYear_format(row),
            "conferenceWorkshopCity": conferenceWorkshopCity_format(row),
            "journalID": journalID_format(row),
            "journalName": journalName_format(row),
            "jornalYear": jornalYear_format(row),
            "jornalVolume": jornalVolume_format(row),
            "keywords": keywords_format(row),
            "referenceIDs": filtered_references,
            "referenceCount": reference_count
        })

    #Convert to DataFrame and save as CSV
    result_df = pd.DataFrame(processed_data)
    result_df.to_csv("data/preprocessed_data/transformed_data.csv", sep=";", index=False)