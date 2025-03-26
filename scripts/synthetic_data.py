import pandas as pd
import random
import ast
from collections import Counter

def citation_counter(df):
    citation_counter = Counter()

    #Count the citations for each paper
    for _, row in df.iterrows():
        citations = row.get('referenceIDs', [])
        if isinstance(citations, str):
            citations = ast.literal_eval(citations)  #Convert string representation to list if necessary
        citation_counter.update(citations)

    if 'citationCount' not in df.columns:
        df['citationCount'] = 0  #Initialize with empty lists    

    #Second pass: Add citation count to each paper
    for i, row in df.iterrows():
        paper_id = row['paperDOI']
        df.at[i, 'citationCount'] = citation_counter[paper_id]

    return df

#Function to assign reviewers to papers
def reviewers(df):
    all_authors = []

    #Extract author IDs and names
    for i, row in df.iterrows():
        #Convert string representation to list
        if isinstance(row['paperAuthorID'], str):
            if '[' in row['paperAuthorID']:
                author_ids = ast.literal_eval(row['paperAuthorID'])
            else:
                author_ids = row['paperAuthorID'].split(',')
        else:
            author_ids = [row['paperAuthorID']] 
            
        #Add authors to the collections
        for j in range(len(author_ids)):
            author_id = author_ids[j]     
            if author_id not in all_authors:
                all_authors.append(author_id)

    if 'reviewerIDs' not in df.columns:
        df['reviewerIDs'] = [[] for _ in range(len(df))]  #Initialize with empty lists

    #For each paper, assign 3 reviewers
    for i, row in df.iterrows():
        paper_id = row['paperDOI']

        #Get this paper's authors for filtering, handling different formats
        if isinstance(row['paperAuthorID'], str):
            if '[' in row['paperAuthorID']:
                paper_authors = ast.literal_eval(row['paperAuthorID'])
            else:
                paper_authors = row['paperAuthorID'].split(',')
        else:
            paper_authors = [row['paperAuthorID']]  
        
        #Find eligible reviewers (anyone who is not an author of this paper)
        eligible_reviewers = [author for author in all_authors if author not in paper_authors]
        num_reviewers = 3
            
        #Select random reviewers
        if eligible_reviewers:
            reviewers = random.sample(eligible_reviewers, num_reviewers)

            df.at[i, 'reviewerIDs'] = reviewers
        else:
            print(f"No eligible reviewers for paper {paper_id}")

    return df
        

#Function to generate references and reference count
#Base reference on paper keywords, if 2 papers have 1 or more commom keywords, they are selected to cite each other
def generate_references(df):
    referenceCountSynthetic = [random.randint(3, 10) for i in range(len(df))]
    
    #Parse all keywords once to avoid repetitive parsing
    paper_keywords = []
    for i in range(len(df)):
        keywords = df.loc[i, 'keywords']
        paper_keywords.append(keywords)
    
    #Generate references for each paper that has no reference (referenceCount = 0)
    for i in range(len(df)):
        if df.loc[i, 'referenceCount'] == 0:
            #Get reference count and keywords for current paper
            count = referenceCountSynthetic[i]
            current_keywords = paper_keywords[i]
            #Find papers with similar keywords (excluding self)
            similar_papers = []
            for j in range(len(df)):
                #Skip self-reference
                if i != j:
                    #Check if any keyword matches
                    if any(keyword in paper_keywords[j] for keyword in current_keywords):
                        similar_papers.append(j)
            #If no papers with similar keywords, use all papers except self
            if not similar_papers:
                similar_papers = [j for j in range(len(df)) if j != i]
            #Adjust count if not enough similar papers
            if count > len(similar_papers):
                count = len(similar_papers)
            df.loc[i, 'referenceCount'] = count
            #Randomly select papers to cite
            if similar_papers and count > 0:
                referenced_indices = random.sample(similar_papers, count)
                referenced_dois = [df.loc[idx, 'paperDOI'] for idx in referenced_indices]
                df.at[i, 'referenceIDs'] = referenced_dois   
    return df

def synthetic_data():
    df = pd.read_csv('data/preprocessed_data/transformed_data.csv', sep= ';')

    df = generate_references(df)
    df = reviewers(df)
    df = citation_counter(df)

    #Write the updated DataFrame back to CSV
    df.to_csv('data/preprocessed_data/transformed_data.csv', sep=';', index=False)

    print("CSV file has been updated and saved as 'transformed_data.csv'")

