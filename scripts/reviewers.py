# Assign Reviewers and extract reviewer relationsip

import pandas as pd
import random
import ast

df = pd.read_csv('transformed_data.csv', sep=';')

# Function to assign reviewers to papers
# Create a list of all authors
all_authors = []
all_names = {}  # Using a dictionary for author ID to name mapping

# Extract author IDs and names
for i, row in df.iterrows():
    # Convert string representation to list
    if isinstance(row['paperAuthorID'], str):
        if '[' in row['paperAuthorID']:
            author_ids = ast.literal_eval(row['paperAuthorID'])
        else:
            author_ids = row['paperAuthorID'].split(',')
    else:
        author_ids = [row['paperAuthorID']] 
        
    # Handle different formats of author names
    if isinstance(row['paperAuthorName'], str):
        if '[' in row['paperAuthorName']:
            author_names = ast.literal_eval(row['paperAuthorName'])
        else:
            author_names = row['paperAuthorName'].split(',')
    else:
        author_names = [row['paperAuthorName']]  
    
    # Add authors to the collections
    for j in range(len(author_ids)):
        author_id = author_ids[j]
        author_name = author_names[j]        
        if author_id not in all_authors:
            all_authors.append(author_id)
            all_names[author_id] = author_name

# Create dataframe to store assignments
results = pd.DataFrame(columns=['paperDOI', 'paperTitle', 'paperAuthorID', 'paperAuthorName', 'reviewerIDs', 'reviewerNames'])

# For each paper, assign 3 reviewers
for i, row in df.iterrows():
    paper_id = row['paperDOI']
    paper_title = row['paperTitle']
    
    # Get original author IDs and names for the paper
    original_author_ids = row['paperAuthorID']
    original_author_names = row['paperAuthorName']
    
    # Get this paper's authors for filtering, handling different formats
    if isinstance(row['paperAuthorID'], str):
        if '[' in row['paperAuthorID']:
            paper_authors = ast.literal_eval(row['paperAuthorID'])
        else:
            paper_authors = row['paperAuthorID'].split(',')
    else:
        paper_authors = [row['paperAuthorID']]  
    
    # Find eligible reviewers (anyone who is not an author of this paper)
    eligible_reviewers = [author for author in all_authors if author not in paper_authors]
    num_reviewers = 3
        
    # Select random reviewers
    if eligible_reviewers:
        reviewers = random.sample(eligible_reviewers, num_reviewers)
        reviewer_names = [all_names.get(r, f"Unknown-{r}") for r in reviewers]
        
        # Add to results
        results.loc[i] = [paper_id, paper_title, original_author_ids, original_author_names, reviewers, reviewer_names]
    else:
        print(f"No eligible reviewers for paper {paper_id}")
        
# save results
results.to_csv('paper_reviewers.csv', index=False)
print("reviewers saved to 'paper_reviewers.csv'")

# 7. Extract Reviewers
df1 = pd.read_csv('paper_reviewers.csv')
reviewer_paper_relations = []

for idx, row in df1.iterrows():
    paper_doi = row['paperDOI']
    if isinstance(row['reviewerIDs'], str):
        reviewers = ast.literal_eval(row['reviewerIDs'])
        
        for reviewer in reviewers:
            reviewer_paper_relations.append({'paperDOI': paper_doi, 'authorID': reviewer})

# Create dataframe
reviewer_paper_df = pd.DataFrame(reviewer_paper_relations)
reviewer_paper_df.to_csv('neo4j_import/reviewer_paper_relations.csv', index=False)
print("reviewer relationship extracted")