# Extract nodes and relationships

import pandas as pd
import ast
import os


def extract_data():
    # Create output directory if it doesn't exist
    os.makedirs('data/neo4j_import', exist_ok=True)

    df = pd.read_csv('data/preprocessed_data/transformed_data.csv', sep= ';')

    # 1. Extract Papers
    papers = df[['paperDOI', 'paperTitle', 'paperAbstract', 'citationCount']].copy()
    papers.rename(columns={'paperDOI': 'doi','paperTitle': 'title','paperAbstract': 'abstract','citationCount': 'citationCount'}, inplace=True)
    papers.to_csv('data/neo4j_import/papers.csv', index=False)


    # 2. Extract Authors 
    authors_list = []
    author_paper_relations = []

    for idx, row in df.iterrows():
        paper_doi = row['paperDOI']
        
        # Convert string representations of lists to actual lists
        author_ids = ast.literal_eval(row['paperAuthorID'])
        author_names = ast.literal_eval(row['paperAuthorName'])
        
        # Make sure all lists have the same length
        for i in range(len(author_ids)):
            authors_list.append({
                'authorID': author_ids[i] if author_ids[i] else '0' ,
                'name': author_names[i]
            })
            
            author_paper_relations.append({
                'paperDOI': paper_doi,
                'authorID': author_ids[i],
                'correspondingAuthor': True if i == 0 else False
            })
        
    # Create dataframes and remove duplicates
    authors_df = pd.DataFrame(authors_list).drop_duplicates(subset=['authorID'])
    authors_df.to_csv('data/neo4j_import/authors.csv', index=False)

    author_paper_df = pd.DataFrame(author_paper_relations)
    author_paper_df.to_csv('data/neo4j_import/author_paper_relations.csv', index=False)

    # 3. Extract Conferences
    conferences = df[df['conferenceWorkshopID'].notna() & (df['conferenceWorkshopID'] != '')][
        ['conferenceWorkshopID', 'conferenceWorkshopName', 'conferenceWorkshopType']].copy()
    conferences.rename(columns={'conferenceWorkshopID': 'conferenceID','conferenceWorkshopName': 'name','conferenceWorkshopType': 'type',
        }, inplace=True)
    conferences.drop_duplicates(subset=['conferenceID'], inplace=True)
    conferences.to_csv('data/neo4j_import/conferences.csv', index=False)

    # Conference-Paper relationships
    conf_paper_relations = df[df['conferenceWorkshopID'].notna() & (df['conferenceWorkshopID'] != '')][
        ['paperDOI', 'conferenceWorkshopID', 'conferenceWorkshopEdition', 'conferenceWorkshopYear', 'conferenceWorkshopCity']].copy()
    conf_paper_relations.rename(columns={
        'conferenceWorkshopID': 'conferenceID', 'conferenceWorkshopEdition': 'edition','conferenceWorkshopYear': 'year',
        'conferenceWorkshopCity': 'city'}, inplace=True)
    conf_paper_relations.to_csv('data/neo4j_import/conference_paper_relations.csv', index=False)

    # 4. Extract Journals
    journals = df[df['journalID'].notna() & (df['journalID'] != '')][['journalID', 'journalName']].copy()
    journals.rename(columns={'journalName': 'name'}, inplace=True)
    journals.drop_duplicates(subset=['journalID'], inplace=True)
    journals.to_csv('data/neo4j_import/journals.csv', index=False)

    # Journal-Paper relationships
    journal_paper_relations = df[df['journalID'].notna() & (df['journalID'] != '')][['paperDOI', 'journalID', 'jornalYear', 'jornalVolume']].copy()
    journal_paper_relations.rename(columns={'jornalYear': 'year','jornalVolume': 'volume'}, inplace=True)
    journal_paper_relations.to_csv('data/neo4j_import/journal_paper_relations.csv', index=False)

    # 5. Extract Keywords
    keywords_list = []
    keyword_paper_relations = []

    for idx, row in df.iterrows():
        paper_doi = row['paperDOI']
        if isinstance(row['keywords'], str):
            keywords = ast.literal_eval(row['keywords'])
            
            for keyword in keywords:
                keywords_list.append({'keyword': keyword})
                keyword_paper_relations.append({'paperDOI': paper_doi, 'keyword': keyword})

    # Create dataframes and remove duplicates
    keywords_df = pd.DataFrame(keywords_list).drop_duplicates(subset=['keyword'])
    keywords_df.to_csv('data/neo4j_import/keywords.csv', index=False)

    keyword_paper_df = pd.DataFrame(keyword_paper_relations)
    keyword_paper_df.to_csv('data/neo4j_import/keyword_paper_relations.csv', index=False)

    # 6. Extract Citations (Paper to Paper relationships)
    citations_list = []
    for idx, row in df.iterrows():
        paper_doi = row['paperDOI']
        if isinstance(row['referenceIDs'], str):
            cited_papers = ast.literal_eval(row['referenceIDs'])
            
            for cited_paper in cited_papers:
                citations_list.append({'paperDOI': paper_doi, 'citedPaperDOI': cited_paper})
    
    citations_df = pd.DataFrame(citations_list)
    citations_df.to_csv('data/neo4j_import/citations.csv', index=False)


    # 7. Extract Reviewers
    reviewer_paper_relations = []
    for idx, row in df.iterrows():
        paper_doi = row['paperDOI']
        if isinstance(row['reviewerIDs'], str):
            reviewers = ast.literal_eval(row['reviewerIDs'])
            
            for reviewer in reviewers:
                reviewer_paper_relations.append({'paperDOI': paper_doi, 'authorID': reviewer})

    # Create dataframe
    reviewer_paper_df = pd.DataFrame(reviewer_paper_relations)
    reviewer_paper_df.to_csv('data/neo4j_import/reviewer_paper_relations.csv', index=False)

    print("Files saved in the 'data/neo4j_import' directory.")