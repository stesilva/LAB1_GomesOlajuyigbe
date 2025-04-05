import pandas as pd
import numpy as np
import random
import os
import shutil

def generate_affiliation(df):
    #Create possible options for institution
    universities = [f"University of {city}" for city in ['Barcelona', 'New York', 'Paris', 'Berlin', 'London', 'Madrid', 'Rome']]
    companies = [f"{adj} Corporation" for adj in ['Zeta', 'Alpha', 'Omega', 'Ijuo']]
    
    #Assign random institutions to authors
    df['type'] = np.random.choice(['university', 'company'], size=len(df))
    df['institution_name'] = np.where(
        df['type'] == 'university',
        np.random.choice(universities, size=len(df)),
        np.random.choice(companies, size=len(df))
    )
    
    #Create unique institution IDs
    institutions = df[['institution_name', 'type']].drop_duplicates()
    institutions['institution_id'] = institutions.groupby(['institution_name', 'type']).ngroup().add(1)
    
    #Merge back to get institution IDs
    df = df.merge(institutions, on=['institution_name', 'type'])
    
    institutions[['institution_id', 'institution_name', 'type']]\
        .rename(columns={'institution_id': 'institutionID', 'institution_name': 'name'})\
        .to_csv('data/neo4j_import/institutions.csv', sep=',', index=False)
    
    return df[['authorID', 'institution_id']]\
        .rename(columns={'institution_id': 'institutionID'})

def generate_reviews(input_file, output_file):
    #Read the CSV file
    review_df = pd.read_csv(input_file)

    #Create lists of reasons for different decisions
    accept_reviews = [
        "The paper presents novel and significant contributions to the field.",
        "The methodology is sound and the results are convincing.",
        "The paper is well-written and clearly structured.",
        "The experimental results strongly support the claims made.",
        "The theoretical framework is robust and well-justified.",
        "The work addresses an important gap in the literature.",
        "The paper's findings have significant practical implications.",
        "The research questions are well-motivated and clearly addressed.",
        "The paper demonstrates excellent technical depth and rigor.",
        "The work shows innovative application of existing methods.",
        "The manuscript is exceptionally well-organized and easy to follow.",
        "The study provides comprehensive analysis with strong statistical support.",
        "The topic is highly relevant and timely within the field.",
        "The paper includes insightful discussions that enhance understanding.",
        "The use of real-world datasets strengthens the credibility of the findings.",
        "The proposed approach outperforms existing methodologies significantly.",
        "The research design is thorough and eliminates potential biases.",
        "The conclusions drawn are well-supported by empirical evidence.",
        "The paper effectively integrates theory with practical applications.",
        "The manuscript offers a fresh perspective on a long-standing issue."
    ]

    revise_reviews = [
        "The paper needs minor revisions to improve clarity and presentation.",
        "The methodology requires additional explanation and justification.",
        "Some claims need to be supported with more evidence or data.",
        "The related work section needs to be expanded.",
        "The limitations of the study should be more clearly acknowledged.",
        "The conclusions should be more tightly connected to the results.",
        "Some experimental details are missing or unclear.",
        "The paper would benefit from additional analysis of the results.",
        "The theoretical framework needs to be more clearly articulated.",
        "The practical implications should be discussed in more depth."
    ]

    reject_reviews = [
        "The contributions are not significant enough for publication.",
        "The methodology has critical flaws that undermine the results.",
        "The paper lacks novelty and does not advance the field.",
        "The experimental design is inadequate to support the claims.",
        "The paper has major theoretical inconsistencies.",
        "The work is too preliminary for publication.",
        "The paper does not meet the quality standards of the venue.",
        "The research questions are not well-motivated or important.",
        "The paper has substantial weaknesses in its technical approach.",
        "The work is out of scope for this publication venue."
    ]

    
    #Function to generate reviews
    def generate_reviews(num_reviewers):
        #Ensure majority accept
        num_accept = random.randint((num_reviewers // 2) + 1, num_reviewers)
        num_other = num_reviewers - num_accept
        
        decisions = ["Accept"] * num_accept + random.choices(["Revise", "Reject"], k=num_other)
        random.shuffle(decisions)  #Shuffle to mix up the order
        
        reviews = []
        for decision in decisions:
            if decision == "Accept":
                reviews.append(random.choice(accept_reviews))
            elif decision == "Revise":
                reviews.append(random.choice(revise_reviews))
            else:
                reviews.append(random.choice(reject_reviews))
        
        return list(zip(decisions, reviews))

    #Generate reviews per paper
    review_data = {}
    for paper_id, group in review_df.groupby('paperDOI'):
        num_reviewers = len(group)
        decisions_reviews = generate_reviews(num_reviewers)
        review_data[paper_id] = decisions_reviews

    #Assign reviews to authors
    final_review_data = []
    for (paper_id, author_id), _ in review_df.groupby(['paperDOI', 'authorID']):
        decision, review = review_data[paper_id].pop(0)
        final_review_data.append((paper_id, author_id, decision, review))

    #Create new DataFrame with reviews
    df_reviews = pd.DataFrame(final_review_data, columns=['paperDOI', 'authorID', 'suggested_decision', 'review'])

    #Merge back to original DataFrame
    review_df = review_df.merge(df_reviews, on=['paperDOI', 'authorID'], how='left')

    review_df.to_csv(output_file, index=False)


def synthetic_data(import_path):
    df = pd.read_csv('data/neo4j_import/authors.csv', sep=',')
    relations_df = generate_affiliation(df)
    
    relations_df.to_csv('data/neo4j_import/author_institution_relations.csv', 
                       sep=',', index=False)

    generate_reviews('data/neo4j_import/reviewer_paper_relations.csv', 'data/neo4j_import/reviewer_paper_properties.csv')
    print("Generated files:\n- institutions.csv\n- author_institution_relations.csv\n- reviewer_paper_properties.csv")

    for file_name in os.listdir('data/neo4j_import'):
        source_file = os.path.join('data/neo4j_import', file_name)
        destination_file = os.path.join(import_path, file_name)
        shutil.copy(source_file, destination_file)
