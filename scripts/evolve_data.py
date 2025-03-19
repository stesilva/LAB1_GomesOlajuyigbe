import pandas as pd
import numpy as np

def generate_affiliation(df):

    #Create possible options for institution
    universities = [f"University of {city}" for city in ['Barcelona', 'New York', 'Paris', 'Berlin', 'London', 'Madrid', 'Rome']]
    companies = [f"{adj} Corporation" for adj in ['Zeta', 'Alpha', 'Omega', 'Ijuo']]
    
    # Assign random institutions to authors
    df['type'] = np.random.choice(['university', 'company'], size=len(df))
    df['institution_name'] = np.where(
        df['type'] == 'university',
        np.random.choice(universities, size=len(df)),
        np.random.choice(companies, size=len(df))
    )
    
    # Create unique institution IDs
    institutions = df[['institution_name', 'type']].drop_duplicates()
    institutions['institution_id'] = institutions.groupby(['institution_name', 'type']).ngroup().add(1)
    
    # Merge back to get institution IDs
    df = df.merge(institutions, on=['institution_name', 'type'])
    
    institutions[['institution_id', 'institution_name', 'type']]\
        .rename(columns={'institution_id': 'institutionID', 'institution_name': 'name'})\
        .to_csv('data/neo4j_import/institutions.csv', sep=',', index=False)
    
    return df[['authorID', 'institution_id']]\
        .rename(columns={'institution_id': 'institutionID'})

def synthetic_data():
    df = pd.read_csv('data/neo4j_import/authors.csv', sep=',')
    relations_df = generate_affiliation(df)
    
    # Save relations
    relations_df.to_csv('data/neo4j_import/author_institution_relations.csv', 
                       sep=',', index=False)
    
    print("Generated files:\n- institutions.csv\n- author_institution_relations.csv")