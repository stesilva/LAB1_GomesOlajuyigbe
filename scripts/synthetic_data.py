# Synthetic data generation for CorrespondingAuthor and Citations

import pandas as pd
import random

df = pd.read_csv('transformed_data.csv', sep= ';')

# Function to mark the first author as corresponding author
def mark_corresponding_author(author_ids):
    corresponding_authors = [False] * len(author_ids)
    corresponding_authors[0] = True  # Mark the first author as corresponding
    return 



# Mark corresponding authors
df['correspondingAuthor'] = df['paperAuthorID'].apply(mark_corresponding_author)

# Function to generate citations anc citation count
def generate_citations(df):
    df['citationCount'] = [random.randint(3, 10) for i in range(len(df))]
    df['citedPapers'] = [[] for i in range(len(df))]
    
    # Parse all keywords once to avoid repetitive parsing
    paper_keywords = []
    for i in range(len(df)):
        keywords = df.loc[i, 'keywords']
        paper_keywords.append(keywords)
    
    # Generate citations for each paper
    for i in range(len(df)):
        # Check if both citationCount and citedPapers are null/None
        if pd.isna(df.loc[i, 'citationCount']) and (pd.isna(df.loc[i, 'citedPapers']) or df.loc[i, 'citedPapers'] == [] or df.loc[i, 'citedPapers'] is None):
            # Get citation count and keywords for current paper
            count = df.loc[i, 'citationCount']
            current_keywords = paper_keywords[i]
            # Find papers with similar keywords (excluding self)
            similar_papers = []
            for j in range(len(df)):
                # Skip self-citation
                if i != j:
                    # Check if any keyword matches
                    if any(keyword in paper_keywords[j] for keyword in current_keywords):
                        similar_papers.append(j)
            # If no papers with similar keywords, use all papers except self
            if not similar_papers:
                similar_papers = [j for j in range(len(df)) if j != i]
            # Adjust count if not enough similar papers
            if count > len(similar_papers):
                count = len(similar_papers)
                df.loc[i, 'citationCount'] = count
                print(f'paper {i+1}: Reduced citation count to {count}')
            # Randomly select papers to cite
            if similar_papers and count > 0:
                cited_indices = random.sample(similar_papers, count)
                cited_dois = [df.loc[idx, 'paperDOI'] for idx in cited_indices]
                df.at[i, 'citedPapers'] = cited_dois   
        return df


# generate citations
df = generate_citations(df)

df.to_csv('synthetic_data.csv', index=False)
print("\nData saved to 'synthetic_data.csv'")

