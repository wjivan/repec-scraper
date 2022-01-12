#%%
import pandas as pd
import numpy as np
import psycopg2
import os
from sqlalchemy import create_engine
from tqdm import tqdm_notebook
import importlib
import yaml
from pyprojroot import here
from tqdm import tqdm
import pickle
import scraper
importlib.reload(scraper)


# Utilities ------------->
with open('config.yaml', 'r') as stream:
    config = yaml.safe_load(stream)

# # Create a database connection
db_password = config['dbpass']
engine = create_engine('postgresql://wenjian:{}@localhost/repec'.format(db_password))
# Set up connection using psycopg2
conn = psycopg2.connect("dbname=repec user=wenjian password={}".format(config['dbpass']))
cur = conn.cursor()

# Pipeline ---------->

## Intermediate step in making a proper database table
## Ensure standard columns and dtypes
def make_paper_table(df_paper):
    paper_table = df_paper[['paper_url','paper','year']].drop_duplicates(subset=['paper']).reset_index(drop=True)
    paper_table['year'] = paper_table['year'].astype(str)
    return paper_table

def make_author_table(df_personal):
    # Ensure that df_personal as the same columns always
    personal_details_columns = ['first_name', 'last_name', 'repec_short_id', 'twitter', 'homepage',
       'aff_department0', 'aff_organisation0', 'aff_department1',
       'aff_organisation1', 'aff_location0', 'aff_location1', 'author_url']
    author_table = df_personal.reindex(columns = personal_details_columns, \
        fill_value = None)
    return author_table

def make_author_paper_table(df_paper):
    paper_author_table = df_paper[['paper_url','paper','author', 'first_name','last_name']].drop_duplicates()
    return paper_author_table

## INITIALISE TABLES into POSTGRES ----->
def create_author_url_table(engine):
    url_df = scraper.get_author_urls()
     # Write the data into the database
    url_df.to_sql('author_urls', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE author_urls
                ADD PRIMARY KEY (author_url);"""
    engine.execute(query)
    
    return print('created author_urls table!!!')

def create_paper_table(paper_table, engine):
    # Write the data into the database
    paper_table.to_sql('paper_details', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE paper_details
                ADD PRIMARY KEY (paper_url);
                """
    engine.execute(query)

    return print('created paper_details table!!')

def create_author_table(author_table, engine):

    # Write data into database
    author_table.to_sql('author_details', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE author_details
                ADD PRIMARY KEY (first_name, last_name);"""
    engine.execute(query)

    # Change all types to text
    query = """ALTER TABLE author_details
                ALTER COLUMN homepage SET DATA TYPE TEXT;"""
    engine.execute(query)

    return print('created author_details table!!')

def create_paper_author_table(paper_author_table, engine):
        # Write data into database
    paper_author_table.to_sql('author_paper', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE author_paper
                ADD PRIMARY KEY (paper_url, first_name, last_name);"""
    engine.execute(query)

    return print('created paper_author table!!')


# Initialise the database with a good example
url = 'https://ideas.repec.org/f/pfa379.html'
df_personal, df_paper = scraper.pipeline_scrape_economists(url)
# Make necessary tables
paper_table = make_paper_table(df_paper)
author_table = make_author_table(df_personal)
author_paper_table = make_author_paper_table(df_paper)
create_author_url_table(engine)
create_author_table(author_table,engine)
create_paper_table(paper_table,engine)
create_paper_author_table(author_paper_table,engine)

# Loop through the rest of the url and update the model tables
# We need to be exigent about the data types to insert
# We also need to make sure that duplicates are dropped or simply updated

def get_unchecked_urls(engine):
    # Query remaining URLs that do not have their details fetched yet
    query = """select t1.author_url 
                from author_urls as t1
                left join author_details t2 on t2.author_url=t1.author_url 
                where t2.author_url is null;
            """
    urls = pd.read_sql_query(query, con = engine)
    url_list = list(urls['author_url'])
    return url_list

def joinup(x):
    y = ', '.join(x)
    return y

def update_tables(db_table, pkey, df_update, conn):
    """Updates the database tables

    Parameters
    ----------
    db_table : string
        name for table in the database
    pkey : list
        primary key list for the table for constraints
    df_update : dataframe
        dataframe that is going to be inserted
    conn : psycopg2 connection
    """
    # Note that psycopy2 will insert the right format to replace
    # all the %s in the string

    # First part of the insert statement
    insert_init = """INSERT INTO {}
                    ({})
                    VALUES
                    ({})
                """.format(db_table, 
                joinup(df_update.columns),
                joinup(['%s'] * df_update.shape[1]))

    constraint_actions = """ON CONFLICT ({})
                        DO NOTHING;
                        """.format(joinup(pkey))
    query = insert_init + constraint_actions
    
    # List of row values to insert into insert_init
    row_vals = []
    for idx, row in df_update.iterrows():
        col_vals = []
        for c in df_update.columns:
            col_vals.append(row[c])
        row_vals.append((col_vals))

    # print(query)
    # print(row_vals[0])
    # Execute query
    cur = conn.cursor()
    cur.executemany(query, row_vals)
    conn.commit()
    

# ROCKET FIRE ----->
# Get a list of unchecked URLS
unchecked_urls = get_unchecked_urls(engine)

# # Set out pkeys
pkeys = {'author_details': ['first_name', 'last_name'], \
    'paper_details':['paper_url'], \
    'author_paper':['paper_url', 'first_name', 'last_name']}

# Set out db tables to update
db_to_update = ['author_details','paper_details','author_paper']

examined = []
for u in tqdm(unchecked_urls):
    success = ''
    try:
        # Get tables
        url = 'https://ideas.repec.org' + u
        print('Scraping {}'.format(url))
        df_personal, df_paper = scraper.pipeline_scrape_economists(url)
        # Make necessary tables
        paper_table = make_paper_table(df_paper)
        author_table = make_author_table(df_personal)
        author_paper_table = make_author_paper_table(df_paper)
        tables_to_update = [author_table, paper_table, author_paper_table]
    except:
        print('Failure in making tables and scraping for {}'.format(u))
    # Update the tables
    try:
        for i in range(3):
            update_tables(db_to_update[i], \
                            pkeys[db_to_update[i]], \
                            tables_to_update[i], \
                            conn)
        success = '-Success'
    except:
        print('Failure in insertion into database {}'.format(u))
        success = '-Fail'

    examined.append(u+success)
    with open('parrot.pkl', 'wb') as f:
        pickle.dump(examined, f)

with open('parrot.pkl','rb') as f:
    seen = pickle.load(f)

print('Saved {} out of {}'.format(len(seen), len(unchecked_urls)))
print(seen)
#%%