#%%
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import yaml
import tweepy
import json


# Utilities ------------->
with open('config.yaml', 'r') as stream:
    config = yaml.safe_load(stream)

# # Create a database connection
db_password = config['dbpass']
engine = create_engine('postgresql://wenjian:{}@localhost/gender'.format(db_password))
# Set up connection using psycopg2
conn = psycopg2.connect("dbname=gender user=wenjian password={}".format(config['dbpass']))
cur = conn.cursor()

#%%
twitter_query = """with author_sum as (
	select first_name, last_name, count(cpd.paper_url) as paper_count, min(year) as first_paper_year, max(year) as last_paper_year
	from clean_author_paper cap 
	left join clean_paper_details cpd 
	on cap.paper_url = cpd.paper_url
	group by first_name, last_name
),
	author_twitter as (
	select first_name, last_name, repec_short_id, twitter
	from author_details
	where twitter != 'NaN'
)
select *
from author_twitter atw 
left join author_sum asum
on atw.first_name = asum.first_name and atw.last_name = asum.last_name;
"""
test_query = """select first_name, last_name, repec_short_id, twitter
	from author_details
	where twitter != 'NaN';
"""
twitter_df = pd.read_sql_query(twitter_query,conn)
# Remove duplicated columns due to joins
twitter_df = twitter_df.loc[:,~twitter_df.columns.duplicated()]
print(twitter_df.shape)
# %%

# Running twitter queries

# Authenticate to Twitter
auth = tweepy.OAuthHandler(config['twitter']['api_key'], \
	config['twitter']['api_secret_key'])
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, \
	parser=tweepy.parsers.JSONParser())

id = twitter_df.loc[0,'twitter']
print(id)
#%%
user_status = api.user_timeline(id)
test = pd.json_normalize(user_status)

# %%
