# %%
import pandas as pd
import numpy as np
import requests
import unicodedata
import string
import re
from fuzzywuzzy import fuzz, process
from bs4 import BeautifulSoup
from tqdm import tqdm

# ----------- UTILITY ---------------
def clean_string(string):
    string = unicodedata.normalize('NFKD',string) \
        .encode('ascii', 'ignore') \
        .decode('ascii') \
        .lower() \
        .strip() \
        .title()
    return string

def clean_series(series):
    cleaned = series.map(clean_string)
    return cleaned

def standardise_column_names(df, remove_punct=True):
    """ Converts all DataFrame column names to lower case replacing
    whitespace of any length with a single underscore. Can also strip
    all punctuation from column names.
    
    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame with non-standardised column names.
    remove_punct: bool (default True)
        If True will remove all punctuation from column names.
    
    Returns
    -------
    df: pandas.DataFrame
        DataFrame with standardised column names.

    """
    
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))

    for c in df.columns:
        c_mod = c.lower()
        if remove_punct:            
            c_mod = c_mod.translate(translator)
        c_mod = '_'.join(c_mod.split(' '))
        if c_mod[-1] == '_':
            c_mod = c_mod[:-1]
        c_mod = re.sub(r'\_+', '_', c_mod)
        df.rename({c: c_mod}, inplace=True, axis=1)
    return df

# There are instances where author name appears as Lastname, Firstname
def reverse_comma(x):
    if ',' in x:
        x = x.split(', ')[-1] +' '+ x.split(', ')[0]
    return x

# PIPELINE ----------------->
# Set up soup
def setup_soup(url):
    # Setup beautiful soup
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup

# Obtain a list of urls to scrape
def get_author_urls():
    url = 'https://ideas.repec.org/i/eall.html'
    soup = setup_soup(url)
    links = soup.find_all('a', href=True)
    url_collection = []
    for i in tqdm(links,desc='Downloading links'):
        author = i.text
        author_url = i['href']
        url_collection.append([author, author_url]) 
    
    # Get rid of unnecessary links by finding the position of the first and last author
    pos = [idx for idx, results in enumerate(url_collection) if ('Aaberge, Rolf ' in results[0]) or ('Zhou, Li ' in results[0])]
    url_collection = url_collection[min(pos):max(pos)+1]    

    # Some cleaning and get it into a DF
    clean_urls = [reverse_comma(clean_string(u[0])).split(None, 2) + [u[1]] for u in url_collection]
    cleaned = pd.DataFrame(clean_urls, columns=['first_name','middle_name','last_name','partial'])
    cleaned['author_url'] = np.where(cleaned['partial'].isnull(), cleaned['last_name'], cleaned['partial'])
    cleaned['last_name'] = np.where(cleaned['partial'].isnull(), cleaned['middle_name'], cleaned['last_name'])
    cleaned['middle_name'] = np.where(cleaned['partial'].isnull(), None, cleaned['middle_name'])
    cleaned = cleaned.drop(columns=['partial'])
    return cleaned

# Scrape papers information from the author
def scrape_papers(soup):
    # Publication classifications free, gate, none
    # There will be overlaps
    publications = soup.find_all('li', class_={'list-group-item downfree', \
        'list-group-item downgate', 'list-group-item downnone'})
    paper_details = {}

    i = 1
    for pub in publications:
        try:
            title = pub.find('a').text
            # Get paper url and do some cleaning
            paper_url = pub.find('a')['href'].replace('https://ideas.repec.org/','')
            if not paper_url[0] == '/':
                paper_url = '/'+paper_url
            
            # Get the authors and year of paper
            name_year = pub.text.strip().split('\n')[0]
            if 'undated' in name_year:
                year = None
                authors = re.sub(r', \"undated\"', '',name_year).split(' & ')
            else:
                year = int(re.findall(r', (\d{4})\.', name_year)[0])
                authors = re.sub(r', \d{4}\.', '',name_year).split(' & ')
            paper_details[title] = {'author': authors, 'year': year, 'paper_url': paper_url}
        except:
            print('something went wrong at paper {}'.format(i))
        i +=1
    return paper_details

# Scraping personal information of the author
def scrape_personal(soup):
    # Find portion where personal details lie in
    personal_details = soup.find('tbody').find_all('tr')

    # Set up a dictionary to collect all personal information
    per = {}
    for p in personal_details:
        k = p.find_all('td')[0].text.replace(':','')
        v = p.find_all('td')[1].text.strip()
        per[k] = v
    
    per_clean = {k:v for (k,v) in per.items() if (v is not '') }
    

    # Find homepage link
    try:    
        homepage = soup.find('td', {'class':'homelabel'}).next_sibling.find('a', href=True)['href']
        per_clean['Homepage'] = str(homepage)
    except:
        print('homepage not found')

    # Find affiliation - can have multiple
    affiliation_soup = soup.find('div', {'id':'affiliation'})

    i = 0
    try:
        for a in affiliation_soup.find_all('h3'):
            if a.find('br'):
                department = a.find('br').previous_sibling
                organisation = a.find('br').next_sibling
            else:
                print('no breaks in affiliation')
                department = ''
                organisation = a
            per_clean['Aff_Department{}'.format(i)] = str(department)
            per_clean['Aff_Organisation{}'.format(i)] = str(organisation)
            i += 1
    except:
        print('affiliation not found')

    # Find affiliation locations - can have multiple
    i = 0
    try:
        for a in affiliation_soup.find_all('span', {'class':'locationlabel'}):
            if a:
                location = a.text
            else:
                print('no location in affiliation')
            per_clean['Aff_Location{}'.format(i)] = str(location)
            i += 1
    except:
        print('affiliation not found')

    # Drop unnamed items
    per_clean = {k:v for (k,v) in per_clean.items() if (k is not '') }

    return per_clean

# Flatten the paper details into a dataframe to be inserted into database
def makedf_paper(paper_details):
    # Flatten the paper_details dictionary into a pandas dataframe
    pd_paperdetails = pd.DataFrame(paper_details) \
        .transpose() \
        .explode('author') \
        .reset_index() \
        .rename(columns = {'index':'paper'})
    
    # Make capitalise titles
    pd_paperdetails[['paper','author']] = pd_paperdetails[['paper','author']] \
        .apply(clean_series, axis=1)

    # Drop duplicates
    pd_paperdetails = pd_paperdetails.drop_duplicates(
        subset = ['paper_url', 'author'])
    
    # Drop titles that are very similar 
    similar = process.dedupe(list(pd_paperdetails['paper'].unique()), threshold = 95)
    pd_paperdetails = pd_paperdetails[pd_paperdetails['paper'].isin(similar)]
   
    # Cleaning
    # Replace anything like (Ed.)
    pd_paperdetails['author'] = pd_paperdetails['author'].str.replace(r'\(.*\)', '')
    # Make year numeric
    pd_paperdetails['year'] = pd_paperdetails['year'].astype(str)
    
    # Reverse Firstname last name
    pd_paperdetails['author'] = pd_paperdetails['author'].map(reverse_comma)

    # Create first & last name & limit to 2 splits - first name, middle name, last name
    pd_paperdetails['first_name'] = pd_paperdetails['author'].str.split(None, 2).str[0]
    pd_paperdetails['last_name'] = pd_paperdetails['author'].str.split(None, 2).str[-1]
    return pd_paperdetails

def makedf_personal(personal_details):
    # Make DF
    df_personal = pd.DataFrame.from_records([personal_details])
    # Standardise column names
    df_personal = standardise_column_names(df_personal)
    return df_personal

def reconcile_first_name(df_paper, df_personal):
    df_paper = df_paper.merge(df_personal[['first_name','last_name']], on=['last_name'], how='left')
    df_paper['first_name'] = np.where(df_paper['first_name_y'].notnull(), df_paper['first_name_y'], df_paper['first_name_x'])
    df_paper = df_paper.drop(['first_name_x', 'first_name_y'], axis=1)
    df_paper = df_paper.drop_duplicates(subset=['paper','first_name','last_name'])
    return df_paper

def pipeline_scrape_economists(url):
    # Takes an url and output a personal detail and paper dataframes
    soup = setup_soup(url)
    paper_details = scrape_papers(soup)
    personal_details = scrape_personal(soup)
    df_paper = makedf_paper(paper_details)
    df_personal = makedf_personal(personal_details)
    df_paper = reconcile_first_name(df_paper, df_personal)

    # Save the url into the author table
    url = url.replace('https://ideas.repec.org', '')
    df_personal['author_url'] = url

    return df_personal, df_paper

def scrape_abstract(df_paper):
    paper_urls = df_paper['paper_url'].drop_duplicates()
    abstract_dict = {}
    for a in tqdm(paper_urls):
        try:
            soup =  setup_soup('https://ideas.repec.org' + a)
            abstract_text = soup.find('div', {'id':'abstract-body'}).text
            abstract_dict[a] = abstract_text
        except:
            print('Paper {} cannot find abstract'.format(a))
            abstract_dict[a] = None
    abstract_table = pd.DataFrame([(x,y) for x,y in abstract_dict.items()], columns = ['paper_url','abstract'])
    return abstract_table


# url = 'https://ideas.repec.org/e/pag127.html'
# soup = setup_soup(url)
# paper_details = scrape_papers(soup)
# personal_details = scrape_personal(soup)
# df_paper = makedf_paper(paper_details)
# df_personal = makedf_personal(personal_details)
# df_paper = reconcile_first_name(df_paper, df_personal)

# ISSUES
   # Remove potential duplicates
    # df_paper = df_paper.merge(abstract_table, on='paper_url', how='left')
    # abstract_table = abstract_table.drop_duplicates(subset=['abstract'])
    # df_paper = df_paper[df_paper['paper_url'].isin(list(abstract_table['paper_url'].unique()))]
# %%
