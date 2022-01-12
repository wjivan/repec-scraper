# Motivation
REPEC IDEAS has up to date information on 63,000 economists' publications all around the whole. It is arguably the most powerful source of information on economist activities. However, the REPEC database is difficult to navigate due to archaic formats. As such, I wrote a scraper to help scrape the website for meta data concerning the economists: journal publications, affiliations, location and paper abstracts. 

This information will be especially useful in studying collaboration networks and large scale topic modeling of publications.

# Process
<img src="/workflow.png" style="text-align:center;">
The scraper I wrote will gather information on all the personal pages for economists on REPEC from this ![summary page](https://ideas.repec.org/i/eall.html). 

The scraper will then go down the list and scrape the useful information on each of the personal pages - with downtime so as not to crash the server. 

One particularly useful information is the Twitter handle information on economists. This information can be used to query Twitter API for further activities of these economists. There are many academic studies that work on scientific dessemination of ideas and this could be one source of information. 