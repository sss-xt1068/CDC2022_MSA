#!/usr/bin/env python
# coding: utf-8

# # Team StormTroopers - CDC 2022
# 
# ## Team Members:
# - Nikhil Suthar
# - Sanket Sahasrabudhe
# - Brendan Bammer
# - Nish Torane

# ### Step 1: Import libraries

# In[1]:


import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import os


# In[2]:


url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"


# In[3]:


# Fetch URLs for all files on the site
page = requests.get(url)
soup = bs(page.content)
links = [x['href'] for x in soup.select('a')]
links = links[7:-2]


# In[15]:


data_path = "MergedData"


# In[4]:


# Declare empty dataframes for storing different files
fatalities = pd.DataFrame()
details = pd.DataFrame()
locations = pd.DataFrame()


# In[8]:


# Download every gzip file as a dataframe and add to dataframe
for link in links:
    path = os.path.join(url, link)
#     file = requests.get(path)
    temp = pd.read_csv(path, compression='gzip', low_memory=False)
    if 'fatalities' in path:
        fatalities = pd.concat([fatalities, temp])
    elif 'details' in path:
        details = pd.concat([details, temp])
    else:
        locations = pd.concat([locations, temp])


# In[9]:


fatalities.shape


# In[11]:


details.shape


# In[12]:


locations.shape


# In[ ]:


# Export CSV files to drive
details.to_csv(os.path.join(data_path, 'details.csv'), index=False)
locations.to_csv(os.path.join(data_path, 'locations.csv'), index=False)
fatalities.to_csv(os.path.join(data_path, 'fatalities.csv'), index=False)


# In[ ]:


def extract_price(price_string):
    price_string = str(price_string)
    if len(price_string)>0:
        if 'k' in price_string.lower():
            try:
                temp = float(price_string[:-1])
                temp = temp*10**3
            except:
                temp = 10**3
        elif 'm' in price_string.lower():
            try:
                temp = float(price_string[:-1])
                temp = temp*10**6
            except:
                temp = 0
        elif 'b' in price_string.lower():
            temp = float(price_string[:-1])
            temp = temp*10**9
        elif 'h' in price_string.lower():
            temp = float(price_string[:-1])
            temp = temp*10**2
        elif '?' in price_string.lower():
            temp = float(price_string[:-1])
        elif 't' in price_string.lower():
            temp = float(price_string[:-1])
        else:
            temp = float(price_string)
    else:
        temp = 0
    return temp


# In[ ]:


details['DAMAGE_CROPS'] = details['DAMAGE_CROPS'].apply(extract_price)
details['DAMAGE_PROPERTY'] = details['DAMAGE_PROPERTY'].apply(extract_price)


# In[ ]:


details['EVENT_TYPE'].replace({'Hurricane (Typhoon)': 'Hurricane',
                                            'Marine Hurricane/Typhoon': 'Hurricane'},
                                           inplace=True)


# In[ ]:


details['TOR_F_SCALE'].replace({'F0': 'EF0',
                          'F1': 'EF1',
                          'F2': 'EF2',
                           'F3': 'EF3',
                           'F4': 'EF4',
                           'F5': 'EF5'
                          },
                         inplace=True)


# In[ ]:


details['STATE'].replace({'Kentucky': 'KENTUCKY'},
                                 inplace=True)details['STATE'].replace({'Kentucky': 'KENTUCKY'},
                                 inplace=True)


# In[ ]:


details['Total_Damage'] = details['DAMAGE_CROPS'] + details['DAMAGE_PROPERTY']

