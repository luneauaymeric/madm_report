#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import numpy as np
import numpy as np
import seaborn as sns

#import bamboolib


# In[2]:


#Path of data

aymeric = "/home/aymeric/python-scripts/anses_medialab/datas/"


# In[3]:


df_tweet = pd.read_csv(f"{aymeric}tweets_pesticides/annotation_compte_twitter.csv", sep = ",")
df_tweet.columns


# In[4]:


df_tweet["world"] = df_tweet["world"].str.replace('\{"choices": \[', "")
df_tweet["world"]
df_tweet["world"] = df_tweet["world"].str.replace('\]\}', "")
df_tweet["world"]
df_tweet["world"] = df_tweet["world"].str.replace('\"', "")
df_tweet["world"] = df_tweet["world"].str.split(',')
df_tweet


# In[5]:


df_tweet["User_world"] = np.nan
df_tweet["User_world2"] =np.nan

for x, world in enumerate(df_tweet["world"]):
    if type(world) is float:
        df_tweet["User_world"].iloc[x] =np.nan
        df_tweet["User_world2"].iloc[x] =np.nan
        
    else:
        if len(world) == 1:
            df_tweet["User_world"].iloc[x] = world[0].strip()
            df_tweet["User_world2"].iloc[x] = world[0].strip()
        elif len(world) > 1 :
            df_tweet["User_world"].iloc[x] = world[0].strip()
            df_tweet["User_world2"].iloc[x] = world[1].strip()
        elif len(world) == 0 :
            df_tweet["User_world"].iloc[x] =np.nan
            df_tweet["User_world2"].iloc[x] =np.nan
        


# In[6]:


df_tweet.columns


# In[7]:


df_fb = pd.read_csv(f"{aymeric}datas_facebook/annotation_compte_facebook.csv", sep = ",")
df_fb.columns


# In[8]:


df_fb = df_fb.loc[df_fb["Type_entite"].isna() == False]


# In[9]:


df_fb["User_world"] = np.nan
df_fb["User_world2"] =np.nan

df_fb["world"] = df_fb["Mondes"].str.split('|')

for x, world in enumerate(df_fb["world"]):
    if type(world) is float:
        df_fb["User_world"].iloc[x] =np.nan
        df_tweet["User_world2"].iloc[x] =np.nan
        
    else:
        if len(world) == 1:
            df_fb["User_world"].iloc[x] = world[0]
            df_fb["User_world2"].iloc[x] = world[0]
        elif len(world) > 1 :
            df_fb["User_world"].iloc[x] = world[0]
            df_fb["User_world2"].iloc[x] = world[1]
        elif len(world) == 0 :
            df_fb["User_world"].iloc[x] =np.nan
            df_tweet["User_world2"].iloc[x] =np.nan


# In[10]:


df_tweet.columns


# In[11]:


df_fb = df_fb[['account_url', 'account_name', 'Comment', 'Type_entite',
       'Genre', 'User_role', 'world',
               'User_world',
               'User_world2',
               'account_description', 'Orientation',
       'thematique', 'account_a_propos',
       'associated_twitter', 'associated_website',
        'account_id', 'account_platform', 
       'account_handle', 'account_type', 'account_page_admin_top_country',
       'account_publication', 'total_posts', 'ratio_posts', ]]


# In[12]:


df_tweet = df_tweet[['user_name', 'Comment', 'Type_entite', 'Genre',
    'User_role', 'world','User_world', 'User_world2', 'Orientation', 'associated_fb_url',
       'associated_website', 'user_account_url',
       'user_screen_name', 'user_location', 'user_id',
       'user_description', 'user_url', 'user_tweets',
       'user_created_at', 
       'user_id_size', 'user_ratio_glypho',  'theme']]


# In[13]:


len(df_tweet.columns)


# In[14]:


df_tweet = df_tweet.rename(columns={
    'user_tweets': 'user_total_post',
       'user_id_size' : 'user_total_pest_posts', 
    'user_ratio_glypho': 'user_ratio_pest_posts', 
    'theme' : 'main_thematic'})

df_tweet["platform"] = "twitter"

df_fb = df_fb.rename(columns={
       'account_url' : "user_account_url", 
    'account_name': 'user_name', 
    'account_description' : 'user_tag',
    'thematique': 'main_thematic', 
    'account_a_propos': 'user_description',
    'associated_twitter' : 'associated_tw_url',
        'account_id': 'user_id', 
    'account_platform' : "platform", 
    'account_handle': 'user_screen_name',
    'account_publication': 'user_total_pest_posts', 
    'total_posts' : 'user_total_posts', 
    'ratio_posts' : 'user_ratio_pest_posts'
})


# In[15]:


df_fb['platform']


# In[16]:


df = pd.concat([df_tweet, df_fb])
df.to_csv("fb_and_tw_annotated_account.csv", sep = "\t")


# In[17]:


df.groupby(['User_world','User_role','platform']).size()#.sort_values(ascending=False)


# In[18]:


dft = df.loc[df["platform"] == "Facebook"]
dft = dft[["user_name","User_world", "User_role"]].dropna()


# In[19]:


dft


# In[20]:


len(result.columns)


# In[108]:


# Create a pivot table
dft = dft.groupby(['User_world','User_role']).count().reset_index()
dft
result = dft.pivot(index='User_world',columns='User_role',values='user_name')#.fillna(0)
#result = result + result.T #get a symmetric matrix
result
dft


# In[109]:


result.index


# In[110]:


import matplotlib
import matplotlib.pyplot as plt

column_name = result.columns
row_name = result.index

plt.rcParams["figure.figsize"] = (10,10)
fig = plt.subplots()

ax = sns.heatmap(result.T, annot=True,  cmap="YlGnBu", linewidths=2, linecolor='red')


# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(),  ha="right", rotation = 45,
         rotation_mode="anchor")



ax.set_title("The worlds of twitter users")

plt.show()


# In[268]:


m = result.to_numpy


# In[297]:



def check_symmetric(a, rtol=1e-05, atol=1e-08):
    return np.allclose(a, a.T, rtol=rtol, atol=atol)


# In[303]:





# In[ ]:


result

