#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import markdown as md


# In[2]:


#url = 'https://drive.google.com/file/d/0B6GhBwm5vaB2ekdlZW5WZnppb28/view?usp=sharing'
#path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
#df = pd.read_csv(path)
#print(path)


# In[3]:


current_directory = os.getcwd()
print("Current directory : ", current_directory)

aymeric =  "/home/aymeric/python-scripts/anses_medialab/datas/" #aymeric
jp = '~/Dropbox/Mac/Desktop/CRD Anses/all3/' # Jean Philippe
jp_index = '~/Dropbox/Mac/Desktop/CRD Anses/code/indexation_results/' # Jean Philippe index

if 'aymeric' in current_directory:
    path_base = aymeric
    index=f"{path_base}index_allall_domainsexhaustive.csv"
elif 'Mac' in current_directory:
    path_base = jp
    index=f'{jp_index}index_allall_domainsexhaustive.csv'
elif 'd:/Projects' in current_directory:
    path_base = "d:/Projects/Medialab/"
    index=f"{path_base}index_allall_domainsexhaustive.csv"

print("Path base : ", path_base)


# # Chargement des données et nettoyage des données
# 
# ## Facebook
# 
# ### Les postes facebook

# In[4]:


# Chargement du corpus facebook (les posts)
data_file = path_base+"datas_facebook/data_ct_glypho_pest_roundup_etc.csv.zip"#,line_terminator='\n',index=False)
    ## fichier des posts facebook
fb_posts = pd.read_csv(data_file, dtype={"account_id":str})
fb_posts = fb_posts.loc[fb_posts["account_platform"] == "Facebook"]
fb_posts["account_publication"] = fb_posts.groupby("account_name")["id"].transform("count")

len(fb_posts)

fb_posts.loc[fb_posts["account_url"] == "https://www.facebook.com/934946709937770", "account_id"] = "934946709937770"
fb_posts.loc[fb_posts["account_url"] == "https://www.facebook.com/934946709937770", "account_handle"] = "SteveFah237"


# ### La liste des pages et groupes facebook

# In[5]:


#chargement de la liste des comptes facebook
fb_account = pd.read_csv(f"{path_base}datas_facebook/all_account_facebook.csv", sep ="\t", dtype={"account_id":str})
len(fb_account)


# In[6]:


df0 = pd.read_csv("/home/aymeric/python-scripts/anses_medialab/analyse_mixte/fb_and_tw_annotated_account2.csv", sep = "\t", dtype={"user_id":str})


# ## Twitter
# 
# ### Le corpus de tweets

# In[7]:


#a changer localement
#media_filename='../medias/data/dwld_pesticides_and_cie_mediacloud_stories.csv.zip'
#tweet_path='../twitter/tweets_pesticides and cie/*'
#fb_filename='../facebook/data/glypho_1percent_posts.csv.gz'
#fb_filename="../facebook/data/post_glyphosate_since_2010_new_sourcing.csv"

##donnees aymeric
tweet_path = f"{path_base}/tweets_pesticides/tweets_pesticides and cie/*"

import glob
#paths = glob.glob('d:/Projects/Medialab/Anses/tweetstotal/*')
paths=glob.glob(tweet_path)
paths

tweets=pd.DataFrame()
for p in [f for f in paths if not '.zip' in f]:    
    df=pd.read_csv(p, dtype={"user_id":str})
    tweets=tweets.append(df, ignore_index=True)
tweets=tweets.drop_duplicates()
tweets['date'] = pd.to_datetime(tweets['local_time']).dt.date
#tweets

print (len(tweets))
tweets=tweets.dropna(subset=['text'])
tweets = tweets.reset_index()

tweets = tweets.loc[~(tweets['text'].str.contains('N.H.L.', case=False, regex=False, na=False))]
print (len(tweets))


# ### La liste des comptes
# 
# Le script ci-dessous sert à uniformiser les "user_name", "user_screen_name" et "user_tweets" (nombre de tweets publiés par un compte), car il arrive que la "valeur" de ces variables changent avec le temps : les propriétaires des comptes peuvent changer leur noms et le nombre de tweets publiés évoluent par définition avec le temps.
# 
# Lorsqu'un compte a changé plusieurs fois de noms (un même "user_id" est associé à plusieurs "user_name" ou "user_screen_name"), nous avons choisi de garder le plus récent. Il en va de même pour le nombre de tweets publiés : nous avons gardé la dernière valeur connue.
# 
# Par ailleurs, du fait qu'il existe des homonymes (plusieurs "user_id" peuvent avoir le même "user_name"), nous travaillerons de préférences avec les "user_id" ou les "user_screen_name".

# In[8]:


tw_account0 = tweets[['id', 'timestamp_utc','local_time',
       'user_screen_name', 'user_location', 
       'user_id', 'user_name', 'user_description',
       'user_url', 'user_tweets', 'user_followers',
       'user_friends', 'user_likes', 'user_lists', 'user_created_at',
       'user_timestamp_utc', 'date']]

total_post = tw_account0[["user_id", 'user_tweets']].sort_values(by=["user_id",'user_tweets'], ascending = [True, False]).drop_duplicates(subset = ["user_id"], keep = "first")


total_post = total_post.rename(columns={"user_tweets": "total_posts"})

recent_scr_name = tw_account0[["user_id","user_screen_name", 'timestamp_utc']].sort_values(by=["user_id",'timestamp_utc'], ascending = [True, False]).drop_duplicates(subset = ["user_id"], keep = "first")

recent_name = tw_account0[["user_id","user_name", 'timestamp_utc']].sort_values(by=["user_id",'timestamp_utc'], ascending = [True, False]).drop_duplicates(subset = ["user_id"], keep = "first")

recent_name = recent_name.merge(recent_scr_name, how = "left", on = ["user_id", 'timestamp_utc']).merge(total_post, how = "left", on = ["user_id"])

recent_name = recent_name.drop(columns = ["timestamp_utc"])

tw_account2 = tw_account0.drop(columns = ["user_screen_name", "user_name"]).merge(recent_name, how = "left", on = ["user_id"])
tweets = tweets.drop(columns = ["user_screen_name", "user_name"]).merge(recent_name, how = "left", on = ["user_id"])



tw_account2 = tw_account2[['id',
       'user_id', 'user_name','user_screen_name', "total_posts",
        'user_description', 'user_location', 
       'user_url', 'user_tweets', 'user_followers',
       'user_friends', 'user_likes', 'user_lists', 'user_created_at',
       'user_timestamp_utc', 'date']]

tw_account2["account_publication"] = tw_account2.groupby(["user_id"])["id"].transform("count")
tw_account = tw_account2.drop_duplicates(subset = ["user_id"])
tw_account["ratio_posts"] = tw_account["account_publication"] / tw_account["total_posts"] *100


# ### Le fichier des pages et groupes annotés

# In[23]:



df_f = df0.loc[df0["platform"] == "Facebook"]
df_t = df0.loc[df0["platform"] == "twitter"]

t = tweets[["user_id", "user_screen_name", "user_name", 'total_posts']].drop_duplicates()


f = fb_account[['account_id', 'account_url',  'account_publication','total_posts',
       'ratio_posts']].rename(
    columns = {"account_id" : "user_id", "account_url": "user_account_url"})



df_f = df_f.drop(columns = ["user_id", 'account_publication','total_posts', 'ratio_posts']).merge(f, how = "left", on = ["user_account_url"])
df_t =  df_t.drop(columns = ["user_id", 'total_posts']).merge(t, how = "left", on = ["user_screen_name", "user_name"])
df_t =  df_t.drop(columns = ["user_screen_name", "user_name"]).merge(
    tw_account[["user_id","user_screen_name", "user_name"]], how = "left", on = ["user_id"])
df = pd.concat([df_t,df_f])


df = df[['user_id', 'user_screen_name','user_name',
         'index', 'Comment', 'Type_entite', 'Genre', 'User_role2', 'User_role',
       'world', 'User_world', 'User_world2', 'Orientation', 'main_thematic',
       'user_description', 'associated_fb_url', 'associated_website',
       'user_account_url', 'user_location', 'total_posts',
       'account_publication', 'ratio_posts', 'user_created_at', 'platform',
       'user_tag', 'associated_tw_url', 'account_type',
       'account_page_admin_top_country']]

df.groupby(["platform"]).size()


# In[28]:


f = fb_account[['account_id', 'account_url',  'account_publication','total_posts', "account_name", "account_handle", 
       'ratio_posts']].rename(
    columns = {"account_id" : "user_id", "account_url": "user_account_url",
              "account_name":"user_name", "account_handle" : "user_screen_name", })

df_f["annote"] = "True"
df_f2 = f.merge(df_f.drop(columns = ["user_id", 'account_publication','total_posts', 
                                     'ratio_posts', 'index', "user_screen_name", "user_name"]), how = "left", on = ["user_account_url"])
df_f2["platform"] = "Facebook"
df_f2.loc[df_f2["annote"].isnull()==False, "annoted"] = "True"
df_f2.loc[df_f2["annote"].isnull()==True, "annoted"] = "False"

df_t["annote"] = "True"
df_t2 =  t.merge(df_t.drop(columns = ["user_id", 'total_posts', "index"]), how = "left", on = ["user_screen_name", "user_name"])
df_t2 =  tw_account[["user_id","user_screen_name", "user_name", 
                    'account_publication','total_posts', 
                    'ratio_posts']].merge(df_t2.drop(columns = ["user_screen_name", 
                                                                "user_name", 'account_publication','total_posts', 
                                                                'ratio_posts']), how = "left", on = ["user_id"])
df_t2["platform"] = "twitter"
df_t2.loc[df_t2["annote"].isnull()==False, "annoted"] = "True"
df_t2.loc[df_t2["annote"].isnull()==True, "annoted"] = "False"
df = pd.concat([df_f2, df_t2])


# In[32]:


df = df.rename(columns = {"main_thematic" : "Logics"})

df["Logics"] = df['Logics'].replace(
    {"Rationaliste" : "Positivistes",
    'Mouvments anti-ogm': 'Mouvements anti-ogm'})

df["Synthetic_logics"] = df['Logics'].replace(
    {'Produits pharmaceutiques et cosmétiques': "Marketing_logic", 
      'Commerce et grande distribution': "Marketing_logic",
     'Industrie agroalimentaire' : 'Agroindustrial_perspectives',
     'Matériels agricoles': 'Agroindustrial_perspectives', 
     'Industrie phytosanitaires et biocides': 'Agroindustrial_perspectives',
     'Positivistes' : 'Positivistic_logic', 'Défense des agricultures conventionnelles' :"Positivistic_logic",
     'Mouvements écologistes' : 'Ecological_perspectives', 
     'Justice environnementale': 'Ecological_perspectives',
     'Mouvements anti-ogm':'Ecological_perspectives',
     'Contre le monde de Monsanto et cie':'Ecological_perspectives',
     "Protection de l'environnement":'Ecological_perspectives',
     'Défense des agricultures non-conventionnelles' : 'Pesticide_free_agriculture',
     'Lutte contre les pesticides': 'Pesticide_free_agriculture',
     'Apiculture': 'Pesticide_free_agriculture', 
     'Mouvement de victimes des pesticides':'Pesticide_free_agriculture',
     'Santé': "Health_perspectives",
     'Actualité' : 'Comment_the_news',
     'Bien-être animal' : 'Ecological_perspectives'
      })


# In[33]:


df.groupby(["Synthetic_logics"]).size()


# In[34]:


fb_posts.columns
posts = fb_posts[['id', 'title', 'caption', 'message', 'account_type',
       'description', 'date', 'account_id', "account_name", "account_handle",
       'account_page_admin_top_country']].rename(columns ={'account_id':'user_id', 
                                                           "account_name" : 'user_name', 
                                                           "account_handle":"user_screen_name"})


# In[35]:


df01 = df[["platform", "annoted", "user_id", "user_screen_name", "user_name", "user_account_url", "Type_entite", "Genre", "User_world", "Logics", "Synthetic_logics", "account_publication", "total_posts", "ratio_posts"]]
df02 = df[["platform", "annoted", "user_id", "user_screen_name", "user_name", "user_account_url", "Type_entite", "User_world2", "Logics", "Synthetic_logics", "account_publication", "total_posts", "ratio_posts"]].rename(columns={"User_world2":"User_world"})

what_platform = {"twitter":"tweets", "Facebook":"Facebook posts"}

for origin in ["twitter", "Facebook"]:
    if origin == "twitter":
        df_t1 =  tweets[['id', 'timestamp_utc', 'local_time', 'text',
               'user_id', 'user_screen_name', 'user_name', 'mentioned_names', 
               'mentioned_ids', 'hashtags', 'date']].merge(
            df01.loc[df["platform"] == "twitter"], how = "left", on = ["user_id"])

        df_t2 =  tweets[['id', 'timestamp_utc', 'local_time', 'text',
               'user_id', 'user_screen_name', 'user_name', 'mentioned_names',
               'mentioned_ids', 'hashtags', 'date']].merge(
            df02.loc[df["platform"] == "twitter"], how = "left", on = ["user_id"])
        
        df_t = pd.concat([df_t1, df_t2]).drop_duplicates()#.dropna(subset=["Type_entite"])

        df_t['date'] = pd.to_datetime(df_t['date'], infer_datetime_format=True)

        df_t['yearmonth']=(df_t['date'].dt.strftime('%Y-%m'))

        df_t['date'] = df_t['date'].dt.date
        
        
        df_t["platform"] = "twitter"
        df_t = df_t[['platform', "annoted", 'id', 'text', 'date', 'mentioned_names', 
                     'mentioned_ids', 'hashtags', 'user_id', 'user_screen_name_x', 'user_name_x', 
                     'user_screen_name_y', 'user_name_y', 'user_account_url',  'Type_entite', 'Genre', 
                     'User_world', 'Logics', 'Synthetic_logics', 'account_publication', 
                     "total_posts", 'ratio_posts', 'yearmonth']]

    elif origin == "Facebook":
        df_f1 =  posts.merge(
            df01.loc[df["platform"] == "Facebook"], how = "left", on = ["user_id"])
        
        df_f2 =  posts.merge(
            df02.loc[df["platform"] == "Facebook"], how = "left", on = ["user_id"])

        df_f = pd.concat([df_f1, df_f2]).drop_duplicates().dropna(subset=["message"])

        df_f['date'] = pd.to_datetime(df_f['date'], infer_datetime_format=True)

        df_f['yearmonth']=(df_f['date'].dt.strftime('%Y-%m'))

        df_f['date'] = df_f['date'].dt.date
 
        df_f["text"] = df_f.title.astype(str).str.cat(df_f.message.astype(str), sep='. ', na_rep ="")
        
        df_f["platform"] = "Facebook"
        
        df_f = df_f[['platform', "annoted", 'id', 'text', 'date', 'account_type', 'user_id',
           'user_screen_name_x', 'user_name_x', 'user_screen_name_y', 'user_name_y', 
                     'user_account_url',  'Type_entite', 'Genre', 'User_world',
           'Logics', 'Synthetic_logics', 'account_publication', "total_posts", 'ratio_posts', 'yearmonth']]



dftext = pd.concat([df_t, df_f]).reset_index()


# In[36]:


import bamboolib
dftext.loc[dftext["user_screen_name_y"].isnull() == True,"user_screen_name"]= dftext["user_screen_name_x"]
dftext.loc[dftext["user_screen_name_y"].isnull() == False,"user_screen_name"]= dftext["user_screen_name_y"]
dftext.loc[dftext["user_name_y"].isnull() == True,"user_name"]= dftext["user_name_x"]
dftext.loc[dftext["user_name_y"].isnull() == False,"user_name"]= dftext["user_name_y"]
dftext = dftext.drop(columns=["user_screen_name_x", "user_screen_name_y", "user_name_x", "user_name_y", "index"])
dftext


# ### Le fichier des périodes

# In[37]:


#a changer localement
#df0 = pd.read_csv(all_corpus, sep = "\t")
dfseg = pd.read_csv("/home/aymeric/python-scripts/anses_medialab/datas/segmentation_common_freq.csv", sep = "\t")

segment = dfseg[["yearmonth", "segm", "origin"]].rename(columns={"origin":"platform"})

segment["platform"] = segment["platform"].replace({"facebook":"Facebook"})


# In[38]:


dfs = dftext.merge(segment, how = "left", on = ["platform", "yearmonth"])
dfs["start_segment"] = dfs.groupby(['platform','segm'])["date"].transform('min')
dfs["end_segment"] = dfs.groupby(['platform','segm'])["date"].transform('max')
dfs["text"] =  dfs.text.apply(lambda x : x.replace('\r', ' '))
dfs["text"] =  dfs.text.apply(lambda x : x.replace('\n', ' '))


# In[39]:


dfs.to_csv(f'/home/aymeric/python-scripts/anses_medialab/datas/tweets_and_posts_with_annotated_account.csv', sep = ",", index=False, doublequote=True)
df.to_csv(f'/home/aymeric/python-scripts/anses_medialab/datas/all_annotated_account.csv', sep = ",", index=False)


# In[ ]:




