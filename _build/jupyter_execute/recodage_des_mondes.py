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


# ### La liste des pages et groupes facebook

# In[5]:


#chargement de la liste des comptes facebook
fb_account = pd.read_csv(f"{path_base}datas_facebook/all_account_facebook.csv", sep ="\t", dtype={"account_id":str})
len(fb_account)


# ## Twitter
# 
# ### Le corpus de tweets

# In[6]:


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
    df=pd.read_csv(p)
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

# In[7]:


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

# In[8]:


# chargement de la liste des comptes twitter et facebook annotés.
df0 = pd.read_csv("fb_and_tw_annotated_account.csv", sep = "\t", dtype={"user_id":str})
df0.groupby(["platform"]).size()


# In[9]:



t = tweets[["user_id", "user_screen_name"]].drop_duplicates()

df_tweet = pd.read_csv(f"{aymeric}tweets_pesticides/annotation_compte_twitter.csv", sep = ",", dtype ={"user_id":str} )
df_tweet.columns
df_tweet["user_id"]


# In[106]:


df_t = df_tweet.drop(columns = ["user_id"]).merge(t, how = "left", on = ["user_screen_name"])
df_tt = df_t.drop(columns = ["user_screen_name", "user_name",
                            'user_location',
       'user_verified', 'user_description', 'user_url', 'user_image',
       'user_tweets', 'user_followers', 'user_friends', 'user_likes',
       'user_lists', 'user_created_at', 'user_timestamp_utc']).merge(tw_account, how = "left", on = ["user_id"])

df_tt = df_tt[['user_id', 'user_name', 'user_screen_name', 
        'Type_entite', 'Genre', 'User_role2',
       'User_role', 'Orientation', 'thematique', 'theme_rec', 'theme_rec2',
       'Scale', 'Locality', 'associated_fb_url', 'associated_website',
       'Stake_holder_anses', 'user_account_url', 
       'quoted_user_id',
       'world', 'theme',
       'total_posts', 'account_publication', 'ratio_posts',
       'user_description', 'user_location', 'user_url',
       'user_created_at', 'user_timestamp_utc', 'cluster_id', 'data', 'Comment']]

df_tt["world"] = df_tt["world"].str.replace('\{"choices": \[', "")
df_tt["world"] = df_tt["world"].str.replace('\]\}', "")
df_tt["world"] = df_tt["world"].str.replace('\"', "")
df_tt["world"] = df_tt["world"].str.split(',')
df_tt

df_tt["User_world"] = np.nan
df_tt["User_world2"] =np.nan

for x, world in enumerate(df_tt["world"]):
    if type(world) is float:
        df_tt["User_world"].iloc[x] =np.nan
        df_tt["User_world2"].iloc[x] =np.nan
        
    else:
        if len(world) == 1:
            df_tt["User_world"].iloc[x] = world[0].strip()
            df_tt["User_world2"].iloc[x] = world[0].strip()
        elif len(world) > 1 :
            df_tt["User_world"].iloc[x] = world[0].strip()
            df_tt["User_world2"].iloc[x] = world[1].strip()
        elif len(world) == 0 :
            df_tt["User_world"].iloc[x] =np.nan
            df_tt["User_world2"].iloc[x] =np.nan

df_tt.loc[(df_tt["User_role"].isnull() == True) & (df_tt["User_role2"] == "Commentateur"), "User_role3"] = df_tt["User_role2"]
df_tt.loc[(df_tt["User_role"].isnull() == False), "User_role3"] = df_tt["User_role"]

df_tt = df_tt[['user_id','user_screen_name','user_name', 'Comment', 'Type_entite', 'Genre','User_role2',
               'User_role3', 'world','User_world', 'User_world2', 'Orientation', 'theme', 
               'user_description', 'associated_fb_url','associated_website', 'user_account_url',
               'user_location', 'total_posts', 'account_publication', 'ratio_posts',
               'user_created_at']]

df_tt = df_tt.rename(columns={'theme' : 'main_thematic', 'User_role3':'User_role'})

df_tt["platform"] = "twitter"

df_tt


# In[107]:


df_fb = pd.read_csv(f"{aymeric}datas_facebook/annotation_compte_facebook.csv", sep = ",")
df_fb = df_fb.loc[df_fb["Type_entite"].isna() == False]
df_fb["User_world"] = np.nan
df_fb["User_world2"] =np.nan

df_fb["world"] = df_fb["Mondes"].str.split('|')

for x, world in enumerate(df_fb["world"]):
    if type(world) is float:
        df_fb["User_world"].iloc[x] =np.nan
        df_fb["User_world2"].iloc[x] =np.nan
        
    else:
        if len(world) == 1:
            df_fb["User_world"].iloc[x] = world[0]
            df_fb["User_world2"].iloc[x] = world[0]
        elif len(world) > 1 :
            df_fb["User_world"].iloc[x] = world[0]
            df_fb["User_world2"].iloc[x] = world[1]
        elif len(world) == 0 :
            df_fb["User_world"].iloc[x] =np.nan
            df_fb["User_world2"].iloc[x] =np.nan

df_fb = df_fb[['account_url', 'account_name', 'account_handle', 'Comment', 'Type_entite',
       'Genre', 'User_role', 'world','User_world','User_world2', 'account_description', 'Orientation',
       'thematique', 'account_a_propos','associated_twitter', 'associated_website',
        'account_id', 'account_platform', 
        'account_type', 'account_page_admin_top_country',
       'account_publication', 'total_posts', 'ratio_posts', ]]

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
})


# In[142]:


df = pd.concat([df_tt, df_fb]).reset_index()
#df.to_csv("fb_and_tw_annotated_account.csv", sep = "\t")


# In[143]:


dd = df.loc[(df["platform"] == "Facebook") & (df["Type_entite"]=="Organisation") & (df["User_world"] == "Les mondes des causes collectives")]
dd

dd = df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Exploitation agricole") & 
           (df["User_world2"] == "Les mondes institutionnels")]

dd


# In[144]:


df.groupby(["User_role"]).size()


# ## Recodage de catégories (facebook)

# In[251]:


df['User_world'] = df['User_world'].replace(
    {"Les mondes rationalistes" : "Les mondes des causes collectives", #terme à remplacer : nouvelle catégorie
    "Les mondes critiques" : "Les mondes des causes collectives",
     "Les mondes scientifiques" : "Les mondes scientifiques et techniques"})


df.loc[(df["User_role"]=="Commentateur"), "User_world"] = df['User_world'].replace(
    {"Les mondes rationalistes" : "Les mondes critiques"})

df.loc[(df["user_screen_name"]=="agasagabon"), "User_world2"] = df['User_world2'].replace(
    {"Les mondes institutionnels" : "Les mondes scientifiques et techniques"})

df.loc[(df["user_screen_name"]=="ofaburkinafaso"), "User_role"] = "Expertise"

df.loc[(df["user_screen_name"]=="ofaburkinafaso"), "User_world"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="ManufacturedesLettres"), "User_role"] = df['User_role'].replace(
    {"Commentateur" : "Média"})

df.loc[(df["user_screen_name"]=="CyrilDionOfficiel"), "User_role"] = "Auteur.trice et réalisateur.trices"

df.loc[(df["user_screen_name"]=="gui.bodin"), "User_role"] = "Auteur.trice et réalisateur.trices"
df.loc[(df["user_screen_name"]=="gui.bodin"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="gui.bodin"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_name"]=="Valérie Murat"), "User_role"] = "Lanceur.se d'alerte"
df.loc[(df["user_name"]=="Valérie Murat"), "User_world"] = "Les mondes des causes collectives"
df.loc[(df["user_name"]=="Valérie Murat"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="insecticidemonamour"), "User_role"] = "Média"
df.loc[(df["user_screen_name"]=="insecticidemonamour"), "Type_entite"] = "Organisation"
df.loc[(df["user_screen_name"]=="insecticidemonamour"), "Genre"] = np.nan

df.loc[(df["user_screen_name"]=="c.gruffat"), "User_role"] = "Partis et personnalités politiques"

df.loc[(df["user_screen_name"]=="Agriculture.Environnement"), "User_role"] = "Média"
df.loc[(df["user_screen_name"]=="Agriculture.Environnement"), "Type_entite"] = "Organisation"
df.loc[(df["user_screen_name"]=="Agriculture.Environnement"), "Genre"] = np.nan
df.loc[(df["user_screen_name"]=="Agriculture.Environnement"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="Agriculture.Environnement"), "User_world2"] = "Les mondes agricoles"


df.loc[(df["platform"] == "Facebook") & (df["User_role"] == "Expertise"), "User_world2"] = df['User_world']

df.loc[(df["platform"] == "Facebook") & (df["User_role"] == "Expertise"), "User_world"] = "Les mondes scientifiques et techniques"

df.loc[(df["platform"] == "Facebook") & 
       (df["User_role"].str.contains("agri")) & 
       (df["User_world"] != "Les mondes agricoles"), "User_world2"] = df['User_world']

df.loc[(df["platform"] == "Facebook") & 
       (df["User_role"].str.contains("agri")) & 
       (df["User_world"] != "Les mondes agricoles"), "User_world"] = "Les mondes agricoles"

df.loc[(df["platform"] == "Facebook") & 
       (df["User_role"] == "Groupement de producteurs et productrices agricoles"), 
       "User_world2"] = "Les mondes économiques"

df.loc[(df["platform"] == "Facebook") & 
       (df["User_role"] == "Syndicat agricole"), 
       "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["platform"] == "Facebook") & 
       (df["User_role"] == "chambre d'agriculture"), 
       "User_world2"] = "Les mondes institutionnels"

df['User_role'] = df['User_role'].replace(
    {"Exploitant.e agricole" : "Exploitation agricole"})


# In[252]:


df.loc[(df["user_screen_name"]=="FondationNH"), "User_world"] = "Les mondes des causes collectives"
df.loc[(df["user_screen_name"]=="FondationNH"), "User_world2"] = "Les mondes politiques"


df.loc[(df["user_screen_name"]=="Doc_IAMM"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="Doc_IAMM"), "User_world2"] = "Les mondes institutionnels"

df.loc[(df["user_screen_name"]=="Alan_Mingh"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="Alan_Mingh"), "User_world2"] = "Les mondes agricoles"


df.loc[(df["user_screen_name"]=="INRAE_France"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="INRAE_France"), "User_world2"] = "Les mondes institutionnels"



df.loc[(df["user_screen_name"]=="abbechartier"), "User_world"] = "Les mondes critiques"
df.loc[(df["user_screen_name"]=="abbechartier"), "User_world2"] = "Les mondes agricoles"


df.loc[(df["user_screen_name"]=="Atmo_na"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="Atmo_na"), "User_world2"] = "Les mondes institutionnels"


df.loc[(df["user_screen_name"]=="Alter_Pesticide"), "User_role"] = "Evènement"
df.loc[(df["user_screen_name"]=="Alter_Pesticide"), "User_world2"] = "Les mondes de l'information"

df.loc[(df["user_screen_name"]=="LCJAMET"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="LCJAMET"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="algrap"), "User_world"] = "Les mondes critiques"
df.loc[(df["user_screen_name"]=="algrap"), "User_world2"] = "Les mondes critiques"

df.loc[(df["user_screen_name"]=="AllassacPBVDM"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="AllassacPBVDM"), "User_world2"] = "Les mondes des causes collectives"


df.loc[(df["user_screen_name"]=="FestimagesNatur"), "User_role"] = "Auteur.trice et réalisateur.trices"
df.loc[(df["user_screen_name"]=="FestimagesNatur"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="FestimagesNatur"), "User_world2"] = "Les mondes de l'information"

df.loc[(df["user_screen_name"]=="Naismtz7"), "User_role"] = "Journalistes"
df.loc[(df["user_screen_name"]=="Naismtz7"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="Naismtz7"), "User_world2"] = "Les mondes de l'information"

df.loc[(df["user_screen_name"]=="brunocrussol"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="brunocrussol"), "User_world2"] = "Les mondes de l'information"


df.loc[(df["user_screen_name"]=="AEGRW"), "User_role"] = "Média"
df.loc[(df["user_screen_name"]=="AEGRW"), "Type_entite"] = "Organisation"
df.loc[(df["user_screen_name"]=="AEGRW"), "Genre"] = np.nan
df.loc[(df["user_screen_name"]=="AEGRW"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="AEGRW"), "User_world2"] = "Les mondes agricoles"



df.loc[(df["user_screen_name"]=="bruno_peiffer"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="bruno_peiffer"), "User_world2"] = "Les mondes agricoles"


df.loc[(df["user_screen_name"]=="pas_jul"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="pas_jul"), "User_world2"] = "Les mondes de l'information"

df.loc[(df["user_screen_name"]=="VAgronomie"), "User_role"] = "Expertise"
df.loc[(df["user_screen_name"]=="VAgronomie"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="VAgronomie"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="emma_ducros"), "User_role"] = "Journalistes"
df.loc[(df["user_screen_name"]=="emma_ducros"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="emma_ducros"), "User_world2"] = "Les mondes de l'information"



df.loc[(df["user_screen_name"]=="benco_c"), "User_world"] = np.nan
df.loc[(df["user_screen_name"]=="benco_c"), "User_world2"] = np.nan

df.loc[(df["user_screen_name"]=="Matadon_"), "User_role"] = "Chercheur.se"
df.loc[(df["user_screen_name"]=="Matadon_"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="Matadon_"), "User_world2"] = "Les mondes agricoles"


df.loc[(df["user_screen_name"]=="CARIasbl"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="CARIasbl"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="iiiprs"), "User_role"] = "Expertise"
df.loc[(df["user_screen_name"]=="iiiprs"), "User_world"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="iiiprs"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="sabine_38"), "User_role"] = df["User_role2"]
df.loc[(df["user_screen_name"]=="sabine_38"), "User_world"] = "Les mondes critiques"
df.loc[(df["user_screen_name"]=="sabine_38"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="Carolinaily"), "User_role"] = df["User_role2"]
df.loc[(df["user_screen_name"]=="Carolinaily"), "User_world"] = "Les mondes critiques"
df.loc[(df["user_screen_name"]=="Carolinaily"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="OGMPourTous"), "User_role"] = "Militant.e"
df.loc[(df["user_screen_name"]=="OGMPourTous"), "User_world"] = "Les mondes critiques"
df.loc[(df["user_screen_name"]=="OGMPourTous"), "User_world2"] = "Les mondes critiques"

df.loc[(df["user_screen_name"]=="SBiodiversite"), "User_world"] = "Les mondes des causes collectives"


df.loc[(df["user_screen_name"]=="MyGandolphe"), "User_world"] = "Les mondes politiques"
df.loc[(df["user_screen_name"]=="MyGandolphe"), "User_world2"] = "Les mondes institutionnels"


df.loc[(df["user_screen_name"]=="petatJM"), "User_world2"] = "Les mondes économiques"

df.loc[(df["platform"] == "twitter") & 
       (df["User_role"] == "mouvements sociaux") & 
       (df["User_world"] == "Les mondes agricoles"), "User_world2"] = df['User_world']

df.loc[(df["platform"] == "twitter") & 
       (df["User_role"] == "mouvements sociaux") & 
       (df["User_world2"] == "Les mondes agricoles"), "User_world"] = "Les mondes des causes collectives"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Activités industrielles ou commerciales") & 
           (df["User_world"] == "Les mondes agricoles"), "User_world2"] = df['User_world']

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Activités industrielles ou commerciales") & 
           (df["User_world2"] == "Les mondes agricoles"), "User_world"] = "Les mondes économiques"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Commentateur") & 
           (df["User_world"] == "Les mondes agricoles"), "User_world2"] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Commentateur") & 
           (df["User_world2"] == "Les mondes agricoles"), "User_world"] = "Les mondes critiques"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role2"] == "Institutions et organismes publics") & 
           (df["User_world"] == "Les mondes agricoles"), "User_role"] = "chambre d'agriculture"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world"] == "Les mondes agricoles"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world2"] == "Les mondes agricoles"), 'User_world'] = "Les mondes de l'information"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Journalistes") & 
           (df["User_world"] == "Les mondes agricoles"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Journalistes") & 
           (df["User_world2"] == "Les mondes agricoles"), 'User_world'] = "Les mondes de l'information"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Partis et personnalités politiques") & 
           (df["User_world"] == "Les mondes agricoles"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Partis et personnalités politiques") & 
           (df["User_world2"] == "Les mondes agricoles"), 'User_world'] = "Les mondes politiques"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Journalistes") & 
           (df["User_world"] == "Les mondes critiques"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Journalistes") & 
           (df["User_world2"] == "Les mondes critiques"), 'User_world'] = "Les mondes de l'information"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Partis et personnalités politiques") & 
           (df["User_world"] == "Les mondes critiques"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Partis et personnalités politiques") & 
           (df["User_world2"] == "Les mondes critiques"), 'User_world'] = "Les mondes politiques"

df.loc[(df["platform"] == "twitter") & 
            (df["User_world"] == "Les mondes critiques") & 
           (df["Type_entite"] == "Personne"), 'User_role'] = "Militant.e"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world"] == "Les mondes des causes collectives"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world2"] == "Les mondes des causes collectives"), 'User_world'] = "Les mondes de l'information"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world"] == "Les mondes économiques"), 'User_world2'] = df["User_world"]

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Média") & 
           (df["User_world2"] == "Les mondes économiques"), 'User_world'] = "Les mondes de l'information"

df["User_world"] = df['User_world'].replace(
    {"Les mondes critiques": "Les mondes des causes collectives"})


d = df.loc[df["platform"]== "twitter"].groupby(['User_world', 'User_role']).size()
print(d.to_string())


# ### Recodage de "User_world2"

# In[304]:


df["User_world2"] = df['User_world2'].replace(
    {"Les mondes critiques": "Les mondes des causes collectives",
    "Les mondes rationalistes": "Les mondes des causes collectives",
    "Les mondes scientifiques": "Les mondes scientifiques et techniques"})

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Exploitation agricole") & 
           (df["User_world"] == "Les mondes agricoles") & 
       (df["User_world2"] == "Les mondes agricoles"), 'User_world2'] = "Les mondes économiques"

df.loc[(df["platform"] == "twitter") & 
           (df["User_world"] == "Les mondes économiques") & 
       (df["User_world2"] == "Les mondes institutionnels"), 'User_world2'] = "Les mondes des causes collectives"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Exploitation agricole") & 
           (df["User_world"] == "Les mondes agricoles") & 
       (df["User_world2"] == "Les mondes de l'information"), 'User_world2'] = "Les mondes économiques"

df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Exploitation agricole") & 
           (df["User_world"] == "Les mondes agricoles") & 
       (df["User_world2"] == "Les mondes politiques"), 'User_world2'] = "Les mondes des causes collectives"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Syndicat agricole") & 
           (df["User_world"] == "Les mondes agricoles") & 
       (df["User_world2"] == "Les mondes politiques"), 'User_world2'] = "Les mondes des causes collectives"


df.loc[(df["platform"] == "twitter") & 
            (df["User_role"] == "Auteur.trice et réalisateur.trices") & 
           (df["User_world"] == "Les mondes de l'information") & 
       (df["User_world2"] == "Les mondes de l'information"), 'User_world2'] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="aympontier"), "User_role"] = "Journalistes"
df.loc[(df["user_screen_name"]=="aympontier"), "User_world2"] = "Les mondes de l'information"

df.loc[(df["user_screen_name"]=="CathyLafon1"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="stvdg456"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="tooxique"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="Bioalaune_com"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="AlerteEnvironne"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="bioaddict"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="sosbiodiversite"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="NatureObsession"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="consoglobe"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="ID_LinfoDurable"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="CDURABLE"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="RDurables"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="InfosVertes"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="Zurbains"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="bidulou"), "User_world2"] = "Les mondes scientifiques et techniques"

df.loc[(df["user_screen_name"]=="CabinetGimalac"), "User_role"] = "Avocat.e"
df.loc[(df["user_screen_name"]=="CabinetGimalac"), "User_world2"] = "Les mondes économiques"

df.loc[(df["user_screen_name"]=="SebastienM"), "User_role"] = "Avocat.e"
df.loc[(df["user_screen_name"]=="SebastienM"), "User_world2"] = "Les mondes économiques"

df.loc[(df["user_screen_name"]=="RuffinengoE"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="IREPSNA"), "User_world2"] = "Les mondes scientifiques et techniques"

df.loc[(df["user_screen_name"]=="agence_eau_RM"), "User_world2"] = "Les mondes scientifiques et techniques"

df.loc[(df["user_screen_name"]=="ghislodi"), "User_world2"] = "Les mondes agricoles"

df.loc[(df["user_screen_name"]=="snefsuOFB"), "User_world2"] = "Les mondes des causes collectives"
df.loc[(df["user_screen_name"]=="snefsuOFB"), "User_world"] = "Les mondes des causes collectives"



df.loc[(df["user_screen_name"]=="ThierrySto"), "User_world2"] = "Les mondes scientifiques et techniques"
df.loc[(df["user_screen_name"]=="ThierrySto"), "User_world"] = "Les mondes agricoles"

d = df.loc[(df["platform"]== "twitter") ].groupby(['User_world', 'User_world2']).size()
print(d.to_string())


# In[331]:


df.loc[(df["user_screen_name"]=="apiculteuse"), "User_world2"] = "Les mondes des causes collectives"
df.loc[(df["user_screen_name"]=="apiculteuse"), "User_world"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="sos.biodiversite"), "User_world2"] = "Les mondes des causes collectives"
df.loc[(df["user_screen_name"]=="sos.biodiversite"), "User_world"] = "Les mondes de l'information"
df.loc[(df["user_screen_name"]=="sos.biodiversite"), "associated_fb_url"] = "https://twitter.com/sosbiodiversite"

df.loc[(df["user_screen_name"]=="LeJardinVivant"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="monecologis"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="AJE.asso"), "User_world2"] = "Les mondes des causes collectives"

df.loc[(df["user_screen_name"]=="langouet35"), "User_world2"] = "Les mondes politiques"

df.loc[(df["user_screen_name"]=="SemaineAlterPesticides"), "User_world2"] = "Les mondes de l'information"



d = df.loc[(df["platform"]== "Facebook")].groupby(['User_world2', 'User_role']).size()
print(d.to_string())


# In[347]:


dd = df.loc[(df["platform"] == "Facebook") & (df["Type_entite"]=="Organisation") & (df["User_world"] == "Les mondes des causes collectives")]
dd

dd = df.loc[(df["platform"] == "Facebook") & 
            #(df["User_role"] == "Média") & 
            (df["User_world"] == "Les mondes agricoles")&
           (df["User_world2"] == "Les mondes institutionnels")]


dd = df.loc[(df["User_world"].isnull())]
dd


# In[346]:


df.to_csv("fb_and_tw_annotated_account2.csv", sep = "\t")

