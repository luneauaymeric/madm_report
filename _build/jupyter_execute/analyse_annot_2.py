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
from myst_nb import glue


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


# In[4]:


# Chargement des données 

## Le corpus de textes avec comptes annotés
dftext = pd.read_csv(f'{path_base}tweets_and_posts_with_annotated_account.csv', sep = ",", doublequote=True, 
                    dtype={"user_id":str, "id":str})

## Les comptes annotés
df = pd.read_csv(f'/home/aymeric/python-scripts/anses_medialab/datas/all_annotated_account.csv', sep = ",",
          dtype={"user_id":str})


fb_account = df.loc[(df["platform"] == "Facebook") & (df["annoted"] == True)].reset_index()
tw_account = df.loc[(df["platform"] == "twitter") & (df["annoted"] == True)].reset_index()
#print(len(fb_account), len(tw_account))


# # La composition des corpus
# 
# ## Le nombre de pages, groupes (Facebook) et comptes annotés (Twitter)

# In[5]:


nb_fb_annoted_account = len(df.loc[(df["platform"] == "Facebook") & (df["annoted"] == True)]) #nombre de pages et groupes facebook annotés
nb_tw_annoted_account = len(df.loc[(df["platform"] == "twitter") & (df["annoted"] == True)]) #nombre de comptes twitter annotés


# In[6]:


#Facebook
nombre_de_posts = len(dftext.loc[(dftext["platform"] == "Facebook")].drop_duplicates(subset = ["id"]))
glue("nombre_de_posts", nombre_de_posts, display = False)

nb_group = len(dftext.loc[(dftext["platform"] == "Facebook") & (dftext["account_type"] =="facebook_group")].drop_duplicates(subset = ["user_id"])) #Nombre de groupes fb
nb_page = len(dftext.loc[(dftext["platform"] == "Facebook") & (dftext["account_type"] =="facebook_page")].drop_duplicates(subset = ["user_id"])) #Nombre de pages fb
nb_fb_account = nb_page + nb_group #Nombre total de sources fb
#nb_fb_account = len(dftext.loc[(dftext["platform"] == "Facebook")].drop_duplicates(subset = ["user_id"])) # Nombre total de comptes twitter

glue("nombre_de_posts", nombre_de_posts, display = False)
glue("nb_group", nb_group, display = False)
glue("nb_page", nb_page, display = False)
glue("nb_fb_account", nb_fb_account, display = False)

## twitter
nombre_de_tweets = len(dftext.loc[(dftext["platform"] == "twitter")].drop_duplicates(subset = ["id"]))
nb_tw_account = len(dftext.loc[(dftext["platform"] == "twitter")].drop_duplicates(subset = ["user_id"])) # Nombre total de comptes twitter

glue("nombre_de_tweets", nombre_de_tweets, display = False)
glue("nb_tw_account", nb_tw_account, display = False)


# Le corpus Twitter" est composé de {glue:text}`nombre_de_tweets` tweets publiés par {glue:text}`nb_tw_account`. Le "corpus Facebook" contient quant à lui {glue:}`nombre_de_post` publications produites par {glue:text}`nb_fb_account` comptes, dont {glue:text}`nb_page` pages et {glue:text}`nb_group`. Toutefois, les analyses ci-après portent uniquement sur les {glue:text}`nb_fb_annoted_account` groupes et pages facebook ainsi que les {glue:text}`nb_tw_annoted_account` comptes twitter que nous avons "annoté" à la main. Les comptes ont été choisis de la façon suivante :
# 
# 1. Pour chacun des corpus (twitter et facebook) de départ, nous avons conservé les comptes avec dix publications ou plus sur le sujet des pesticides.

# In[7]:


##sources twitter avec 10 publications ou plus
sub_tw_account = dftext.loc[(dftext["platform"] == "twitter") & (dftext["account_publication"] >= 10)].drop_duplicates(subset = ["user_id"])

#Nombre de comptes twitter avec 10 publications ou plus
nb_tw_with_10_posts = len(sub_tw_account)


glue("sub_tw_account", sub_tw_account, display = False)
glue("nb_tw_with_10_posts", nb_tw_with_10_posts, display = False)

##sources fb avec 10 publications ou plus
sub_fb_account = dftext.loc[(dftext["platform"] == "Facebook") & (dftext["account_publication"] >= 10)].drop_duplicates(subset = ["user_id"])
glue("sub_fb_account", sub_fb_account, display = False)


#Nombre de groupes fb avec 10 publications ou plus
nb_group_with_10_posts = len(sub_fb_account.loc[sub_fb_account["account_type"] == "facebook_group"]) 
glue("nb_group_with_10_posts", nb_group_with_10_posts, display = False)

#Nombre de pages fb avec 10 publications ou plus
nb_page_with_10_posts = len(sub_fb_account.loc[sub_fb_account["account_type"] == "facebook_page"])
glue("nb_page_with_10_posts", nb_page_with_10_posts, display = False)

#Nombre de sources fb avec 10 publications ou plus
nb_fb_with_10_posts = nb_page_with_10_posts + nb_group_with_10_posts
glue("nb_fb_with_10_posts", nb_fb_with_10_posts, display = False)

#Total des posts et tweets publiés par les comptes ayant 10 publications ou plus
sub_nb_post = len(dftext.loc[(dftext["platform"] == "Facebook") & (dftext["account_publication"] >= 10)].drop_duplicates(subset = ["id"]))
sub_nb_tweets = len(dftext.loc[(dftext["platform"] == "twitter") & (dftext["account_publication"] >= 10)].drop_duplicates(subset = ["id"]))
glue("sub_nb_post", sub_nb_post, display = False)
glue("sub_nb_post", sub_nb_tweets, display = False)

part_of_fb_account_with_10_post = round(nb_fb_with_10_posts/nb_fb_account*100, 1)
part_of_tw_account_with_10_tweets = round(nb_tw_with_10_posts/nb_tw_account*100, 1)
glue("part_of_fb_account_with_10_post", part_of_fb_account_with_10_post, display = False)
glue("part_of_tw_account_with_10_tweets", part_of_tw_account_with_10_tweets, display = False)

contrib_of_fb_account_with_10 = round(sub_nb_post/nombre_de_posts*100, 1)
contrib_of_tw_account_with_10 = round(sub_nb_tweets/nombre_de_tweets*100, 1)
glue("contrib_of_fb_account_with_10", contrib_of_fb_account_with_10, display = False)
glue("contrib_of_fb_account_with_10", contrib_of_fb_account_with_10, display = False)


# Cela représente un premier échantillon de {glue:text}`nb_page_with_10_posts` pages et {glue:text}`nb_group_with_10_posts` groupes Facebook, soit {glue:text}`nb_fb_with_10_posts` comptes. Cela représente {glue:text}`part_of_fb_account_with_10_post` % du nombre total de comptes. Ils sont à l'origine de {glue:text}`contrib_of_fb_account_with_10` % des posts du corpus.
# 
# Concernant le corpus twitter, on a {glue:text}`nb_tw_with_10_posts` comptes ayant publié dix tweets ou plus, soit {glue:text}`part_of_tw_account_with_10_tweets` % des comptes. Ces {glue:text}`nb_tw_with_10_posts` comptes ont publiés {glue:text}`contrib_of_tw_account_with_10` % des tweets du corpus ({glue:text}`sub_nb_tweets` sur {glue:text}`nombre_de_tweets`).

# 2. À partir de ce premier sous-ensemble, nous avons calculé un ratio donnant pour chaque compte la part des publications relatives aux pesticides (celles incluses dans le corpus) sur le total de ses publications. 

# In[8]:



## Sources twitter avec 10 publications ou plus et 1% de publications sur pesticides
subsub_tw_account = sub_tw_account.loc[(sub_tw_account["ratio_posts"] >= 1) & (sub_tw_account["ratio_posts"] <= 100)].drop_duplicates(subset =["user_id"])
glue("subsub_tw_account", subsub_tw_account, display = False)


#Nombre de sources twitter avec 10 publications ou plus et 1% de publications sur pesticides
nb_tw_with_1percent_posts = len(subsub_tw_account)
glue("nb_tw_with_1percent_posts", nb_tw_with_1percent_posts, display = False)


## Sources fb avec 10 publications ou plus et 1% de publications sur pesticides
subsub_fb_account = sub_fb_account.loc[sub_fb_account["ratio_posts"] >= 1].drop_duplicates(subset =["user_id"])
glue("subsub_fb_account", subsub_fb_account, display = False)

#Nombre de groupes fb avec 10 publications ou plus et 1% de publications sur pesticides
nb_group_with_1percent_posts = len(subsub_fb_account.loc[subsub_fb_account["account_type"] == "facebook_group"])
glue("nb_group_with_1percent_posts", nb_group_with_1percent_posts, display = False)

#Nombre de pages fb avec 10 publications ou plus et 1% de publications sur pesticides
nb_page_with_1percent_posts = len(subsub_fb_account.loc[subsub_fb_account["account_type"] == "facebook_page"])
glue("nb_group_with_1percent_posts", nb_group_with_1percent_posts, display = False)

#Nombre de sources fb avec 10 publications ou plus et 1% de publications sur pesticides
nb_fb_with_1percent_posts = nb_page_with_1percent_posts + nb_group_with_1percent_posts
glue("nb_fb_with_1percent_posts", nb_fb_with_1percent_posts, display = False)


#Total des posts et tweets ubliés par les comptes ayant au moins 1% de publications sur pesticides
onep_sub_nb_post =len(dftext.loc[(dftext["platform"] == "Facebook") & (dftext["ratio_posts"] >= 1) & (dftext["ratio_posts"] <= 100)].drop_duplicates(subset = ["id"]))
onep_sub_nb_tweets = len(dftext.loc[(dftext["platform"] == "twitter") & (dftext["ratio_posts"] >= 1) & (dftext["ratio_posts"] <= 100)].drop_duplicates(subset = ["id"]))
glue("onep_sub_nb_post", onep_sub_nb_post, display = False)
glue("onep_sub_nb_tweets", onep_sub_nb_tweets, display = False)

part_of_fb_account_with_1percent = round(nb_fb_with_1percent_posts/nb_fb_account*100, 1)
part_of_tw_account_with_1percent = round(nb_tw_with_1percent_posts/nb_tw_account*100, 1)
glue("part_of_fb_account_with_1percent", part_of_fb_account_with_1percent, display = False)
glue("part_of_tw_account_with_1percent", part_of_fb_account_with_1percent, display = False)

contrib_of_fb_account_with_10 = round(onep_sub_nb_post/nombre_de_posts*100, 1)
contrib_of_tw_account_with_10 = round(onep_sub_nb_tweets/nombre_de_tweets*100, 1)
glue("contrib_of_fb_account_with_10", contrib_of_fb_account_with_10, display = False)
glue("contrib_of_tw_account_with_10", contrib_of_tw_account_with_10, display = False)


# In[9]:


#import bamboolib
annoted_fb_account = df.loc[(df["platform"] == "Facebook") & (df["annoted"] == True)].reset_index()
annoted_tw_account = df.loc[(df["platform"] == "twitter") & (df["annoted"] == True)].reset_index()
glue("annoted_fb_account", annoted_fb_account, display = False)
glue("annoted_tw_account", annoted_tw_account, display = False)

fb_account = df.loc[(df["platform"] == "Facebook")].reset_index()
tw_account = df.loc[(df["platform"] == "twitter")].reset_index()

glue("fb_account", fb_account, display = False)
glue("tw_account", tw_account, display = False)

glypho_campagne = fb_account["user_name"].iloc[2]
glypho_campagne_nb_pest = fb_account["account_publication"].iloc[2]
glypho_campagne_nb = int(fb_account["total_posts"].iloc[2])
glypho_campagne_ratio = round(fb_account["ratio_posts"].iloc[2], 1)

glue("glypho_campagne", glypho_campagne, display = False)
glue("glypho_campagne_nb_pest", glypho_campagne_nb_pest, display = False)
glue("glypho_campagne_nb", glypho_campagne_nb, display = False)
glue("glypho_campagne_ratio", glypho_campagne_ratio, display = False)

alternatiba = fb_account["user_name"].iloc[2214]
alternatiba_nb_pest = fb_account["account_publication"].iloc[2214]
alternatiba_nb = fb_account["total_posts"].iloc[2214]
alternatiba_ratio = round(fb_account["ratio_posts"].iloc[2214], 1)

glue("alternatiba", alternatiba, display = False)
glue("alternatiba_nb_pest", alternatiba_nb_pest, display = False)
glue("alternatiba_nb", alternatiba_nb, display = False)
glue("alternatiba_ratio", alternatiba_ratio, display = False)

rebellion = fb_account["user_name"].iloc[500]
rebellion_nb_pest = fb_account["account_publication"].iloc[500]
rebellion_nb = int(fb_account["total_posts"].iloc[500])
rebellion_ratio = round(fb_account["ratio_posts"].iloc[500], 1)

glue("rebellion", rebellion, display = False)
glue("rebellion_nb_pest", rebellion_nb_pest, display = False)
glue("rebellion_nb", rebellion_nb, display = False)
glue("rebellion_ratio", rebellion_ratio, display = False)


# Ce faisant, on obtient dans le cas du corpus Facebook un second sous-ensemble composé de {glue:text}`nb_page_with_1percent_posts` pages et {glue:text}`nb_group_with_1percent_posts` groupes, soit {glue:text}`nb_fb_with_1percent_posts` comptes. Cela représente {glue:text}`part_of_fb_account_with_1percent` % des comptes, leurs contributions constituant {glue:text}`contrib_of_fb_account_with_10` % du corpus Facebook.
# 
# Dans le cas du corpus Twitter, les comptes qui ont 1% ou plus de leurs tweets sur les pesticides sont {glue:text}`onep_sub_nb_tweets`, soit {glue:text}`part_of_tw_account_with_1percent` %. Ils sont à l'origine de {glue:text}`contrib_of_tw_account_with_10` % des tweets du corpus.
# 
# 3. Enfin, après avoir triés les tableaux de données en fonction du ratio "publication sur les pesticides/total des publications" par ordre décroissant, nous avons annotés les {glue:text}`nb_fb_annoted_account` premiers groupes et pages Facebook, et les {glue:text}`nb_tw_annoted_account` premiers comptes Twitter. Illustrons cette dernière avec trois cas concrets (pris du corpus Facebook) :
# 
# > Cas 1: La page {glue:text}`glypho_campagne` a publié {glue:text}`glypho_campagne_nb_pest` posts relatifs à la question des pesticides sur un total de {glue:text}`glypho_campagne_nb`. On a donc un ratio de {glue:text}`glypho_campagne_ratio` %. Il fait par ailleurs partie du 2e plus gros contributeur du corpus. Il sera donc annoté.
# 
# > Cas 2 : La page {glue:text}`alternatiba` a publié {glue:text}`alternatiba_nb_pest` posts relatifs à la question des pesticides sur un total de {glue:text}`alternatiba_nb`. On a donc un ratio de {glue:text}`alternatiba_ratio` %. Le ratio est inférieur à 1 %, la page ne sera pas annotée.
# 
# > Cas 3 : La page {glue:text}`rebellion` a publié {glue:text}`rebellion_nb_pest` posts relatifs à la question des pesticides sur un total de {glue:text}`rebellion_nb`. On a donc un ratio de {glue:text}`rebellion_ratio` %. Bien que le ratio soit supérieur à 1 %, la page n'est que la cinq cent unième contributrice du corpus. Elle ne sera pas annotée.

# In[10]:


#Total des posts et tweets publiés par les comptes annotés
first_fb_account = subsub_fb_account.iloc[0:230]
first_tw_account = subsub_tw_account.iloc[0:464]

glue("first_fb_account", first_fb_account, display = False)
glue("first_tw_account", first_tw_account, display = False)

annoted_nb_post = np.sum(first_fb_account["account_publication"])
annoted_nb_tw = np.sum(first_tw_account["account_publication"])
glue("annoted_nb_post", annoted_nb_post, display = False)
glue("annoted_nb_tw", annoted_nb_tw, display = False)

part_of_fb_annoted_account = round(len(annoted_fb_account)/len(fb_account)*100,1)
part_of_tw_annoted_account = round(len(annoted_tw_account)/len(tw_account)*100,1)
glue("part_of_fb_annoted_account", part_of_fb_annoted_account, display = False)
glue("part_of_tw_annoted_account", part_of_tw_annoted_account, display = False)


contrib_of_fb_annoted_account = round(annoted_nb_post/nombre_de_posts*100, 1)
contrib_of_tw_annoted_account = round(annoted_nb_tw/nombre_de_tweets*100, 1)
glue("contrib_of_fb_annoted_account", contrib_of_fb_annoted_account, display = False)
glue("contrib_of_tw_annoted_account", contrib_of_tw_annoted_account, display = False)


# Au final, Les {glue:text}`nb_fb_annoted_account` comptes Facebook annotés représentent {glue:text}`part_of_fb_annoted_account` % des comptes. Ils sont auteurs de {glue:text}`contrib_of_fb_annoted_account` % des posts du corpus. Les {glue:text}`nb_tw_annoted_account` comptes twitter annotés forment pour leur part {glue:text}`part_of_tw_annoted_account` % des comptes et ont publié {glue:text}`contrib_of_tw_annoted_account`% des tweets du corpus. Ces différentes subdivisions sont illustrées par la figure ci-dessous.

# In[11]:


import squarify    # pip install squarify (algorithm for treemap)
import circlify #!pip install circlify


# In[12]:


size_all_corpus = len(dftext.drop_duplicates(subset = ["id"]))

data2 = [{'id': "all", 'name': 'Les corpus Facebook et Twitter', 'datum': size_all_corpus, "children" :[
         {'id': "corpus", 'name': 'Le corpus Facebook', 'datum': nombre_de_posts, "children":[
             {'id': "sub_corpus", "name": 'Les posts des comptes ayant au moins 10 publications', "datum" : sub_nb_post},
          {'id': "sub_sub_corpus", "name": 'Les posts des comptes ayant au moins 1% de leurs publications sur pesticides', 'datum': onep_sub_nb_post},
          {'id': "annoted_corpus", "name": 'Les posts des comptes facebook annotés', 'datum':annoted_nb_post}
         ]},
    {'id': "corpus_tw", 'name': 'Le corpus Twitter', 'datum': nombre_de_tweets, "children" :[
             {'id': "sub_corpus_tw", "name": 'Les tweets des comptes ayant au moins 10 publications', "datum" : sub_nb_tweets},
          {'id': "sub_sub_corpus_tw", "name": 'Les tweets des comptes ayant au moins 1% de leurs publications sur pesticides', 'datum': onep_sub_nb_tweets},
          {'id': "annoted_corpus_tw", "name": 'Les tweets des comptes twitter annotés', 'datum':annoted_nb_tw}
        ]}
]
         }]
# Compute circle positions thanks to the circlify() function
circles2 = circlify.circlify(
    data2, 
    show_enclosure=False
)


# In[13]:


name_legend = []

fig, ax = plt.subplots(figsize=(14,14))

# Title
fig.suptitle("Le poids des tweets et posts facebook publiés par les comptes annotés", fontsize= 20)

# Remove axes
ax.axis('off')

# Find axis boundaries
lim = max(
    max(
        abs(circle.x) + circle.r,
        abs(circle.y) + circle.r,
    )
    for circle in circles2
)
plt.xlim(-lim, lim)
plt.ylim(-lim, lim)

for circle in circles2:
    if circle.level == 2 and circle.ex["id"] == "corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x, y+0.5), r, alpha=0.5, linewidth=2, facecolor="Orange", edgecolor="black"))
        label = circle.ex["name"]
        name_legend.append(label)
        plt.annotate(label, (x, y) ,size = 20, xytext = (x-0.25,y+ 0.65))
        
    elif circle.level == 3 and circle.ex["id"] == "sub_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.1, y+0.4), r, alpha=1, linewidth=2, color="#FF8C00"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
    
    elif circle.level == 3 and circle.ex["id"] == "sub_sub_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x, y+0.03), r, alpha=0.9, linewidth=2, color="Tomato"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
            
    elif circle.level == 3 and circle.ex["id"] == "annoted_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.15, y-0.15), r, alpha=1, linewidth=2, color="OrangeRed"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
        
    elif circle.level == 2 and circle.ex["id"] == "corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x, y+0.3), r, alpha=0.4, linewidth=2, facecolor="Plum", edgecolor="black"))
        label = circle.ex["name"]
        name_legend.append(label)
        plt.annotate(label, (x,y) ,size = 20, xytext = (x,y+ 0.6))
        
    elif circle.level == 3 and circle.ex["id"] == "sub_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.2, y+0.1), r, alpha=1, linewidth=2, color="#DA70D6"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
    
    elif circle.level == 3 and circle.ex["id"] == "sub_sub_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.7, y-0.6), r, alpha=0.9, linewidth=2, color="#9932CC"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
            
    elif circle.level == 3 and circle.ex["id"] == "annoted_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.3, y-0.7), r, alpha=1, linewidth=2, color="DarkSlateBlue"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
         
                


name_legende = [
    "Le corpus de posts facebook",
    "Le corpus des tweets", 
    "Les posts annotés",
    "Les posts des comptes ayant au moins 10 publications",
    "Les posts des comptes ayant au moins 1% de leurs publications sur pesticides",
    
    "Les tweets des comptes ayant au moins 10 publications",
    "Les tweets des comptes ayant au moins 1% de leurs publications sur pesticides",
    "Les tweets des comptes annotés"]

plt.legend(name_legend, loc=3, fontsize = 12, ncol = 1)


# In[14]:


size_all_corpus = len(df)

data2 = [{'id': "all", 'name': 'Les comptes Facebook et Twitter', 'datum': size_all_corpus, "children" :[
         {'id': "corpus", 'name': 'Les comptes Facebook', 'datum': nb_fb_account, "children":[
             {'id': "sub_corpus", "name": 'Les comptes ayant au moins 10 publications', "datum" : nb_fb_with_10_posts},
          {'id': "sub_sub_corpus", "name": 'Les comptes ayant au moins 1% de leurs publications sur pesticides', 'datum': nb_fb_with_1percent_posts},
          {'id': "annoted_corpus", "name": 'Les comptes facebook annotés', 'datum':nb_fb_annoted_account}
         ]},
    {'id': "corpus_tw", 'name': 'Les comptes Twitter', 'datum': nb_tw_account, "children" :[
             {'id': "sub_corpus_tw", "name": 'Les comptes ayant au moins 10 publications', "datum" : nb_tw_with_10_posts},
          {'id': "sub_sub_corpus_tw", "name": 'Les comptes ayant au moins 1% de leurs publications sur pesticides', 'datum': nb_tw_with_1percent_posts},
          {'id': "annoted_corpus_tw", "name": 'Les comptes twitter annotés', 'datum':nb_tw_annoted_account}
        ]}
]
         }]
# Compute circle positions thanks to the circlify() function
circles2 = circlify.circlify(
    data2, 
    show_enclosure=False
)

name_legend = []

fig, ax = plt.subplots(figsize=(14,14))

# Title
fig.suptitle("Le poids des tweets et posts facebook publiés par les comptes annotés", fontsize= 20)

# Remove axes
ax.axis('off')

# Find axis boundaries
lim = max(
    max(
        abs(circle.x) + circle.r,
        abs(circle.y) + circle.r,
    )
    for circle in circles2
)
plt.xlim(-lim, lim)
plt.ylim(-lim, lim)

for circle in circles2:
    if circle.level == 2 and circle.ex["id"] == "corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x, y+0.5), r, alpha=0.5, linewidth=2, facecolor="Orange", edgecolor="black"))
        label = circle.ex["name"]
        name_legend.append(label)
        plt.annotate(label, (x, y) ,size = 20, xytext = (x-0.25,y+ 0.65))
        
    elif circle.level == 3 and circle.ex["id"] == "sub_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.1, y+0.4), r, alpha=1, linewidth=2, color="#FF8C00"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
    
    elif circle.level == 3 and circle.ex["id"] == "sub_sub_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.1, y+0.06), r, alpha=0.9, linewidth=2, color="Tomato"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
    elif circle.level == 3 and circle.ex["id"] == "annoted_corpus":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.2, y), r, alpha=1, linewidth=2, color="OrangeRed"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
        
    elif circle.level == 2 and circle.ex["id"] == "corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.05, y+0.05), r, alpha=0.4, linewidth=2, facecolor="Plum", edgecolor="black"))
        label = circle.ex["name"]
        name_legend.append(label)
        plt.annotate(label, (x,y) ,size = 20, xytext = (x-0.3,y+ 0.55))
        
    elif circle.level == 3 and circle.ex["id"] == "sub_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x-0.15, y), r, alpha=1, linewidth=2, color="#DA70D6"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
    
    elif circle.level == 3 and circle.ex["id"] == "sub_sub_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.85, y-0.75), r, alpha=0.9, linewidth=2, color="#9932CC"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
            
    elif circle.level == 3 and circle.ex["id"] == "annoted_corpus_tw":
        x, y, r = circle
        #print(r*circle.ex["datum"])
        ax.add_patch( plt.Circle((x+0.8, y-0.7), r, alpha=1, linewidth=2, color="DarkSlateBlue"))
        label = circle.ex["name"]
        name_legend.append(label)
        #plt.annotate(label, (x,y ) ,size = 20)
        
    
                


name_legende = [
    "Le corpus de posts facebook",
    "Le corpus des tweets", 
    "Les posts annotés",
    "Les posts des comptes ayant au moins 10 publications",
    "Les posts des comptes ayant au moins 1% de leurs publications sur pesticides",
    
    "Les tweets des comptes ayant au moins 10 publications",
    "Les tweets des comptes ayant au moins 1% de leurs publications sur pesticides",
    "Les tweets des comptes annotés"]

plt.legend(name_legend, loc=3, fontsize = 12, ncol = 1)


# In[ ]:




