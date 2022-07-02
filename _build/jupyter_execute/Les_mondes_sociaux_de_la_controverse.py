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
#import bamboolib


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


dftext = pd.read_csv(f'{path_base}tweets_and_posts_with_annotated_account.csv', sep = ",", doublequote=True)
#df.to_csv(f'/home/aymeric/python-scripts/anses_medialab/datas/all_annotated_account.csv', sep = ",", index=False)
len(dftext)


# In[5]:


#df.to_csv(f'/home/aymeric/python-scripts/anses_medialab/datas/all_annotated_account.csv', sep = ",", index=False)
df = pd.read_csv(f'{path_base}all_annotated_account.csv', sep = ",")


# # Le système d'annotation des comptes
# 
# L'annotation des comptes, c'est-à-dire leur description, s'est faite en renseignant une dizaine de variables. Certaines variables proviennent de catégories choisies par les propriétaires des comptes -- on peut parler de catégories indigènes--, tandis que d'autres proviennent de nos propres catégorisation.
# 
# Par exemple, si on se rend dans la section "à propos" de la page facebook <a href="https://www.facebook.com/CampagneGlyphosate83/about">"Campagne Glyphosate 83"</a>, on peut lire dans la rubrique "général", sous le nombre de personnes "aimant la page" et le nombre d'abonnés, qu'elle est définie comme une "cause". Cette étiquette correspond à la "catégorie" que toute personne créant une page facebook doit obligatoirement renseignée. Une page peut appartenir au maximum à trois catégories (<a href="https://www.facebook.com/pages/creation?ref_type=comet_home">voir "créer une page facebook"</a>). En revanche, ce champ n'existe pas pour les groupes. Il n'existe pas non plus sur Twitter d'équivalents exacts aux catégories proposées par Facebook. On utilisera à la place les hashtags utilisés par auteur.trices dans la description de leur compte.
# 
# De notre côté, nous avons qualifié la "Campagne Glyphosate 83" comme étant un mouvement social de lutte contre les pesticides et plus particluièrement le glyphosate. En tant que mouvement social, nous avons par ailleurs classé la "Campagne Glyphosate 83" parmi "Les mondes des causes collectices". Nous avons utilisés le même jeu de variables et de modalités pour décrire les pages, les groupes et les comptes twitter. Cette qualification prend en compte quatre types d'attribut:
# 
# - Le premier attribut dit si la page, le groupe<a href="#note1" id="note1ref"><sup>1</sup></a>  ou le compte twitter représente une personne ou une "organisation". Lorsque c'est une personne, nous précisons lorsque c'est possible, s'il s'agit d'un homme ou d'une femme.
# 
# - Le second attribut vise à classer les comptes en fonction du **"rôle"** qu'ils endossent. Autrement dit, au nom de quoi ou de qui une page, un groupe ou un compte twitter s'expriment-ils ? Est-ce au nom d'un mouvement social ou d'une instituation, comme journaliste, en tant qu'agriculteur.trice ou personnalité politque ?
# 
# - Le deuxième attribut est plus transparent (dans son intitulé). Il concerne la **thématique** des pages, groupes et comptes Twitter: dénoncent-ils l'utilisation des pesticides ? Défendent-ils au contraire l'agriculture conventionnelle ? S'agit-il d'un mouvement de reconnaissances de victimes ?
# 
# - Enfin le quatrième attribut propose de qualifier les **mondes** auxquels les pages, groupes et comptes observés semblent appartenir. Cette qualification s'appuie sur la définition au préalable du rôle. Ainsi, nous avons considéré que toutes les pages qualifiées de "mouvement social" ou de "contre-mouvement social"<a href="#note2" id="note2ref"><sup>2</sup></a> appartenaient aux "mondes des causes collectives". Une page, un groupe ou un compte peut appartenir au maximum à deux "mondes". Une agricultrice qui manifeste une appartenance à un quelconque mouvement social sera classée à la fois dans "les mondes agricoles" et dans "les mondes des causes collectives".
# 
# 
# Le "rôle", les "mondes" et la "thématique" ont permi de synthétiser un premier codage extensif. "Extensif" au sens où les catégories étaient créée au fur et à mesure de l'exploration des pages, des groupes et des comptes. Nous nous sommes par ailleurs appuyé uniquement sur les éléments de description<a href="#note3" id="note3ref"><sup>3</sup></a>. Ces variables ne disent donc rien du contenu des publications.
# 
# 
# 
# 
# <a id="note1" href="#note1ref"><sup>1</sup></a> Dans la suite du document, les termes de "page" et "groupe" font exclusivement référence aux "pages et groupes Facebook"
# 
# <a id="note2" href="#note2ref"><sup>2</sup></a> La catégorie de "contre-mouvement sociale", appelée de cette façon en référence à Snow et Benford, sert à qualifier les pages, groupes ou comptes créés en réaction à des mouvements tels que "Générations fututres", "Nous voulons des coquelicots" ou "Greenpeace". Comme exemple de contre-mouvements sociaux, on peut citer "Écologie et rationalisme" ou "Notre futur dans les champs".
# 
# <a id="note3" href="#note3ref"><sup>3</sup></a> Dans le cas de Twitter, les données collectées à l'aide de minet contiennent une colonne nommée "user_description" correspondant à la phrase de présentation qui est affichée, lorsqu'elle existe, sous l'avatar et le pseudonyme de l'utilisateur.trice. Dans le cas de Facebook, les éléments de descriptions ne font pas partie du corpus de données récupérées à partir de CrowTangle. Cela suppose donc de visiter manuellement chaque compte pour accéder à son "à propos" et ses éléments de description.

# ![images/process.png](images/process.png)

# ![images/description_compte_page_group.png](images/description_compte_page_group.png)

# In[6]:


import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

d = df.drop_duplicates(subset=["user_id", "User_world"]).dropna(subset=['User_world'])
#d["count_user"]= d.groupby(['User_world'])['platform'].transform('count')


fig = px.treemap(d, path=['platform','User_world'],color='User_world', branchvalues = "total")
fig


# In[7]:


fig = px.treemap(df.drop_duplicates(subset=["user_id","User_world", "Logics"]).dropna(subset=["User_world",'Logics']), path=["platform", "User_world",'Logics'], color='Logics')
fig


# # Visualisation de la série temporelle

# In[8]:


d = df.dropna(subset=["User_world"])
d["User_world"].unique()


# In[9]:


liste_world = [x for x in d["User_world"].unique()]
smoothing_scale= 90
context_scale=182
freq_threshold=100
liste_world


# In[10]:


liste_world= [
 'Les mondes des causes collectives',
 'Les mondes agricoles',
 "Les mondes de l'information",
 'Les mondes politiques']



plt.rcParams["figure.figsize"] = (30,40)
fig, axes = plt.subplots(len(liste_world), 2)
#fig.suptitle('A single ax with no data')

for i, x in enumerate(liste_world):
    #print(i, x)
    df_time = dftext.loc[(dftext["platform"]=="twitter") & (dftext["User_world"]==x)].groupby(["date"]).agg(world_count = ("id","size")).reset_index()
    #first data is smoothed over one week
    df_time['count_roll']= df_time['world_count'].rolling(smoothing_scale).mean()
    
    df_time['date'] = pd.to_datetime(df_time['date'], infer_datetime_format=True)
    
    sns.lineplot(ax=axes[i,0], x="date", y="count_roll", data=df_time)
    
    axes[i,0].set_title(f'Tweets published by authors belonging to {x}', fontsize = 20)
    axes[i,0].set_xlabel("Date", fontsize = 14)
    axes[i,0].set_ylabel(f"Rolling average over {smoothing_scale}", fontsize = 14)
    
    
    df_time = dftext.loc[(dftext["platform"]=="Facebook") & (dftext["User_world"]==x)].groupby(["date"]).agg(world_count = ("id","size")).reset_index()
    #first data is smoothed over one week
    df_time['count_roll']= df_time['world_count'].rolling(smoothing_scale).mean()
    
    df_time['date'] = pd.to_datetime(df_time['date'], infer_datetime_format=True)
    
    sns.lineplot(ax=axes[i,1], x="date", y="count_roll", data=df_time, color='r')
    
    axes[i,1].set_title(f'Posts published by authors belonging to {x}', fontsize = 20)
    axes[i,1].set_xlabel("Date", fontsize = 14)
    axes[i,1].set_ylabel(f"Rolling average over {smoothing_scale}", fontsize = 14)
    
    axes[i,0].legend(labels=["Twitter"])
    axes[i,1].legend(labels=["Facebook"])


# In[11]:


liste_logics = [x for x in d["Logics"].unique()]
liste_logics


# In[12]:



d = df.dropna(subset=["Synthetic_logics"])
liste_logics = [
 'Positivistes',
 'Défense des agricultures conventionnelles',
 'Défense des agricultures non-conventionnelles',
 'Lutte contre les pesticides']
liste_logics = ['Positivistic_logic',
 'Pesticide_free_agriculture']

fig, axes = plt.subplots(2, len(liste_logics))

for i, x in enumerate(liste_logics):
    df_time = dftext.loc[(dftext["platform"]=="twitter") & (dftext["Synthetic_logics"]==x)].groupby(["date"]).agg(world_count = ("user_id","size")).reset_index()
    #first data is smoothed over one week
    df_time['count_roll']= df_time['world_count'].rolling(smoothing_scale).mean()
    df_time['ave'] =  (df_time["world_count"] / df_time["world_count"].max())#.rolling(shortmean).mean()
    df_time['roll' ] = df_time["ave"].rolling(smoothing_scale).mean()
    df_time['date'] = pd.to_datetime(df_time['date'], infer_datetime_format=True)
    
    sns.lineplot(ax=axes[0,i], x="date", y="count_roll", data=df_time)
    
    axes[0,i].set_title(f'{x}', fontsize=20)
    axes[0,i].set_xlabel("Date", fontsize = 14)
    axes[0,i].set_ylabel(f"Rolling average over {smoothing_scale}", fontsize = 14)
    
    
    df_time = dftext.loc[(dftext["platform"]=="Facebook") & (dftext["Synthetic_logics"]==x)].groupby(["date"]).agg(world_count = ("user_id","size")).reset_index()
    #first data is smoothed over one week
    df_time['count_roll']= df_time['world_count'].rolling(smoothing_scale).mean()
    df_time['ave'] =  (df_time["world_count"] / df_time["world_count"].max())#.rolling(shortmean).mean()
    df_time['roll' ] = df_time["ave"].rolling(smoothing_scale).mean()
    df_time['date'] = pd.to_datetime(df_time['date'], infer_datetime_format=True)
    
    sns.lineplot(ax=axes[1,i], x="date", y="count_roll", data=df_time, color='r')
    
    axes[1,i].set_title(f'{x}', fontsize = 20)
    axes[1,i].set_xlabel("Date", fontsize = 14)
    axes[1,i].set_ylabel(f"Rolling average over {smoothing_scale}", fontsize = 14)
    
    axes[0,i].legend(labels=["Twitter"])
    axes[1,i].legend(labels=["Facebook"])

fig.set_size_inches(20, 20)

fig.savefig('logic_temporality.png', dpi=300)


# ## Les mondes sociaux et la périodisation de la controverse

# ## Bumpchart

# In[13]:


def ranked_element(data, which, top_rank):
    """
    data = the dataframe
    which = a string ; the variable that we want to rank and display on bump chart
    top_rank = an integer ; the rank that values should reach at least one time to be part of bump chart
    """
    
    df = (
        data
          .value_counts(["platform", which, 'segm', 'start_segment'])
          .groupby(["platform", "segm"])
          .rank("first", ascending=False)
          .rename("rank")
          .sort_index()
          .reset_index()
         )
    
    if top_rank is not None :
        df.loc[df["rank"] > top_rank, f"top_rank_{top_rank}"] = 0
        df.loc[df["rank"] < top_rank, f"top_rank_{top_rank}"] = 1
        df["count_rank"] = df.groupby(["platform",which])[f"top_rank_{top_rank}"].transform("sum")
        df = df.loc[df["count_rank"] >= 1]
    else:
        pass
    
    ## Compute correlation matrix for each origin
    
    
    return df
    


# In[14]:


def bump_chart(data, which):
    
    """
    data = the dataframe
    """
    
    list_plaform = ["twitter", "Facebook"]


    for n, x in enumerate(list_origin):
    
        df = data.loc[data["platform"]== x]
        n_top_ranked = 10
        top_sources = df[df["segm"] == df["segm"].max()].nsmallest(n_top_ranked, "rank")

        fig, ax = plt.subplots(figsize=(16, 10), subplot_kw=dict(ylim=(0.5, 0.5 + n_top_ranked)))
        
        fig.suptitle(x, fontsize=16)


        ax.xaxis.set_major_locator(MultipleLocator(1))
        ax.yaxis.set_major_locator(MultipleLocator(1))

        yax2 = ax.secondary_yaxis("right")
        yax2.yaxis.set_major_locator(FixedLocator(top_sources["rank"].to_list()))
        yax2.yaxis.set_major_formatter(FixedFormatter(top_sources[which].to_list()))


        for i, j in df.groupby([which]):
            ax = plt.plot("segm", "rank", "o-", data=j)
            

        plt.gca().invert_yaxis()
    
    return fig


# In[15]:


def interactive_bump_chart(data, which, cluster):

    if cluster is None :
        pass
    else:
        data["cluster"] = pd.to_numeric(data["cluster"])
        data = data.loc[data["cluster"] == cluster]
        
    fig = px.line(data, x="start_segment", y="rank", color = which, facet_row="platform").update_traces(mode='markers+lines')
    fig['layout']['yaxis']['autorange'] = "reversed"

    fig.show()
    fig.write_html("images/bumpchart2.html")


# In[16]:


d = dftext.dropna(subset=["Synthetic_logics"])
df_rank = ranked_element(data = d, which="Synthetic_logics", top_rank= None)
bump = interactive_bump_chart(data = df_rank, which = "Synthetic_logics", cluster = None)


# In[17]:


import bamboolib
t = dftext[["id", "user_screen_name","text", "Synthetic_logics"]].loc[(dftext["text"].str.contains("néonic")==True) & (dftext["platform"]=="twitter")]
t
#t = dftext["text"].loc[dftext["id"] == "1490196"]
t

