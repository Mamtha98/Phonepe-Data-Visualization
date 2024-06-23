#Required Libraries
import streamlit as st
import math
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import re
from sqlalchemy import exc
import numpy as np
import json
import sys
from streamlit_option_menu import option_menu
from mysql.connector import errorcode
import plotly.express as px
import plotly.graph_objects as go
import requests


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="Phonepe"
)
mycursor = mydb.cursor()
#Create SQLAlchemy engine
engine = create_engine("mysql+mysqlconnector://root:root@localhost/Phonepe")

def PieTT(where_clause,table_name,gb1,sv1,sv2):
    mycursor.execute(f"select {gb1}, sum({sv1}), sum({sv2}) from {table_name} {where_clause} group by 1")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1,sv2])
    ATAS1.reset_index(inplace= True)
    c1,c2 = st.columns(2)
    with c1:
        fig_ATAS1 = px.pie(ATAS1, values=sv1, names=gb1 ,
            title=f'Total {sv1} for each {gb1}',
            hover_data=[sv1],color_discrete_sequence=px.colors.sequential.Sunset)
        fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_ATAS1)
    with c2:
        fig_ATAS2 = px.pie(ATAS1, values=sv2, names=gb1,
                    title=f'Total {sv2} for each {gb1}',
                    hover_data=[sv2],color_discrete_sequence=px.colors.sequential.Magenta) 
        fig_ATAS2.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_ATAS2)    
    return None

def GrPie(where_clause,table_name,gb1,gb2,sv1):
    mycursor.execute(f"select {gb1},{gb2}, sum({sv1}) from {table_name} {where_clause} group by 1,2")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,gb2,sv1])
    ATAS1.reset_index(inplace= True)
    fig = px.sunburst(ATAS1, path =[gb1,gb2],values=sv1,color = gb1,hover_data=[sv1]
                      ,color_continuous_scale='Agsunset')
    fig.update_traces(textinfo='label+percent parent',insidetextfont=dict(color='black', weight='bold'))
    fig.update_layout( title_text = f'{sv1} breakdown across each {gb1} and {gb2}',title_x =0 )
    st.plotly_chart(fig)
    return None
def MapTT(where_clause,table_name,gb1,sv1):
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
    states_name_tra.sort()
    mycursor.execute(f"select {gb1},sum({sv1}) from {table_name} {where_clause} group by 1")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1])
    ATAS1.reset_index(inplace= True)
    fig_india_1= px.choropleth(ATAS1, geojson= data1, locations= gb1, featureidkey= "properties.ST_NM",
                             color= sv1, color_continuous_scale = "Reds",
                             range_color= (ATAS1[sv1].min(),ATAS1[sv1].max()),
                             hover_name= gb1,title = f'Total {sv1} across each State',
                             fitbounds= "locations",width =600, height= 600)
    fig_india_1.update_geos(visible =False)
    st.plotly_chart(fig_india_1)
    return None
def BarTTSingle(where_clause,table_name,gb1,sv1):
    mycursor.execute(f"select {gb1},sum({sv1}) from {table_name} {where_clause} group by 1")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1])
    ATAS1.reset_index(inplace= True)
    fig = px.bar(ATAS1, x=sv1, y=gb1, orientation='h',color =gb1,
             title=f'Total {sv1} for each {gb1}',color_discrete_sequence=px.colors.sequential.Agsunset_r)
    st.plotly_chart(fig)
    return None


def BarTT(where_clause,table_name,gb1,sv1,sv2):
    mycursor.execute(f"select {gb1},sum({sv1}),sum({sv2}) from {table_name} {where_clause} group by 1")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1,sv2])
    ATAS1.reset_index(inplace= True)
    fig = go.Figure()
    trace1= fig.add_trace(go.Bar(
    x=ATAS1[gb1],
    y=ATAS1[sv1],
    name=f'Total {sv1}',
    marker_color='indianred'
    ))
    trace2=fig.add_trace(go.Bar(
    x=ATAS1[gb1],
    y=ATAS1[sv2],
    name=f'Total {sv2}',
    marker_color='lightsalmon'
    ))
    #fig = go.Figure(data=[trace1, trace2])
    fig.update_layout(title=f'Grouped Bar Chart: {sv1} vs {sv2} by {gb1}',barmode='group',xaxis_tickangle=-90)
    st.plotly_chart(fig)
    return None

def lineTT(where_clause,table_name,gb1,gb2,sv1):
    mycursor.execute(f"select {gb1},{gb2}, sum({sv1}) from {table_name} {where_clause} group by 1,2")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,gb2,sv1])
    ATAS1.reset_index(inplace= True)
    fig = px.line(ATAS1, x=gb1, y=sv1, color=gb2, 
                  title=f'Total {sv1} by {gb1} and {gb2}',markers=True)
    st.plotly_chart(fig)
    return None

def lineTT2(where_clause,table_name,gb1,sv1,sv2):
    c1,c2=st.columns(2)
    with c1:
        mycursor.execute(f"select {gb1},sum({sv1}) from {table_name} {where_clause} group by 1")
        ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1])
        ATAS1.reset_index(inplace= True)
        fig1 = px.line(ATAS1, x=gb1, y=sv1, 
                      title=f'Total {sv1} by {gb1}',markers = True ,color_discrete_sequence=['blue'])
        st.plotly_chart(fig1)
    with c2:
        mycursor.execute(f"select {gb1},sum({sv2}) from {table_name} {where_clause} group by 1")
        ATAS2 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv2])
        ATAS2.reset_index(inplace= True)
        fig2 = px.line(ATAS2, x=gb1, y=sv2, 
                      title=f'Total {sv2} by {gb1}',markers = True,color_discrete_sequence=['Red'])
        st.plotly_chart(fig2)
    return None

def TopPieTT(where_clause,table_name,gb1,sv1):
    mycursor.execute(f"select {gb1},sum({sv1}) as sum_value from {table_name} {where_clause} group by 1 order by sum_value desc limit 10; ")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1])
    ATAS1.reset_index(inplace= True)
    fig_ATAS1 = px.pie(ATAS1, values=sv1, names= gb1,
        title=f"Top 10 {gb1} with {sv1} ",hole=0.5,
        hover_data=[sv1],color_discrete_sequence=px.colors.sequential.haline_r)
    fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
    st.plotly_chart(fig_ATAS1)
    return None
def LowPieTT(where_clause,table_name,gb1,sv1):
    mycursor.execute(f"select {gb1},sum({sv1}) as sum_value from {table_name} {where_clause} group by 1 order by sum_value asc limit 10;")
    ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=[gb1,sv1])
    ATAS1.reset_index(inplace= True)
    fig_ATAS1 = px.pie(ATAS1, values=sv1, names= gb1,
        title=f"Least 10 {gb1} with {sv1} ",
        hover_data=[sv1],color_discrete_sequence=px.colors.sequential.haline_r)
    fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
    st.plotly_chart(fig_ATAS1)
    return None


def sqy_transaction_all (s,y,q,mydb):
    type1=[0,0,0]
    conditions = []
    if s is not None:
        type1[0]=1
        conditions.append(f"State = '{s}'")
    if y is not None:
        type1[1]=1
        conditions.append(f"Year = '{y}'")
    if q is not None:
        type1[2]=1
        conditions.append(f"Quater = '{q}'")
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
    else:
        where_clause = ""
    type1 = [1 if condition else 0 for condition in [s, y, q]]
    return type1,where_clause 

def chartFunction(type1,where_clause):
    ts = sum(type1)
    if ts == 1:
        if type1[0] == 1 :
            PieTT(where_clause,'agg_transaction','Transaction_type','Transaction_amount','Transaction_count')
            lineTT2(where_clause,'agg_transaction','Year','Transaction_amount','Transaction_count')
        elif type1[1] == 1 :
            BarTT(where_clause,'agg_transaction','Transaction_type','Transaction_amount','Transaction_count')
            PieTT(where_clause,'agg_transaction','Quater','Transaction_amount','Transaction_count')
        else:
            PieTT(where_clause,'agg_transaction','Transaction_type','Transaction_amount','Transaction_count')
    elif ts == 2:
        if type1[2] == 0 :
            GrPie(where_clause,'agg_transaction','Quater', 'Transaction_type','Transaction_amount')
        elif type1[1] == 0 :
            BarTT(where_clause,'agg_transaction','Year','Transaction_amount','Transaction_count')
        else :
            MapTT(where_clause,'agg_transaction','State','Transaction_amount')
            PieTT(where_clause,'agg_transaction','Transaction_type','Transaction_amount','Transaction_count')
    elif ts == 3:
        PieTT(where_clause,'agg_transaction','Transaction_type','Transaction_amount','Transaction_count')
    return None
def chartFunction2(type1,where_clause):
    ts = sum(type1)
    print(ts)
    if ts == 1:
        if type1[0] == 1 :
            lineTT(where_clause,'map_transaction','Year','District','Transaction_amount')
        elif type1[1] == 1 :
            BarTTSingle(where_clause,'map_transaction','District','Transaction_amount')
        else:
            MapTT(where_clause,'map_transaction','State','Transaction_amount')
            BarTT(where_clause,'map_transaction','Year','Transaction_amount','Transaction_count')            
    elif ts == 2:
        if type1[2] == 0 :
            GrPie(where_clause,'map_transaction','Quater', 'District','Transaction_amount')
        elif type1[1] == 0 :
            lineTT(where_clause,'map_transaction','Year','District','Transaction_amount')
        else :
            MapTT(where_clause,'map_transaction','State','Transaction_amount')
    elif ts == 3:
        BarTT(where_clause,'map_transaction','District','Transaction_amount','Transaction_count')
    return None
def chartFunction3(type1,where_clause):
    ts = sum(type1)
    if ts == 1:
        if type1[0] == 1 :
            PieTT(where_clause,'agg_user','Quater',"Registered_Users","App_Opens")
            lineTT2(where_clause,'agg_user','Year','Registered_Users',"App_Opens")
        elif type1[1] == 1 :
            BarTT(where_clause,'agg_user','State',"Registered_Users","App_Opens")
            PieTT(where_clause,'agg_user','Quater',"Registered_Users","App_Opens")
        else:
            lineTT(where_clause,'agg_user','Year','States','Registered_Users')
            lineTT(where_clause,'agg_user','Year','States',"App_Opens")
    elif ts == 2:
        if type1[2] == 0 :
            PieTT(where_clause,'agg_user','Quater', 'Registered_Users',"App_Opens")
        elif type1[1] == 0 :
            lineTT2(where_clause,'agg_user','Year','Registered_Users',"App_Opens")
        else :
            MapTT(where_clause,'agg_user','State','Registered_Users')
            MapTT(where_clause,'agg_user','State','App_Opens')        
    elif ts == 3:
        mycursor.execute(f"select State,Year,Quater,Registered_Users,App_Opens from agg_user {where_clause}")
        ATAS1 = pd.DataFrame(mycursor.fetchall(),columns=['State','Year','Quarter','Registered Users','App Opens'])
        ATAS1.reset_index(drop= True)
        ATAS1.index += 1
        st.table(ATAS1)
        
    return None

def chartFunction4(type1,where_clause):
    ts = sum(type1)
    if ts == 1:
        if type1[0] == 1 :
            lineTT(where_clause,'map_user','Year','District','Registered_Users')
            BarTTSingle(where_clause,'map_user','District','App_Opens')
        elif type1[1] == 1 :
            PieTT(where_clause,'map_user','Quater','Registered_Users',"App_Opens")
            MapTT(where_clause,'map_user','State',"App_Opens")
        else:
            BarTT(where_clause,'map_user','District','Registered_Users',"App_Opens")        
    elif ts == 2:
        if type1[2] == 0 :
            PieTT(where_clause,'map_user','Quater','Registered_Users',"App_Opens")
            lineTT(where_clause,'map_user','Quater', 'District','App_Opens')
        elif type1[1] == 0 :
            BarTT(where_clause,'map_user','Year','App_Opens','Registered_Users')
            
        else :
            MapTT(where_clause,'map_user','State',"Registered_Users")
            MapTT(where_clause,'map_user','State',"App_Opens")
    elif ts == 3:
        BarTT(where_clause,'map_user','District','Registered_Users',"App_Opens")
    return None

def chartfunction5(table_name1,table_name2,sv1):
    col1,col2= st.columns(2)
    mycursor.execute(f"select min(Year) from {table_name1};")
    min_year = mycursor.fetchall() 
    mycursor.execute(f"select max(Year) from {table_name1};")
    max_year = mycursor.fetchall() 
    mycursor.execute(f"select min(Quater) from {table_name1};")
    min_Quarter = mycursor.fetchall() 
    mycursor.execute(f"select max(Quater) from {table_name1};")
    max_Quarter = mycursor.fetchall() 
    with col1:
        s1= st.slider("**Select the Year**",min_year[0][0],max_year[0][0])
        if s1 == 2024:
            s2= st.slider("**Select the Quarter**",0,1)
        else:
            s2= st.slider("**Select the Quarter**",min_Quarter[0][0],max_Quarter[0][0])
        type1,where_clause= sqy_transaction_all(None,s1,s2,mydb)
    with col2:
        TopPieTT(where_clause,table_name1,'State',sv1)
    c1,c2 =st.columns(2)
    with c1:
        TopPieTT(where_clause,table_name1,'District',sv1)
    with c2:
         type1,where_clause= sqy_transaction_all(None,s1,s2,mydb)
         TopPieTT(where_clause,table_name2,'Pincode',sv1)
    mycursor.execute(f"select distinct(State) from {table_name1};")
    unique_states = pd.DataFrame(mycursor.fetchall(),columns=['State'])
    option_dropdown4 = st.selectbox("Select the state",unique_states)
    type1,where_clause= sqy_transaction_all(option_dropdown4,s1,s2,mydb)
    c1,c2 =st.columns(2)
    with c1:
        TopPieTT(where_clause,table_name1,'District',sv1)
    with c2:
        type1,where_clause= sqy_transaction_all(option_dropdown4,s1,s2,mydb)
        TopPieTT(where_clause,table_name2,'Pincode',sv1)
    return None 

def render_logo_and_heading(logo_url, heading_text):
    st.markdown(f"""
    <div style="display: flex; align-items: center;">
        <img src="{logo_url}" alt="Logo" style="width: 80px; height: auto; margin-right: 5px;">
        <h2>{heading_text}</h2>
    </div>
    """, unsafe_allow_html=True)
    return None 

def drop_down(table_name,i):
    c1,c2,c3 = st.columns(3)
    with c1:
        mycursor.execute(f"select distinct State from {table_name}")
        unique_states = pd.DataFrame(mycursor.fetchall(),columns=['State'])
        option_dropdown1 = st.selectbox("Select the state",unique_states, index=None, key='k'+str(i))
        i+=1
    with c2:
        mycursor.execute(f"select distinct Year from {table_name}")
        unique_year = pd.DataFrame(mycursor.fetchall(),columns=['Year'])
        option_dropdown2 = st.selectbox("Select the year",unique_year, index=None, key='k'+str(i))
        i+=1
    with c3:
        if option_dropdown2 == 2024:
            unique_quarter = [1]
        else:
            mycursor.execute(f"select distinct Quater from {table_name}")
            unique_quarter = pd.DataFrame(mycursor.fetchall(),columns=['Quarter'])
        option_dropdown3 = st.selectbox("Select the quarter",unique_quarter, index=None, key='k'+str(i))
        i+=1
    return (option_dropdown1,option_dropdown2,option_dropdown3)

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title= "Phonepe Pulse Data Visualization - Mamtha S")

#set the colour of sidebar
st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #5F259F;
    }
    .sidebar-title {
    font-weight: bold; /* Makes the text bold */
    color: #333; /* Sets the text color to a dark shade (e.g., black) */
}

</style>
""", unsafe_allow_html=True)

#set the option menu's option list and customise styles
st.sidebar.title("Phonepe Pulse Data Visualization")
st.sidebar.markdown("<style>h1{color: white;}</style>", unsafe_allow_html=True)
#st.sidebar.title(:white[Phonepe Pulse Data Visualization])
with st.sidebar:
    option = option_menu(None,
                #menu_title='Main Menu',
                #menu_title='Phonepe Pulse Data Visualization',
                options=["Home", 'Analysis','Insights'],
                icons=['house',"bar-chart-line", "exclamation-circle"],
                #menu_icon="cast",
                default_index=0,
                styles={
                    "container": {"background-color":'white',"height":"188px","border": "3px solid #000000","border-radius": "0px"},
        "icon": {"color": "black", "font-size": "16px"}, 
        "nav-link": {"color":"black","font-size": "15px", "text-align": "centre", "margin":"4px", "--hover-color": "white","border": "1px solid #000000", "border-radius": "10px"},
        "nav-link-selected": {"background-color": "#5F259F"},}
                
                )

if option == "Home":
    #st.markdown("## :black[Phonepe Data Visualization and Exploration]")
    render_logo_and_heading("data:image/svg+xml;base64,PHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHg9IjAiIHk9IjAiIHZpZXdCb3g9IjAgMCAxMzIgNDgiIHhtbDpzcGFjZT0icHJlc2VydmUiPjxzdHlsZT4uc3Qwe2ZpbGw6IzVmMjU5Zn08L3N0eWxlPjxjaXJjbGUgdHJhbnNmb3JtPSJyb3RhdGUoLTc2LjcxNCAxNy44NyAyNC4wMDEpIiBjbGFzcz0ic3QwIiBjeD0iMTcuOSIgY3k9IjI0IiByPSIxNy45Ii8+PHBhdGggY2xhc3M9InN0MCIgZD0iTTkwLjUgMzQuMnYtNi41YzAtMS42LS42LTIuNC0yLjEtMi40LS42IDAtMS4zLjEtMS43LjJWMzVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYyMy45YzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA1LjYgMS45IDUuNiA1LjR2Ny40YzAgLjMtLjMuNi0uNi42SDkyYy0uOSAwLTEuNS0uNy0xLjUtMS41em05LTMuOWwtLjEuOWMwIDEuMi44IDEuOSAyLjEgMS45IDEgMCAxLjktLjMgMi45LS44LjEgMCAuMi0uMS4zLS4xLjIgMCAuMy4xLjQuMi4xLjEuMy40LjMuNC4yLjMuNC43LjQgMSAwIC41LS4zIDEtLjcgMS4yLTEuMS42LTIuNC45LTMuOC45LTEuNiAwLTIuOS0uNC0zLjktMS4yLTEtLjktMS42LTIuMS0xLjYtMy42di0zLjljMC0zLjEgMi01IDUuNC01IDMuMyAwIDUuMiAxLjggNS4yIDV2Mi40YzAgLjMtLjMuNi0uNi42aC02LjN6bS0uMS0yLjJIMTAzLjJ2LTFjMC0xLjItLjctMi0xLjktMnMtMS45LjctMS45IDJ2MXptMjUuNSAyLjJsLS4xLjljMCAxLjIuOCAxLjkgMi4xIDEuOSAxIDAgMS45LS4zIDIuOS0uOC4xIDAgLjItLjEuMy0uMS4yIDAgLjMuMS40LjIuMS4xLjMuNC4zLjQuMi4zLjQuNy40IDEgMCAuNS0uMyAxLS43IDEuMi0xLjEuNi0yLjQuOS0zLjguOS0xLjYgMC0yLjktLjQtMy45LTEuMi0xLS45LTEuNi0yLjEtMS42LTMuNnYtMy45YzAtMy4xIDItNSA1LjQtNSAzLjMgMCA1LjIgMS44IDUuMiA1djIuNGMwIC4zLS4zLjYtLjYuNmgtNi4zem0tLjEtMi4ySDEyOC42di0xYzAtMS4yLS43LTItMS45LTJzLTEuOS43LTEuOSAydjF6TTY2IDM1LjdoMS40Yy4zIDAgLjYtLjMuNi0uNnYtNy40YzAtMy40LTEuOC01LjQtNC44LTUuNC0uOSAwLTEuOS4yLTIuNS40VjE5YzAtLjgtLjctMS41LTEuNS0xLjVoLTEuNGMtLjMgMC0uNi4zLS42LjZ2MTdjMCAuMy4zLjYuNi42aDIuM2MuMyAwIC42LS4zLjYtLjZ2LTkuNGMuNS0uMiAxLjItLjMgMS43LS4zIDEuNSAwIDIuMS43IDIuMSAyLjR2Ni41Yy4xLjcuNyAxLjQgMS41IDEuNHptMTUuMS04LjRWMzFjMCAzLjEtMi4xIDUtNS42IDUtMy40IDAtNS42LTEuOS01LjYtNXYtMy43YzAtMy4xIDIuMS01IDUuNi01IDMuNSAwIDUuNiAxLjkgNS42IDV6bS0zLjUgMGMwLTEuMi0uNy0yLTItMnMtMiAuNy0yIDJWMzFjMCAxLjIuNyAxLjkgMiAxLjlzMi0uNyAyLTEuOXYtMy43em0tMjIuMy0xLjdjMCAzLjItMi40IDUuNC01LjYgNS40LS44IDAtMS41LS4xLTIuMi0uNHY0LjVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYxOS4yYzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA2LjEgMi4yIDYuMSA1LjZ2Mi40ek01MS43IDIzYzAtMS42LTEuMS0yLjQtMi42LTIuNC0uOSAwLTEuNS4zLTEuNS4zdjYuNmMuNi4zLjkuNCAxLjYuNCAxLjUgMCAyLjYtLjkgMi42LTIuNFYyM3ptNjguMiAyLjZjMCAzLjItMi40IDUuNC01LjYgNS40LS44IDAtMS41LS4xLTIuMi0uNHY0LjVjMCAuMy0uMy42LS42LjZoLTIuM2MtLjMgMC0uNi0uMy0uNi0uNlYxOS4yYzAtLjQuMy0uNy42LS44IDEuNS0uNSAzLS44IDQuNi0uOCAzLjYgMCA2LjEgMi4yIDYuMSA1LjZ2Mi40em0tMy42LTIuNmMwLTEuNi0xLjEtMi40LTIuNi0yLjQtLjkgMC0xLjUuMy0xLjUuM3Y2LjZjLjYuMy45LjQgMS42LjQgMS41IDAgMi42LS45IDIuNi0yLjRWMjN6Ii8+PHBhdGggZD0iTTI2IDE5LjNjMC0uNy0uNi0xLjMtMS4zLTEuM2gtMi40bC01LjUtNi4zYy0uNS0uNi0xLjMtLjgtMi4xLS42bC0xLjkuNmMtLjMuMS0uNC41LS4yLjdsNiA1LjdIOS41Yy0uMyAwLS41LjItLjUuNXYxYzAgLjcuNiAxLjMgMS4zIDEuM2gxLjR2NC44YzAgMy42IDEuOSA1LjcgNS4xIDUuNyAxIDAgMS44LS4xIDIuOC0uNXYzLjJjMCAuOS43IDEuNiAxLjYgMS42aDEuNGMuMyAwIC42LS4zLjYtLjZWMjAuOGgyLjNjLjMgMCAuNS0uMi41LS41di0xem0tNi40IDguNmMtLjYuMy0xLjQuNC0yIC40LTEuNiAwLTIuNC0uOC0yLjQtMi42di00LjhoNC40djd6IiBmaWxsPSIjZmZmIi8+PC9zdmc+", "Phonepe Pulse Data Visualization")
    st.divider()
    col1,col2 = st.columns([3,2],gap="medium")
    with col1:
        st.image('https://www.phonepe.com/pulsestatic/799/pulse/static/1ca92fceac99e4712aa74a5e281562c7/bf2fd/spotlight_1.jpg', use_column_width=True)
    with col2:
        st.markdown("<h4 style='text-align: left;'><strong>Problem Statement</strong></h4>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: justify;'>The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.</p>""", unsafe_allow_html=True)
    st.markdown("<h5 style='text-align: left;'><strong>Domain: Fintech</strong></h5>", unsafe_allow_html=True)
    st.markdown("<h5 style='text-align: left;'><strong>Technologies Used:</strong></h5>", unsafe_allow_html=True)
    multi = ''' 
        1. Github Cloning
        2. Python (Pandas)
        3. MySQL
        4. Plotly'''
    st.markdown(multi)
    st.markdown("<h5 style='text-align: left;'><strong>Overview:</strong></h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: justify;'>In this streamlit web app you can visualize the phonepe pulse data and gain lot of insights on transactions, number of users, top 10 state, district, pincode and which brand has most number of users and so on. Using Bar charts, Pie charts, Line Charts and Geo map visualization useful insights can be gained.</p>""", unsafe_allow_html=True)
    
if option == "Analysis":
    c1,c2 = st.columns([2,3])
    with c1:
        option_dropdown = st.selectbox(
        "What would you like to analyse?",
        ("Transaction", "User"))
    if option_dropdown == "Transaction":
        tab1, tab2, tab3= st.tabs(["Aggregated data Analysis", "Map data Analysis", "Top data Analysis"])
        with tab1:
            dd1,dd2,dd3 = drop_down('agg_transaction',0)
            type1,where_clause= sqy_transaction_all(dd1,dd2,dd3,mydb)
            cf=chartFunction(type1,where_clause)
        with tab2:
            dd4,dd5,dd6 = drop_down('map_transaction',3)
            type1,where_clause= sqy_transaction_all(dd4,dd5,dd6,mydb)
            cf2=chartFunction2(type1,where_clause)
        with tab3:
            cf5 =chartfunction5('top_district_transaction','top_pincode_transaction','Transaction_amount')
            
    if option_dropdown == "User":
        tab1, tab2, tab3= st.tabs(["Aggregated data Analysis", "Map data Analysis", "Top data Analysis"])
        with tab1:
            dd7,dd8,dd9 = drop_down('agg_user',6)
            type1,where_clause= sqy_transaction_all(dd7,dd8,dd9,mydb)
            cf3=chartFunction3(type1,where_clause)
        with tab2:
            dd10,d11,dd12 = drop_down('map_user',9)
            type1,where_clause= sqy_transaction_all(dd10,d11,dd12,mydb)
            cf4=chartFunction4(type1,where_clause)
            #type1,where_clause= sqy_transaction_all(dd10,d11,dd12,mydb)
        with tab3:
            cf6 = chartfunction5('top_district_user','top_pincode_user','Registered_Users')
if option == "Insights":
    q= st.selectbox('Select your Question:',
                              ('1. 10 States with lowest transactions',
                               '2. 10 States with Lowest registered users',
                               '3. 25 Districts with highest App opens',
                               '4. 10 Districts with lowest transactions',
                               '5. 10 Postal codes with lowest Registered users',
                               '6. Top 5 Years with lowest transaction',
                               '7. Top 5 Years with highest transaction',
                               '8. Top Mobile Brands users',
                               '9. Districts with highest transaction count',
                               '10.Top most used Transaction type'),
                              key='collection_question')
    if q == '1. 10 States with lowest transactions':
        LowPieTT('','agg_transaction','State','Transaction_amount')
    if q == '2. 10 States with Lowest registered users':
        LowPieTT('','agg_user','State','Registered_Users')
    if q == '3. 25 Districts with highest App opens':
        mycursor.execute(f"select District,sum(App_Opens) as sum_value from map_user group by 1 order by sum_value desc limit 25; ")
        sorted_data1_q3 = pd.DataFrame(mycursor.fetchall(),columns=['District','App_Opens'])
        fig_q3= px.bar(sorted_data1_q3, x= "District", y= "App_Opens", title= "25 Districts with highest App opens",
            color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_q3) 
    if q == '4. 10 Districts with lowest transactions':
        LowPieTT('','map_transaction','District','Transaction_amount')
    if q == '5. 10 Postal codes with lowest Registered users':
        LowPieTT('','top_pincode_user','Pincode','Registered_Users') 
    if q == '6. Top 5 Years with lowest transaction':
        mycursor.execute(f"select Year,sum(Transaction_amount) as sum_value from agg_transaction group by 1 order by sum_value asc limit 5; ")
        sorted_data1_q6 = pd.DataFrame(mycursor.fetchall(),columns=['Years','Transaction_amount'])
        fig_q6 = px.line(sorted_data1_q6, x="Years", y="Transaction_amount", title="Top 5 years with lowest transactions",markers = True ,color_discrete_sequence=['blue'])
        st.plotly_chart(fig_q6)   
    if q == '7. Top 5 Years with highest transaction':
        mycursor.execute(f"select Year,sum(Transaction_amount) as sum_value from agg_transaction group by 1 order by sum_value desc limit 5; ")
        sorted_data1_q7 = pd.DataFrame(mycursor.fetchall(),columns=['Years','Transaction_amount'])
        fig_q7 = px.line(sorted_data1_q7, x="Years", y="Transaction_amount", title="Top 5 Years with highest transactions",markers = True ,color_discrete_sequence=['black'])
        st.plotly_chart(fig_q7)  
    if q == '8. Top Mobile Brands users':
        mycursor.execute(f"select Mobile_brand,sum(User_count) as sum_value from agg_user_brand group by 1 order by sum_value desc limit 5; ")
        sorted_data1_q8 = pd.DataFrame(mycursor.fetchall(),columns=['Mobile_brand','User_count'])
        fig_q8= px.pie(sorted_data1_q8, values= "User_count", names= "Mobile_brand", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands users")
        fig_q8.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_q8)
    if q == '9. Districts with highest transaction count':
        TopPieTT('','top_district_transaction','District','Transaction_count')
    if q == '10.Top most used Transaction type':
        mycursor.execute(f"select Transaction_type,sum(Transaction_amount) as sum_value from agg_transaction group by 1 order by sum_value desc limit 2; ")
        sorted_data1_q10 = pd.DataFrame(mycursor.fetchall(),columns=['Transaction_type','Transaction_amount'])
        fig_q10= px.pie(sorted_data1_q10, values= "Transaction_amount", names= "Transaction_type", title="Top most used Transaction type",
                color_discrete_sequence=px.colors.sequential.Emrld_r)
        st.plotly_chart(fig_q10)

