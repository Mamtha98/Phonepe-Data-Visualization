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

mycursor.execute("""select *  from agg_transaction;""")
AT = mycursor.fetchall()
agg_transaction = pd.DataFrame(AT,columns = ["States", "Years", "Quarter", "Transaction_type", "Transaction_count","Transaction_amount"])

mycursor.execute("""select * from agg_user;""")
AU = mycursor.fetchall()
agg_user = pd.DataFrame(AU,columns = ["States", "Years", "Quarter", "Registered_Users", "App_Opens"])

mycursor.execute("""select * from agg_user_brand;""")
AUB = mycursor.fetchall()
agg_user_brand = pd.DataFrame(AUB,columns = ["States", "Years", "Quarter", "Mobile_brand", "User_count","percentage_of_user"])

mycursor.execute("""select * from map_transaction;""")
MT = mycursor.fetchall()
map_transaction = pd.DataFrame(MT,columns = ["States", "Years", "Quarter", "District", "Transaction_count","Transaction_amount"])

mycursor.execute("""select * from map_user;""")
MU = mycursor.fetchall()
map_user = pd.DataFrame(MU,columns = ["States", "Years", "Quarter", "District", "Registered_Users","App_Opens"])

mycursor.execute("""select *  from top_district_transaction;""")
TDT = mycursor.fetchall()
top_district_transaction = pd.DataFrame(TDT,columns = ["States", "Years", "Quarter", "District", "Transaction_count","Transaction_amount"])

mycursor.execute("""select *  from top_pincode_transaction;""")
TPT = mycursor.fetchall()
top_pincode_transaction = pd.DataFrame(TPT,columns = ["States", "Years", "Quarter", "Pincode", "Transaction_count","Transaction_amount"])

mycursor.execute("""select *  from top_district_user;""")
TDU = mycursor.fetchall()
top_district_user = pd.DataFrame(TDU,columns = ["States", "Years", "Quarter", "District", "Registered_Users"])

mycursor.execute("""select *  from top_pincode_user;""")
TPU = mycursor.fetchall()
top_pincode_user = pd.DataFrame(TPU,columns = ["States", "Years", "Quarter", "Pincode", "Registered_Users"])

def PieTT(df,gb1,sv1,sv2):
    ATAS1=df.groupby(gb1)[[sv1, sv2]].sum()
    ATAS1.reset_index(inplace= True)
    c1,c2 = st.columns(2)
    with c1:
        fig_ATAS1 = px.pie(ATAS1, values=sv1, names= gb1,
            title=f'Total {sv1} for each {gb1}',
            hover_data=[sv1],color_discrete_sequence=px.colors.sequential.Sunset)
        fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_ATAS1)
    with c2:
        fig_ATAS2 = px.pie(ATAS1, values=sv2, names= gb1,
                    title=f'Total {sv2} for each {gb1}',
                    hover_data=[sv2],color_discrete_sequence=px.colors.sequential.Magenta) 
        fig_ATAS2.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_ATAS2)
        
    return None
def GrPie(ATAS,gb1,gb2,sv1):
    agg_data = ATAS.groupby([gb1, gb2])[sv1].sum().reset_index()
    fig = px.sunburst(agg_data, path =[gb1,gb2],values=sv1,color = gb1,hover_data=[sv1]
                      ,color_continuous_scale='Agsunset')
    fig.update_traces(textinfo='label+percent parent',insidetextfont=dict(color='black', weight='bold'))
    fig.update_layout( title_text = f'{sv1} breakdown across each {gb1} and {gb2}',title_x =0 )
    st.plotly_chart(fig)
    return None
def MapTT(df,gb,sv):
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
    states_name_tra.sort()
    MT=df.groupby(gb)[[sv]].sum()
    MT.reset_index(inplace= True)

    fig_india_1= px.choropleth(MT, geojson= data1, locations= gb, featureidkey= "properties.ST_NM",
                             color= sv, color_continuous_scale= "Reds",
                             range_color= (MT[sv].min(),MT[sv].max()),
                             hover_name= gb,title = f'Total {sv} across each State',
                             fitbounds= "locations",width =600, height= 600)
    fig_india_1.update_geos(visible =False)
    st.plotly_chart(fig_india_1)
    return None
def BarTTSingle(df,gb,sv1):
    ATAY1=df.groupby(gb)[[sv1]].sum()
    #st.write(ATAY1)
    ATAY1.reset_index(inplace= True)
    fig = px.bar(df, x=sv1, y=gb, orientation='h',color =gb,
             title=f'Total {sv1} for each {gb}',color_discrete_sequence=px.colors.sequential.Agsunset_r)
    st.plotly_chart(fig)
    return None


def BarTT(df,gb,sv1,sv2):
    ATAY1=df.groupby(gb)[[sv1,sv2]].sum()
    #st.write(ATAY1)
    ATAY1.reset_index(inplace= True)
    fig_ATAY1 = go.Figure()
    trace1= fig_ATAY1.add_trace(go.Bar(
    x=ATAY1[gb],
    y=ATAY1[sv1],
    name=f'Total {sv1}',
    marker_color='indianred'
    ))
    trace2=fig_ATAY1.add_trace(go.Bar(
    x=ATAY1[gb],
    y=ATAY1[sv2],
    name=f'Total {sv2}',
    marker_color='lightsalmon'
    ))
    #fig = go.Figure(data=[trace1, trace2])
    fig_ATAY1.update_layout(title=f'Grouped Bar Chart: {sv1} vs {sv2} by {gb}',barmode='group',xaxis_tickangle=-90)
    st.plotly_chart(fig_ATAY1)
    return None

def lineTT(df,gb1,gb2,sv):
    grouped_data = df.groupby([gb1, gb2 ])[sv].sum().reset_index()

    # Create line chart using Plotly Express
    fig = px.line(grouped_data, x=gb1, y=sv, color=gb2, 
                  title=f'Total {sv} by {gb1} and {gb2}',markers=True)
    st.plotly_chart(fig)
    return None
def lineTT2(df,gb,sv1,sv2):
    c1,c2=st.columns(2)
    with c1:
        grouped_data1 = df.groupby([gb])[sv1].sum().reset_index()

        # Create line chart using Plotly Express
        fig1 = px.line(grouped_data1, x=gb, y=sv1, 
                      title=f'Total {sv1} by {gb}',markers = True ,color_discrete_sequence=['blue'])
        st.plotly_chart(fig1)
    with c2:
        grouped_data2 = df.groupby([gb])[sv2].sum().reset_index()
    
        # Create line chart using Plotly Express
        fig2 = px.line(grouped_data2, x=gb, y=sv2, 
                      title=f'Total {sv2} by {gb}',markers = True,color_discrete_sequence=['Red'])
        st.plotly_chart(fig2)
    return None

def TopPieTT(df,gb,sv):
    ATAS1=df.groupby(gb)[[sv]].sum()
    sorted_df = ATAS1.sort_values(by=sv,ascending = False).head(10)
    sorted_df.reset_index(inplace= True)
    fig_ATAS1 = px.pie(sorted_df, values=sv, names= gb,
        title=f"Top 10 {gb} with {sv} ",hole=0.5,
        hover_data=[sv],color_discrete_sequence=px.colors.sequential.haline_r)
    fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
    st.plotly_chart(fig_ATAS1)
    return None
def LowPieTT(df,gb,sv):
    ATAS1=df.groupby(gb)[[sv]].sum()
    sorted_df = ATAS1.sort_values(by=sv).head(10)
    sorted_df.reset_index(inplace= True)
    fig_ATAS1 = px.pie(sorted_df, values=sv, names= gb,
        title=f"Least 10 {gb} with {sv} ",
        hover_data=[sv],color_discrete_sequence=px.colors.sequential.haline_r)
    fig_ATAS1.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
    st.plotly_chart(fig_ATAS1)
    return None


def sqy_transaction_all (s,y,q,df):
    type1 = [0,0,0]
    if s is not None:
        type1[0]=1
        df=df[df["States"] == s]
    if y is not None:
        type1[1]=1
        if not df.empty:
            df=df[df["Years"] == y] 
        else:
            df=df[df["Years"] == y]
    if q  is not None:
        type1[2]=1
        if  not df.empty:
            df=df[df["Quarter"] == q] 
        else:
            df=df[df["Quarter"] == q] 
    return df,type1 

def chartFunction(ATAS,type1):
    ts = sum(type1)
    if ts == 1:
        if type1[0] == 1 :
            PieTT(ATAS,'Transaction_type','Transaction_amount','Transaction_count')
            lineTT2(ATAS,'Years','Transaction_amount','Transaction_count')
        elif type1[1] == 1 :
            BarTT(ATAS,'Transaction_type','Transaction_amount','Transaction_count')
            PieTT(ATAS,'Quarter','Transaction_amount','Transaction_count')
        else:
            PieTT(ATAS,'Transaction_type','Transaction_amount','Transaction_count')
    elif ts == 2:
        if type1[2] == 0 :
            GrPie(ATAS,'Quarter', 'Transaction_type','Transaction_amount')
        elif type1[1] == 0 :
            BarTT(ATAS,'Years','Transaction_amount','Transaction_count')
        else :
            MapTT(ATAS,'States','Transaction_amount')
            PieTT(ATAS,'Transaction_type','Transaction_amount','Transaction_count')
    elif ts == 3:
        PieTT(ATAS,'Transaction_type','Transaction_amount','Transaction_count')
    return None
def chartFunction2(type1,ATAS):
    ts = sum(type1)
    print(ts)
    if ts == 1:
        if type1[0] == 1 :
            lineTT(ATAS,'Years','District','Transaction_amount')
        elif type1[1] == 1 :
            BarTTSingle(ATAS,'District','Transaction_amount')
        else:
            MapTT(ATAS,'States','Transaction_amount')
            BarTT(ATAS,'Years','Transaction_amount','Transaction_count')            
    elif ts == 2:
        if type1[2] == 0 :
            GrPie(ATAS,'Quarter', 'District','Transaction_amount')
        elif type1[1] == 0 :
            lineTT(ATAS,'Years','District','Transaction_amount')
        else :
            MapTT(ATAS,'States','Transaction_amount')
    elif ts == 3:
        BarTT(ATAS,'District','Transaction_amount','Transaction_count')
    return None
def chartFunction3(ATAS,type1):
    ts = sum(type1)
    if ts == 1:
        if type1[0] == 1 :
            
            PieTT(ATAS,'Quarter',"Registered_Users","App_Opens")
            lineTT2(ATAS,'Years','Registered_Users',"App_Opens")
        elif type1[1] == 1 :
            BarTT(ATAS,'States',"Registered_Users","App_Opens")
            PieTT(ATAS,'Quarter',"Registered_Users","App_Opens")
        else:
            lineTT(ATAS,'Years','States','Registered_Users')
            lineTT(ATAS,'Years','States',"App_Opens")
    elif ts == 2:
        if type1[2] == 0 :
            PieTT(ATAS,'Quarter', 'Registered_Users',"App_Opens")
        elif type1[1] == 0 :
            lineTT2(ATAS,'Years','Registered_Users',"App_Opens")
        else :
            MapTT(ATAS,'States',"Registered_Users")
            MapTT(ATAS,'States',"App_Opens")
            
    elif ts == 3:
        df = ATAS.reset_index(drop=True)
        df.index += 1
        st.table(df)
        
    return None

def chartFunction4(type1,ATAS):
    ts = sum(type1)
    print(ts)
    if ts == 1:
        if type1[0] == 1 :
            lineTT(ATAS,'Years','District','Registered_Users')
            BarTTSingle(ATAS,'District','App_Opens')
        elif type1[1] == 1 :
            PieTT(ATAS,'Quarter','Registered_Users',"App_Opens")
            MapTT(ATAS,'States',"App_Opens")
        else:
            BarTT(ATAS,'District','Registered_Users',"App_Opens")        
    elif ts == 2:
        if type1[2] == 0 :
            PieTT(ATAS,'Quarter','Registered_Users',"App_Opens")
            lineTT(ATAS,'Quarter', 'District','App_Opens')
        elif type1[1] == 0 :
            BarTT(ATAS,'Years','App_Opens','Registered_Users')
            
        else :
            MapTT(ATAS,'States',"Registered_Users")
            MapTT(ATAS,'States',"App_Opens")
    elif ts == 3:
        BarTT(ATAS,'District','Registered_Users',"App_Opens")
    return None

def chartfunction5(df1,df2,sv1):
    col1,col2= st.columns(2)
    with col1:
        s1= st.slider("**Select the Year**", df1["Years"].min(), df1["Years"].max(),df1["Years"].min())
        s2= st.slider("**Select the Quarter**", df1["Quarter"].min(), df1["Quarter"].max(),df1["Quarter"].min())
        SQY_T ,type1= sqy_transaction_all(None,s1,s2,df1)
    with col2:
        TopPieTT(SQY_T,'States',sv1)
    c1,c2 =st.columns(2)
    with c1:
        TopPieTT(SQY_T,'District',sv1)
    with c2:
         SQY_T ,type1= sqy_transaction_all(None,s1,s2,df2)
         TopPieTT(SQY_T,'Pincode',sv1)
    unique_states = df1['States'].unique()
    option_dropdown4 = st.selectbox("Select the state",unique_states)
    SQY_T ,type1= sqy_transaction_all(option_dropdown4,s1,s2,df1)
    c1,c2 =st.columns(2)
    with c1:
        TopPieTT(SQY_T,'District',sv1)
    with c2:
        SQY_T ,type1= sqy_transaction_all(option_dropdown4,s1,s2,df2)
        TopPieTT(SQY_T,'Pincode',sv1)
    return None 

def render_logo_and_heading(logo_url, heading_text):
    st.markdown(f"""
    <div style="display: flex; align-items: center;">
        <img src="{logo_url}" alt="Logo" style="width: 80px; height: auto; margin-right: 5px;">
        <h2>{heading_text}</h2>
    </div>
    """, unsafe_allow_html=True)
    return None 

def drop_down(df,i):
    c1,c2,c3 = st.columns(3)
    with c1:
        unique_states = df['States'].unique()
        option_dropdown1 = st.selectbox("Select the state",unique_states, index=None, key='k'+str(i))
        i+=1
    with c2:
        unique_year = df['Years'].unique()
        option_dropdown2 = st.selectbox("Select the year",unique_year, index=None, key='k'+str(i))
        i+=1
    with c3:
        unique_quarter = df['Quarter'].unique()
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
            dd1,dd2,dd3 = drop_down(agg_transaction,0)
            SQY_T ,type1= sqy_transaction_all(dd1,dd2,dd3,agg_transaction)
            cf=chartFunction(SQY_T,type1)
        with tab2:
            dd4,dd5,dd6 = drop_down(map_transaction,3)
            SQY_T ,type1= sqy_transaction_all(dd4,dd5,dd6,map_transaction)
            cf2=chartFunction2(type1,SQY_T)
        with tab3:
            cf5 =chartfunction5(top_district_transaction,top_pincode_transaction,'Transaction_amount')
            
    if option_dropdown == "User":
        tab1, tab2, tab3= st.tabs(["Aggregated data Analysis", "Map data Analysis", "Top data Analysis"])
        with tab1:
            dd7,dd8,dd9 = drop_down(agg_user,6)
            SQY_T ,type1= sqy_transaction_all(dd7,dd8,dd9,agg_user)
            cf3=chartFunction3(SQY_T,type1)
        with tab2:
            dd10,d11,dd12 = drop_down(map_transaction,9)
            SQY_T ,type1= sqy_transaction_all(dd10,d11,dd12,map_user)
            cf4=chartFunction4(type1,SQY_T)
            SQY_T1 ,type2= sqy_transaction_all(dd10,d11,dd12,agg_user_brand)
        with tab3:
            cf6 = chartfunction5(top_district_user,top_pincode_user,'Registered_Users')
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
        LowPieTT(agg_transaction,'States','Transaction_amount')
    if q == '2. 10 States with Lowest registered users':
        LowPieTT(agg_user,'States','Registered_Users')
    if q == '3. 25 Districts with highest App opens':
        grouped_data_q3 = map_user.groupby(["District"])["App_Opens"].sum().reset_index()
        sorted_data1_q3 = grouped_data_q3.sort_values(by="District",ascending = False).head(25)
        fig_q3= px.bar(sorted_data1_q3, x= "District", y= "App_Opens", title= "25 Districts with highest App opens",
            color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_q3) 
    if q == '4. 10 Districts with lowest transactions':
        LowPieTT(map_transaction,'District','Transaction_amount')
    if q == '5. 10 Postal codes with lowest Registered users':
        LowPieTT(top_pincode_user,'Pincode','Registered_Users') 
    if q == '6. Top 5 Years with lowest transaction':
        grouped_data_q6 = agg_transaction.groupby(["Years"])["Transaction_amount"].sum().reset_index()
        sorted_data1_q6 = grouped_data_q6.sort_values(by="Years",ascending = True).head(5)
        print(sorted_data1_q6)
        fig_q6 = px.line(sorted_data1_q6, x="Years", y="Transaction_amount", title="Top 5 years with lowest transactions",markers = True ,color_discrete_sequence=['blue'])
        st.plotly_chart(fig_q6)   
    if q == '7. Top 5 Years with highest transaction':
        grouped_data_q7 = agg_transaction.groupby(["Years"])["Transaction_amount"].sum().reset_index()
        sorted_data1_q7 = grouped_data_q7.sort_values(by="Years",ascending = False).head(5)
        fig_q7 = px.line(sorted_data1_q7, x="Years", y="Transaction_amount", title="Top 5 Years with highest transactions",markers = True ,color_discrete_sequence=['black'])
        st.plotly_chart(fig_q7)  
    if q == '8. Top Mobile Brands users':
        grouped_data_q8= agg_user_brand.groupby("Mobile_brand")["User_count"].sum().sort_values(ascending=False)
        sorted_data1_q8= pd.DataFrame(grouped_data_q8).reset_index()
        fig_q8= px.pie(sorted_data1_q8, values= "User_count", names= "Mobile_brand", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands users")
        fig_q8.update_traces(textposition='inside', textinfo='percent+label',insidetextfont=dict(color='black', weight='bold'))
        st.plotly_chart(fig_q8)
    if q == '9. Districts with highest transaction count':
        TopPieTT(top_district_transaction,'District','Transaction_count')
    if q == '10.Top most used Transaction type':
        grouped_data_q10 = agg_transaction.groupby(["Transaction_type"])["Transaction_amount"].sum().reset_index()
        sorted_data1_q10 = grouped_data_q10.sort_values(by="Transaction_type",ascending = False).head(2)
        fig_q10= px.pie(sorted_data1_q10, values= "Transaction_amount", names= "Transaction_type", title="Top most used Transaction type",
                color_discrete_sequence=px.colors.sequential.Emrld_r)
        st.plotly_chart(fig_q10)

