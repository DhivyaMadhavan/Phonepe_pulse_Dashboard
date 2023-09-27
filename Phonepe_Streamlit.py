
import Phonepe_DataExtraction as file1
import streamlit as st
from mysql import connector
import mysql.connector 
import pandas as pd
import plotly.express as px
from PIL import Image
import time
import json
from urllib.request import urlopen
from streamlit_option_menu import option_menu

css = '''
<style>
section.main > div:has(~ footer ) {
    padding-bottom: 5px;
}
</style>
'''
page_bg_img ="""
<style>
[data-testid="stAppViewContainer"]{
       background: #92c9d9;     
}
</style>
"""

col_slide =""" 
<style>
stSlider[data-baseweb = "slider"]{
 
    background: #92c9d9
      }
</style>"""

st.set_page_config(page_title="Phonepe Pulse",layout='wide')
with st.spinner("Loading..."):
    time.sleep(2)

st.markdown(page_bg_img,unsafe_allow_html=True)
st.markdown(col_slide,unsafe_allow_html=True)

st.write('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)
padding_top = 0
#st.markdown(css, unsafe_allow_html=True)

con = mysql.connector.connect(host = "localhost",
                                user = "root",
                                password = "1234",
                                database = "phonedb")
c = con.cursor()

col1,col2 = st.columns([3,6],gap = "small")
with col1:
      image_logo= Image.open("C:\\Users\\Natarajan\\Desktop\\Dhivya\\DS\\capstone\\Phonepe\\phonepe-logo.png")
      st.image(image_logo)
with col2:      
      st.markdown('#### :brown[PhonepeData Visualization and Exploration]')
      st.markdown("##### :black[A User-Friendly Tool Using Streamlit and Plotly Dashboard]") 

select = option_menu(
    menu_title = None,
    options = ["About","Explore Data","Basic insights"],
    icons =["house","bar-chart","toggles"],
    default_index=0,
    orientation="horizontal",
    styles={"container": {"padding": "0!important", "background-color": "yello","size":"cover", "width": "100%"},
        "icons": {"color": "black", "font-size": "20px"},
        "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#92c9d9"},
        "nav-link-selected": {"background-color": "#264c6d"},
        "icons-selected": {"color":"#fff"}})
if select == 'About': 
          
       st.info(""" The Indian digital payments story has truly captured the world's imagination.
                    From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and states-of-the-art payments infrastructure built as Public Goods championed by the central bank and the government.
                    Founded in December 2015, PhonePe has been a strong beneficiary of the API driven digitisation of payments in India. When we started, we were constantly looking for granular and definitive data sources on digital payments in India.
                    PhonePe Pulse is our way of giving back to the digital payments ecosystem """)
       st.write("---")
       st.markdown("##### :navblue[Technologies used :] Github Cloning, Python, Pandas, MySQL,mysql-connector-python, Streamlit, and Plotly,Geopandas,Matplotlib.pyplot.")
       st.markdown("##### :navblue[Overview :] The Phonepe pulse Github repository contains a large amount of data related tovarious metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.")
       st.markdown("##### :blue[Approach :]")
       st.info("""               
             - Data extraction: The clone of Github using scripting is done to fetch the data from the
               Phonepe pulse Github repository and then stored it in a suitable format such as CSV
               or JSON.
            - Data transformation: Cleaning the data, handling missing values, and transforming the data
               into a format suitable for analysis and visualization is done using pandas.
            - Database insertion: "mysql-connector-python" library in Python  is used to
               connect to a MySQL database and inserted the transformed data using SQL
               commands. 
            - Dashboard: The datas are fetched from mysql database and the data visualisation is done
               using statistical methods depending on the queries.""")     

if select == 'Explore Data':
       #st.markdown("## :blue[Explore]")      
       st.subheader("Let's explore some Data")        
       chkbx = st.checkbox('Add Filters') 
       if chkbx:   
            option_filter = st.radio("select",('Based on payment','Based on transaction type')) 
            if option_filter == 'Based on payment':     
                  col1,col2,col3 = st.columns([3,3,3],gap="small")         
                  with col1:
                        Type = st.selectbox('**Select Type**', ('Transaction','User'),key='in_tr_Type',placeholder="Select...",disabled=False,label_visibility='visible')    
                  with col2:
                        Year = st.slider("**Year**", min_value=2018, max_value=2023)                 
                  with col3:                
                        Quarter = st.slider("Quarter", min_value=1, max_value=4)
                       
                  if Type == 'Transaction':
                        col4,col5 = st.columns([5,5],gap="medium")
                        with col4: 
                              if Year == 2023 and Quarter in [3,4]:
                                    st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4")    
                              else:                              
                                    c.execute(f"select District,sum(Transaction_Amount) as total_amount from Top_Trans_District where Year ={Year} and Quarter = {Quarter} group by District order by total_amount desc limit 15")
                                    df1 = pd.DataFrame(c.fetchall(),columns=["District","Transaction_Amount"])                                    
                                    con.commit()  
                                    fig = px.pie(df1, values='Transaction_Amount',
                                                names='District',
                                                title='District wise transaction amount',
                                                color_discrete_sequence=px.colors.sequential.Agsunset,
                                                hover_data=['Transaction_Amount'])                                                
                                    fig.update_traces(textposition='inside', textinfo='percent+label')
                                    st.plotly_chart(fig,use_container_width=True)  

                        with col5:
                              if Year == 2023 and Quarter in [3,4]:
                                    st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4") 
                              else:
                                    c.execute(f"select District,sum(Transaction_Count) as total_count from Top_Trans_District where Year ={Year} and Quarter = {Quarter} group by District order by total_count desc limit 15")
                                    df1 = pd.DataFrame(c.fetchall(),columns=["District","Transaction_Count"])                                    
                                    con.commit()  
                                    fig = px.pie(df1, values='Transaction_Count',
                                                names='District',
                                                title='District wise transaction count',
                                                color_discrete_sequence=px.colors.sequential.RdBu,
                                                hover_data=['Transaction_Count'],
                                                hole = 0.3)                                            
                                    fig.update_traces(textposition='inside', textinfo='percent+label')
                                    st.plotly_chart(fig,use_container_width=True)  
                        st.info(
                              """
                              Details of Piechart:
                              - 
                              - Pie chart also called as circle chart in the left which is divided into slices illustrates total transaction amount of the districts in proportion
                              - The chart in the right illustrates the transaction count of the districts in propotion
                              - Higher the percentage higher the total transactions and transaction count 
                              - Hover data will show the details like Total transactions, Total amount of the corresponding district
                              """
                              )
                        st.info(
                              """
                              Important Observations:
                              - User can observe Transactions of PhonePe in  Districtwide 
                              - we can see the datas changing to the corresponding type of transaction, years and quarters
                              - We get basic idea about transactions district wide
                              """
                              )            

                  if Type == 'User': 
                        col6,col7 = st.columns([6,6],gap="medium")
                        with col6:          
                              c.execute(f"select State,sum(Transaction_Amount) as total_amount from Top_Trans_District where Year ={Year} and Quarter = {Quarter} group by State order by State")
                              df1 = pd.DataFrame(c.fetchall(),columns=["State","Transaction_Amount"])
                              df2 = pd.read_csv('C:\\Users\\Natarajan\\Desktop\\Dhivya\\DS\\capstone\\Phonepe\\Statenames.csv')
                              df1.State = df2
                              con.commit()
                              
                              fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                          featureidkey='properties.ST_NM',
                                          locations='State',
                                          title = "State wise transaction amount",
                                          color='Transaction_Amount',
                                          color_continuous_scale="Plasma")
                                    
                              fig.update_geos(fitbounds="locations", visible=True)
                              st.plotly_chart(fig, use_container_width=True)

                       with col7:          
                              c.execute(f"select State,sum(Transaction_Count) as total_count from Top_Trans_District where Year ={Year} and Quarter = {Quarter} group by State order by State")
                              df1 = pd.DataFrame(c.fetchall(),columns=["State","Transaction_Count"])
                              df2 = pd.read_csv('C:\\Users\\Natarajan\\Desktop\\Dhivya\\DS\\capstone\\Phonepe\\Statenames.csv')
                              df1.State = df2
                              con.commit()
                              
                              fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                          featureidkey='properties.ST_NM',
                                          locations='State',
                                          title = "State wise transaction count",
                                          color='Transaction_Count',
                                          color_continuous_scale="Reds")                                    
                              fig.update_geos(fitbounds="locations", visible=True)
                              st.plotly_chart(fig, use_container_width=True)

                        fig = px.bar(file1.Agg_User_df, x = 'Brand_type', y = 'Transaction_count', color = 'Quarter', hover_name = 'State', animation_frame = "Year", range_y = [0,100000000],title = "Brand-wise Transaction Count")
                        st.plotly_chart(fig,use_container_width=True) 

                        fig = px.bar(file1.Top_User_District_df, x = 'District', y = 'No_of_Registeredusers', color = 'Quarter', hover_name = 'State',animation_frame='Year',title = "District wise number of registered users")
                        st.plotly_chart(fig,use_container_width=True) 

            if option_filter == 'Based on transaction type':           
                  col1,col2 = st.columns(2)
                  with col1:
                        fig = px.sunburst(file1.Agg_Trans_df, path=['Transaction_type', 'Year'], values='Transaction_count',
                        color='Transaction_type', hover_data=['Transaction_amount'],title = "Yearly wise Transaction data based on Transaction type",
                        color_continuous_scale='RdBu')
                        st.plotly_chart(fig,use_container_width=True)

                        fig = px.pie(file1.Agg_Trans_df, names = 'Quarter', values = 'Transaction_amount',hover_name ="Transaction_type", hole = 0.4,title = "Quarter wise Transaction data based on Transaction type")
                        st.plotly_chart(fig,use_container_width=True)

                  with col2:
                        #fig = px.sunburst(file1.Agg_Trans_df, path=['Transaction_type', 'Quarter'], values='Transaction_amount',
                        #title = "Quarterly Transaction data based on Transaction type") 
                        fig = px.box(file1.Agg_Trans_df, y = "Transaction_count", x = "Transaction_type", points = "all",title = "Transaction count based on Transaction Type")                                    
                        st.plotly_chart(fig,use_container_width=True)

                        fig = px.box(file1.Agg_Trans_df, y = "Transaction_amount", x = "Transaction_type", points = "all",title = "Transaction amount based on Transaction Type")                                    
                        st.plotly_chart(fig,use_container_width=True)

if select == 'Basic insights':      
      col1,col2,col3,col4=st.columns([1,0.5,1,1],gap='small')
      option_empty =col2.empty()
      with col1:
            type=st.selectbox('**Select Type**', ('','Transaction','User'),key='in_tr_type',placeholder="Select...",disabled=False,label_visibility='visible')
      with option_empty: 
            option = st.radio("select",('State','District','Pincodes'))
            
      with col3:
            year=st.slider('**Select Year**', 2018,2023)
      with col4:
            quarter =st.slider('**Select Quarter**', 1,4)  

      
      if type == 'Transaction' and option == "State" :
            trcol1,trcol2 = st.columns([5,7],gap = "medium") 
            with trcol1:
                  st.markdown('#### :blue[State]')            
                  c.execute(f"select State as state,sum(Transaction_Count) as total_count,sum(Transaction_Amount) as total_amount from agg_trans where Year ={year} and Quarter = {quarter} group by state order by total_amount desc limit 10")
                  df1 = pd.DataFrame(c.fetchall(),columns=["State","Transaction_Count","Transaction_Amount"]) 
                  st.dataframe(df1) 
                         
            with trcol2:
                  fig = px.bar(df1, x = 'State', y = 'Transaction_Amount', color = 'Transaction_Count')
                  st.plotly_chart(fig,use_container_width=True)

            
      elif type == 'Transaction' and option == "District":               
            trcol1,trcol2 = st.columns([5,7],gap = "medium") 
            with trcol1:
                  st.markdown('#### :blue[District]')                               
                  c.execute(f"select District as district,sum(District_Count) as total_count ,sum(District_Amount)as total_amount  from map_trans where year = {year} and quarter = {quarter} group by district order by total_amount desc limit 10" )
                  df2 = pd.DataFrame(c.fetchall(),columns=["District","District_Count","District_Amount"]) 
                  st.dataframe(df2)
                  with trcol2:
                        fig = px.bar(df2, x = 'District', y = 'District_Amount', color = 'District_Count')
                        st.plotly_chart(fig,use_container_width=True)
                  
      elif (type == 'Transaction' and option == "Pincodes") :
        trcol1,trcol2 = st.columns([5,7],gap = "medium") 
        with trcol1:
                  st.markdown('#### :blue[Pincodes]')            
                  c.execute(f"select Pincodes as pincodes,sum(Transaction_Count) as total_count,sum(Transaction_Amount) as total_amount from top_trans_pincode where Year ={year} and Quarter = {quarter} group by pincodes order by total_amount desc limit 10")
                  df3 = pd.DataFrame(c.fetchall(),columns=["Pincodes","Transaction_count","Transaction_amount"]) 
                  st.dataframe(df3)        
        with trcol2:
                  fig = px.bar(df3, x = 'Pincodes', y = 'Transaction_amount', color = 'Transaction_count')
                  st.plotly_chart(fig,use_container_width=True) 

      if type == 'User':
            with option_empty: 
                  option_new = st.radio("select",('State','District','Pincodes','Brand'))                   

      if(type == 'User' and option_new == "State") :            
            trcol1,trcol2 = st.columns([5,7],gap = "medium")            
            with trcol1:
                  st.markdown('#### :blue[State]') 
                  if year == 2023 and quarter in [3,4]:
                        st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4") 
                  else:                
                        c.execute(f"select State,sum(No_of_Registeredusers) as Total_Users  from map_user where Year ={year} and Quarter = {quarter} group by State order by Total_Users desc limit 10")
                        df4 = pd.DataFrame(c.fetchall(),columns=["State","No_of_Registeredusers"])
                        st.dataframe(df4)        
                        with trcol2:
                              fig = px.bar(df4, x = 'State', y = 'No_of_Registeredusers', color = 'No_of_Registeredusers', title = "State wise registered users")
                              st.plotly_chart(fig,use_container_width=True)       

      elif(type == 'User' and option_new == "District") :
            trcol1,trcol2 = st.columns([5,7],gap = "medium") 
            with trcol1:
                  st.markdown('#### :blue[District]')   
                  if year == 2023 and quarter in [3,4]:
                        st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4")
                  else:                           
                        c.execute(f"select District,sum(No_of_Registeredusers) as Total_Users from map_user where Year ={year} and Quarter = {quarter} group by District order by Total_Users desc limit 10")
                        df5 = pd.DataFrame(c.fetchall(),columns=["District","No_of_Registeredusers"]) 
                        st.dataframe(df5)        
                        with trcol2:
                              fig = px.pie(df5, names = 'District', values = 'No_of_Registeredusers')
                              st.plotly_chart(fig,use_container_width=True) 

      elif(type == 'User' and option_new == "Pincodes") :
            trcol1,trcol2 = st.columns([5,7],gap = "medium") 
            with trcol1:
                  st.markdown('#### :blue[Pincodes]') 
                  if year == 2023 and quarter in [3,4]:
                         st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4") 
                  else:                 
                        c.execute(f"select Pincodes,sum(No_of_Registeredusers) as Total_Users from Top_User_Pincode where Year ={year} and Quarter = {quarter} group by Pincodes order by Total_Users desc limit 10")
                        df6 = pd.DataFrame(c.fetchall(),columns=["Pincodes","No_of_Registeredusers"]) 
                        st.dataframe(df6)        
                        with trcol2:
                              fig = px.scatter(df6, x = 'Pincodes', y = 'No_of_Registeredusers')
                              st.plotly_chart(fig,use_container_width=True) 

      elif(type == 'User' and option_new == "Brand"):            
            trcol1,trcol2 = st.columns([5,7],gap = "medium") 
            with trcol1:
                  st.markdown('#### :blue[Brand]') 
                  if year == 2023 and quarter in [3,4]:
                        st.markdown("#### Sorry No Data to Display for 2023 Qtr 3,4") 
                  else:                
                        c.execute(f"select Brand_Type,sum(Transaction_Count) as total_count,(Avg(Transaction_Percentage)*100) as avg_percentage from agg_user where Year ={year} and Quarter = {quarter} group by Brand_Type order by total_count desc limit 10")
                        df7 = pd.DataFrame(c.fetchall(),columns=["Brand_Type","Transaction_count","Transaction_Percentage"]) 
                        st.dataframe(df7)        
                        with trcol2:
                              fig = px.bar(df7, x = 'Brand_Type', y = 'Transaction_count', color = 'Transaction_Percentage')
                              st.plotly_chart(fig,use_container_width=True)              
                                                              
