import pandas as pd
import os
import subprocess
import json
from pprint import pprint
import mysql.connector

repo_url = "https://github.com/PhonePe/pulse.git"
repo_dir = r"C:\Users\Natarajan\Desktop\Dhivya\DS\capstone\Phonepe\Dataclone"
subprocess.Popen(['git', 'clone', str(repo_url), repo_dir])

#Aggregated Transaction   
path1="C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/aggregated/transaction/country/india/state/"
Agg_trans_list=os.listdir(path1)
columns1={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for state in Agg_trans_list:
    cur_state=path1+state+"/"
    Agg_yr_list=os.listdir(cur_state)
    for year in Agg_yr_list:
        cur_year= cur_state+year+'/'
        Agg_file_list=os.listdir(cur_year)
        for file in Agg_file_list:
            cur_file=cur_year+file
            Data1=open(cur_file,'r')
            toload_data1=json.load(Data1)
            try:
                for z in toload_data1['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']

                    columns1['Transaction_type'].append(Name)
                    columns1['Transaction_count'].append(count)
                    columns1['Transaction_amount'].append(amount)
                    columns1['State'].append(state)
                    columns1['Year'].append(year)
                    columns1['Quarter'].append(int(file.strip('.json')))
            except:
               pass       

Agg_Trans_df=pd.DataFrame(columns1) 

#Aggregated User           
path2="C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/aggregated/user/country/india/state/"
Agg_user_list=os.listdir(path2)
columns2={'State':[], 'Year':[],'Quarter':[],'Brand_type':[],'Transaction_count':[], 'Transaction_percentage':[]}
for state in Agg_user_list:
    cur_state=path2+state+"/"
    Agg_yr_list=os.listdir(cur_state)

    for year in Agg_yr_list:
        cur_year= cur_state+year+'/'
        Agg_file_list=os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file=cur_year+file
            Data2=open(cur_file,'r')
            toload_data2=json.load(Data2)

            try:            
                for z in toload_data2['data']["usersByDevice"]:
                    brand=z['brand']
                    count=z['count']
                    percentage=z['percentage']

                    columns2['Brand_type'].append(brand)
                    columns2['Transaction_count'].append(count)
                    columns2['Transaction_percentage'].append(percentage)
                    columns2['State'].append(state)
                    columns2['Year'].append(year)
                    columns2['Quarter'].append(file.strip('.json'))
            except:
                pass  
              
Agg_User_df=pd.DataFrame(columns2)


#MapTransaction 
path3 = "C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/map/transaction/hover/country/india/state/"
map_trans_list=os.listdir(path3)

columns3={'State':[], 'Year':[],'Quarter':[],'District':[],'District_count':[], 'District_amount':[]}
for state in map_trans_list:
    cur_state=path3+state+"/"
    map_yr_list=os.listdir(cur_state)

    for year in map_yr_list:
        cur_yr=cur_state+year+'/'
        map_file_list=os.listdir(cur_yr)

        for year in map_yr_list:
            cur_yr=cur_state+year+'/'
            map_file_list=os.listdir(cur_yr)

            for file in map_file_list:
                cur_file=cur_yr+file
                Data3=open(cur_file,'r')
                toload_data3=json.load(Data3)
                try:
                    for z in toload_data3['data']['hoverDataList']:
                        name=z['name']
                        count=z['metric'][0]['count']
                        amount=z['metric'][0]['amount']

                        columns3['District'].append(name)
                        columns3['District_count'].append(count)
                        columns3['District_amount'].append(amount)
                        columns3['State'].append(state)
                        columns3['Year'].append(year)
                        columns3['Quarter'].append(file.strip('.json'))
                except:
                    pass  
                  
Map_Trans_df=pd.DataFrame(columns3)


#Map User
path4 = 'C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/map/user/hover/country/india/state/'
map_user_list=os.listdir(path4)


columns4={'State':[], 'Year':[],'Quarter':[],'District':[],'No_of_Registeredusers':[], 'No_of_Appopen':[]}
for state in map_user_list:
    cur_state=path4+state+"/"
    map_yr_list=os.listdir(cur_state)

    for year in map_yr_list:
        cur_yr=cur_state+year+'/'
        map_file_list=os.listdir(cur_yr)

        for year in map_yr_list:
            cur_yr=cur_state+year+'/'
            map_file_list=os.listdir(cur_yr)

            for file in map_file_list:
                cur_file=cur_yr+file
                Data4=open(cur_file,'r')
                toload_data4=json.load(Data4)
                try:    
                    for z in toload_data4['data']['hoverData'].items():
                        name = z[0]
                        regUsers=z[1]['registeredUsers']
                        appOpen=z[1]['appOpens']

                        columns4['District'].append(name)
                        columns4['No_of_Registeredusers'].append(regUsers)
                        columns4['No_of_Appopen'].append(appOpen)
                        columns4['State'].append(state)
                        columns4['Year'].append(year)
                        columns4['Quarter'].append(file.strip('.json'))
                except:
                    pass  
                  
Map_User_df=pd.DataFrame(columns4)

##Top Transaction based on pincode
path5='C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/top/transaction/country/india/state/'
top_trans_pin_list=os.listdir(path5)

columns5={'State':[], 'Year':[],'Quarter':[],'Pincodes':[],'Transaction_count':[], 'Transaction_amount':[]}
for state in top_trans_pin_list:
    cur_state=path5+state+"/"
    top_yr_list=os.listdir(cur_state)

    for year in top_yr_list:
        cur_yr=cur_state+year+'/'
        top_file_list=os.listdir(cur_yr)

        for year in top_yr_list:
            cur_yr=cur_state+year+'/'
            top_file_list=os.listdir(cur_yr)

            for file in top_file_list:
                cur_file=cur_yr+file
                Data5=open(cur_file,'r')
                toload_data5=json.load(Data5)
                try:
                    for z in toload_data5['data']['pincodes']:
                        pincode = z['entityName']
                        count=z['metric']['count']
                        amount=z['metric']['amount']

                        columns5['Pincodes'].append(pincode)
                        columns5['Transaction_count'].append(count)
                        columns5['Transaction_amount'].append(amount)
                        columns5['State'].append(state)
                        columns5['Year'].append(year)
                        columns5['Quarter'].append(file.strip('.json'))
                except:
                    pass
                  
Top_Trans_Pincode_df=pd.DataFrame(columns5)
          
##Top Transaction based on disctrict                
path6='C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/top/transaction/country/india/state/'
top_trans_district_list=os.listdir(path6)

columns6={'State':[], 'Year':[],'Quarter':[],'District':[],'Transaction_count':[], 'Transaction_amount':[]}
for state in top_trans_district_list:
    cur_state=path6+state+"/"
    top_yr_list=os.listdir(cur_state)

    for year in top_yr_list:
        cur_yr=cur_state+year+'/'
        top_file_list=os.listdir(cur_yr)

        for year in top_yr_list:
            cur_yr=cur_state+year+'/'
            top_file_list=os.listdir(cur_yr)

            for file in top_file_list:
                cur_file=cur_yr+file
                Data6=open(cur_file,'r')
                toload_data6=json.load(Data6)
                try:
                    for z in toload_data6['data']['districts']:
                        district = z['entityName']
                        count=z['metric']['count']
                        amount=z['metric']['amount']

                        columns6['District'].append(district)
                        columns6['Transaction_count'].append(count)
                        columns6['Transaction_amount'].append(amount)
                        columns6['State'].append(state)
                        columns6['Year'].append(year)
                        columns6['Quarter'].append(file.strip('.json'))
                except:
                    pass

Top_Trans_District_df=pd.DataFrame(columns6) 


#Top User 
path7 = "C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/top/user/country/india/state/"
top_user_pin_list=os.listdir(path7)

columns7={'State':[], 'Year':[],'Quarter':[],'Pincodes':[],'No_of_Registeredusers':[]}
for state in top_user_pin_list:
    cur_state=path7+state+"/"
    top_yr_list=os.listdir(cur_state)

    for year in top_yr_list:
        cur_yr=cur_state+year+'/'
        top_file_list=os.listdir(cur_yr)

        for year in top_yr_list:
            cur_yr=cur_state+year+'/'
            top_file_list=os.listdir(cur_yr)

            for file in top_file_list:
                cur_file=cur_yr+file
                Data7=open(cur_file,'r')
                toload_data7=json.load(Data7)
                try:
                    for z in toload_data7['data']['pincodes']:
                        pincode = z['name']
                        regUsers = z['registeredUsers']

                        columns7['Pincodes'].append(pincode)
                        columns7['No_of_Registeredusers'].append(regUsers)
                        columns7['State'].append(state)
                        columns7['Year'].append(year)
                        columns7['Quarter'].append(file.strip('.json'))
                except:
                    pass

Top_User_Pincode_df=pd.DataFrame(columns7)  
          
path8 = "C:/Users/Natarajan/Desktop/Dhivya/DS/capstone/Phonepe/Dataclone/data/top/user/country/india/state/"
top_user_district_list=os.listdir(path8)

columns8={'State':[], 'Year':[],'Quarter':[],'District':[],'No_of_Registeredusers':[]}
for state in top_user_district_list:
    cur_state=path8+state+"/"
    top_yr_list=os.listdir(cur_state)

    for year in top_yr_list:
        cur_yr=cur_state+year+'/'
        top_file_list=os.listdir(cur_yr)

        for year in top_yr_list:
            cur_yr=cur_state+year+'/'
            top_file_list=os.listdir(cur_yr)

            for file in top_file_list:
                cur_file=cur_yr+file
                Data8=open(cur_file,'r')
                toload_data8=json.load(Data8)
                try:
                    for z in toload_data8['data']['districts']:
                        district = z['name']
                        regUsers = z['registeredUsers']

                        columns8['District'].append(district)
                        columns8['No_of_Registeredusers'].append(regUsers)
                        columns8['State'].append(state)
                        columns8['Year'].append(year)
                        columns8['Quarter'].append(file.strip('.json'))
                except:
                    pass 
                  
Top_User_District_df=pd.DataFrame(columns8)   

################################################################################
def data_clean():   
    #getting the info of dataframes
    #Agg_Trans_df.info()
    #Agg_User_df.info()
    #Map_Trans_df.info()
    #Map_User_df.info()
    #Top_Trans_Pincode_df.info()
    #Top_Trans_District_df.info()
    #Top_User_Pincode_df.info()
    #Top_User_District_df.info()

    #checking for duplicated values  and dropping them
    Agg_Trans_df.duplicated().any()
    Agg_User_df.duplicated().any()
    Map_Trans_df.duplicated().any()
    Map_Trans_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)
    Map_User_df.duplicated().any()
    Map_User_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)
    Top_Trans_Pincode_df.duplicated().any()
    Top_Trans_Pincode_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)
    Top_Trans_District_df.duplicated().any()
    Top_Trans_District_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)
    Top_User_Pincode_df.duplicated().any()
    Top_User_Pincode_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)
    Top_User_District_df.duplicated().any()
    Top_User_District_df.drop_duplicates(keep = 'first',inplace=True,ignore_index= False)

    #finding null values
    Agg_Trans_df.isnull().sum()
    Agg_User_df.isnull().sum()
    Map_Trans_df.isnull().sum()
    Map_User_df.isnull().sum()
    Top_Trans_Pincode_df.isnull().sum()
    Top_Trans_District_df.isnull().sum()
    Top_User_Pincode_df.isnull().sum()
    Top_User_District_df.isnull().sum()

    #stripping off any whitespaces
    Agg_Trans_df.rename(columns=lambda x: x.strip(), inplace=True)
    Agg_User_df.rename(columns=lambda x: x.strip(), inplace=True)
    Map_Trans_df.rename(columns=lambda x: x.strip(), inplace=True)
    Map_User_df.rename(columns=lambda x: x.strip(), inplace=True)
    Top_Trans_Pincode_df.rename(columns=lambda x: x.strip(), inplace=True)
    Top_Trans_District_df.rename(columns=lambda x: x.strip(), inplace=True)
    Top_User_Pincode_df.rename(columns=lambda x: x.strip(), inplace=True)
    Top_User_District_df.rename(columns=lambda x: x.strip(), inplace=True)

     #creating CSV files
    Agg_Trans_df.to_csv('Agg_Trans_file.csv',index = False)
    Agg_User_df.to_csv('Agg_User_file.csv',index = False)
    Map_Trans_df.to_csv('Map_Trans_file.csv',index = False)
    Map_User_df.to_csv('Map_User_file.csv',index = False)
    Top_Trans_Pincode_df.to_csv('Top_Trans_Pincode_file.csv',index = False)
    Top_Trans_District_df.to_csv('Top_Trans_District_file.csv',index = False)
    Top_User_Pincode_df.to_csv('Top_User_Pincode_file.csv',index = False)
    Top_User_District_df.to_csv('Top_User_District_file.csv',index = False)
    
    #Establishing Sqlite3 connectivity
    con = mysql.connector.connect(host = "localhost",
                                user = "root",
                                password = "1234",
                                database = "phonedb")

c = con.cursor()
print("Mysql conenction established")

#creation of various tables
#1.creating Aggregation Transaction 
def SQL_tables():
    Agg_Trans_SQL_Table='''CREATE TABLE Agg_Trans ( sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    Transcation_Type varchar(100), 
                    Transaction_Count int,
                    Transaction_Amount float); '''
    c.execute(Agg_Trans_SQL_Table)
    # Inserting Values to Top_User table
    for i,row in Agg_Trans_df.iterrows():
        mysql='INSERT INTO Agg_Trans (State,Year,Quarter,Transcation_Type,Transaction_Count,Transaction_Amount)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['Transaction_type'],row['Transaction_count'],row['Transaction_amount'])
        c.execute(mysql,val)
    con.commit()
    
#2.creating Aggregation User Table
    Agg_User_SQL_Table='''CREATE TABLE Agg_User (sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    Brand_Type varchar(100), 
                    Transaction_Count int,
                    Transaction_Percentage float); '''
    c.execute(Agg_User_SQL_Table)
    # Inserting Values to Top_User table
    for i,row in Agg_User_df.iterrows():
        mysql='INSERT INTO Agg_User (State,Year,Quarter,Brand_Type,Transaction_Count,Transaction_Percentage)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['Brand_type'],row['Transaction_count'],row['Transaction_percentage'])
        c.execute(mysql,val)
    con.commit()
    
#3.creating Map Transaction Table
    Map_Trans_SQL_Table='''CREATE TABLE Map_Trans ( sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    District varchar(100), 
                    District_Count int,
                    District_Amount float); '''
    c.execute(Map_Trans_SQL_Table)
    # Inserting Values to Top_User table
    for i,row in Map_Trans_df.iterrows():
        mysql='INSERT INTO Map_Trans (State,Year,Quarter,District,District_count,District_amount)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['District'],row['District_count'],row['District_amount'])
        c.execute(mysql,val)
    con.commit()
    
 #4.creating Map User Table
    Map_User_SQL_Table='''CREATE TABLE Map_User (sno INT AUTO_INCREMENT PRIMARY KEY, 
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    District varchar(100), 
                    No_of_Registeredusers int,
                    No_of_Appopen int); '''
    c.execute(Map_User_SQL_Table)
    for i,row in Map_User_df.iterrows():
        mysql='INSERT INTO Map_User (State,Year,Quarter,District,No_of_Registeredusers,No_of_Appopen)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['District'],row['No_of_Registeredusers'],row['No_of_Appopen'])
        c.execute(mysql,val)
    con.commit()
    
    #5.creating Top Transaction Table
    Top_Trans_Pincode_SQL_Table='''CREATE TABLE Top_Trans_Pincode (sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    Pincodes int, 
                    Transaction_Count int,
                    Transaction_Amount float); '''
    c.execute(Top_Trans_Pincode_SQL_Table)
    for i,row in Top_Trans_Pincode_df.iterrows():
        mysql='INSERT INTO Top_Trans_Pincode(State,Year,Quarter,Pincodes,Transaction_Count,Transaction_Amount)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['Pincodes'],row['Transaction_count'],row['Transaction_amount'])
        c.execute(mysql,val)
    con.commit()
    
    #6.creating Top Transaction Table
    Top_Trans_District_SQL_Table='''CREATE TABLE Top_Trans_District (sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    District varchar(100), 
                    Transaction_Count int,
                    Transaction_Amount float); '''
    c.execute(Top_Trans_District_SQL_Table)
    for i,row in Top_Trans_District_df.iterrows():
        mysql='INSERT INTO Top_Trans_District(State,Year,Quarter,District,Transaction_Count,Transaction_Amount)VALUES (%s,%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['District'],row['Transaction_count'],row['Transaction_amount'])
        c.execute(mysql,val)
    con.commit()
   
    #7.creating Top Transaction Table
    Top_User_Pincode_SQL_Table='''CREATE TABLE Top_User_Pincode (sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    Pincodes int, 
                    No_of_Registeredusers int); '''
    c.execute(Top_User_Pincode_SQL_Table)
    for i,row in Top_User_Pincode_df.iterrows():
        mysql='INSERT INTO Top_User_Pincode(State,Year,Quarter,Pincodes,No_of_Registeredusers)VALUES (%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['Pincodes'],row['No_of_Registeredusers'])
        c.execute(mysql,val)
    con.commit()

    #8.creating Top Transaction Table
    Top_User_District_SQL_Table='''CREATE TABLE Top_User_District (sno INT AUTO_INCREMENT PRIMARY KEY,
                    State varchar(100), 
                    Year int,
                    Quarter int, 
                    District varchar(100), 
                    No_of_Registeredusers int); '''
    c.execute(Top_User_District_SQL_Table)
    for i,row in Top_User_District_df.iterrows():
        mysql='INSERT INTO Top_User_District(State,Year,Quarter,District,No_of_Registeredusers)VALUES (%s,%s,%s,%s,%s)'
        val=(row['State'],row['Year'],row['Quarter'],row['District'],row['No_of_Registeredusers'])
        c.execute(mysql,val)
    con.commit()   
#calling the functions 
data_clean()
print("Cleaning of data is done")
SQL_tables()


