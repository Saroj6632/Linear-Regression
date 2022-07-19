import pandas as pd
import env
import os

# function to establish connection to MySQL workbench to retrieve data.
def get_connection(db, user=env.username, host=env.host, password=env.password):
    return f'mysql+pymysql://{env.username}:{env.password}@{env.host}/{db}'



# function that returns the zillow data with the columns we wanted using sql querry
def zillow_data():
    sql_querry = '''
                 select 
                 bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, fips
                 from properties_2017
                 join propertylandusetype using (propertylandusetypeid)
                 where propertylandusedesc = "Single Family Residential"
                 '''
    #Read in DataFrame from Database server
    df= pd.read_sql(sql_querry, get_connection('zillow'))
    
    return df



# below reads zillow database from codeup, writes data to csv file if local file doesnot exist
def get_zillow_data():
    filename = "zillow.csv"
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        df = zillow_data()
        df.to_csv(filename)
        return df
                

def null_handlers(df):
    ''' This function handles null values in zillow data by dropping all null values
    as 99.41% of data sre retianed even after dropping all rows containing nulls'''
    df=df.dropna()
    return df

def optimize_types(df):
    # Convert some columns to integers
    # fips, yearbuilt, and bedrooms can be integers
    df["fips"] = df["fips"].astype(int)
    df["yearbuilt"] = df["yearbuilt"].astype(int)
    df["bedroomcnt"] = df["bedroomcnt"].astype(int)    
    df["taxvaluedollarcnt"] = df["taxvaluedollarcnt"].astype(int)
    df["calculatedfinishedsquarefeet"] = df["calculatedfinishedsquarefeet"].astype(int)
    return df


def handle_outliers(df):
    """Manually handle outliers that do not represent properties likely for 99% of buyers and zillow visitors"""
    df = df[df.bathroomcnt <= 6]
    
    df = df[df.bedroomcnt <= 6]

    df = df[df.taxvaluedollarcnt < 2_000_000]

    return df


def wrangle_zillow():
    """
    Acquires Zillow data
    Handles nulls
    optimizes or fixes data types
    handles outliers w/ manual logic
    returns a clean dataframe
    """
    df = get_zillow_data()

    df = null_handlers(df)

    df = optimize_types(df)

    df = handle_outliers(df)

    df.to_csv("zillow.csv", index=False)

    return df
    