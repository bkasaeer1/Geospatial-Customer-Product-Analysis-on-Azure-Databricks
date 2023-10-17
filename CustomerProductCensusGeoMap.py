#imports
import time
import pandas as pd 
import numpy as np
import os
from itertools import chain

from glob import glob
from pyspark.sql.types import *
import pyspark.sql.functions as f
import matplotlib.pyplot as plt

#product model with their corresponding code, in terms of quality A > B > C! 
product_model_dict = {4:'C', 2:'B', 1: 'A'}

#also define the geographic leves and their number of digits! 
subgeo_dict = {'Blocks':15, 'Block Groups':12, 'Tracts':11, 'Counties':5, 'States':2}

#specify the version of products dataset!
product_data_version = 'p3'

#specify the version of customers data!
customer_data_version = 'c5'

#All means the entire USA will be processed. To do only one state write state fips code like 11 for DC.
state_fips = 'All'

#if you did not have delta in hand and want to generate it for future faster runs below should be True!
#otherwise if you prefer to run it from csvs, make below False and make sure all CSV sources in section below are valid!
generateDelta = True

############Specify input and output directories within mounted drives
#path to all delta tables!
deltaPath = '/mnt/Delta'

#Specify the mounted folder in which you like your outputs to be stored in! 
outputCSV_location = '/mnt/output/%s'%product_data_version

#specify location some base data that are static and won't change in time! 
baseDataCSV_location = "/mnt/base/baselines"

#specify the mounted folder in which the potnetial customers data are located
customerDataCSV_location = "/mnt/customer/%s"%customer_data_version

#specify the mounted folder in which your products data is located
productDataCSV_location = "/mnt/product/v%s"%product_data_version


def create_state_sdf():
    
    """
    This function creates a Spark DataFrame containing state names, abbreviations, and FIPS codes using SQL queries.

    INPUTS:
    This function has no inputs as the inputs are defined inside it.

    OUTPUTS:
    A Spark DataFrame containing the state info.
    """
    
    # This includes US state names, abbreviations, and FIPS code.
    # Source: https://www.census.gov/library/reference/code-lists/ansi.html
    state_dict = {
        '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas', '06': 'California',
        '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia',
        '12': 'Florida', '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois',
        '18': 'Indiana', '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana',
        '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
        '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska', '32': 'Nevada',
        '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico', '36': 'New York',
        '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma', '41': 'Oregon',
        '42': 'Pennsylvania', '44': 'Rhode Island', '45': 'South Carolina', '46': 'South Dakota',
        '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont', '51': 'Virginia',
        '53': 'Washington', '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming',
        '60': 'American Samoa', '66': 'Guam', '69': 'Commonwealth of the Northern Mariana Islands',
        '72': 'Puerto Rico', '78': 'United States Virgin Islands'
    }

    state_abbr_dict = {
        '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE',
        '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA',
        '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN',
        '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM',
        '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
        '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA',
        '54': 'WV', '55': 'WI', '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP',
        '72': 'PR', '78': 'VI'
    }

    # Create a temporary view of the data
    spark.createDataFrame(pd.DataFrame({'STATEFP20': state_dict.keys(),
                                        'state_name': state_dict.values(),
                                        'state_abbr': state_abbr_dict.values()})).createOrReplaceTempView("tmp_state_data")

    # Execute a SQL query to select the desired columns
    state_sdf = spark.sql("SELECT STATEFP20, state_name, state_abbr FROM tmp_state_data")

    return state_sdf


def convert_sdf_to_delta(sdf, deltaTablePath):
    
    """
    This function converts the spark data table to delta table format to improve the performance! 

    INPUTS:
    sdf: The spark df to be converted to delta format!  
    delta_path: path to the delta table to be written!

    OUTPUTS: 
    a delta table equivalent of the spark df! 
    """
    
    # write table to delta 
    sdf.write.format('delta').save(deltaTablePath)
    return spark.read.load(deltaTablePath)


def read_base_data(subgeo, generateDelta = False):
    
    """
    This function reads the base information for a specific subgeography using data previously generated from 
    past version of business dataset and is static. Pre loading this base data significantly reduces the 
    computation time since most of these parameters are static and won't change in a long period of time.

    INPUTS:
    subgeo: the target subgeography from a list of keys in subgeo_dict! 
    generateDelta: if delta doesn't exists, this allows the user to generate it and take advantage of it!

    OUTPUTS: 
    a spark dataframe containing the base data for the target subgeography!
    """
    
    delta_location = deltaPath + "/Base/%sBaseData.delta"%subgeo
    csv_location = baseDataCSV_location + "/%sBaseData.csv"%subgeo
    
    try:
        base_sdf = spark.read.load(delta_location)
    except:  
        
        #read columns and customize schema 
        cols = spark.read.format('csv') \
                    .options(header = True, inferSchema = False) \
                    .option("sep", ",") \
                    .load(csv_location).columns

        #define data types!
        df_schema = StructType([StructField(c, StringType()) for c in cols])

        base_sdf = spark.read.format('csv') \
                        .schema(df_schema) \
                        .options(header = True, inferSchema = False) \
                        .option("sep", ",") \
                        .load(csv_location) 
        #now if generate request is in place: 
        if generateDelta:
            base_sdf = convert_sdf_to_delta(base_sdf, delta_location)
            
    return base_sdf


def read_customer_data(customer_data_version, state_fips = 'All', generateDelta = False):
    
    """
    This function reads the customer data geographic locations for the specified version of the table using SQL queries!

    INPUTS:
    customer_data_version: The customers data version!
    state_fips: This is to ensure processing can be narrowed down to a specific state for the testing phase.
    Default is All, meaning the entire USA, but can be any FIPS code like 11, 51, etc.
    generateDelta: If a delta doesn't exist, this allows the user to generate it and take advantage of it!

    OUTPUTS:
    A Spark DataFrame containing the customers' positions for the specified version.
    """
    
    # AllProducts: all business product positions
    delta_location = deltaPath + '/customerLocations%s.delta' % customer_data_version
    csv_location = customerDataCSV_location + "/%s.csv" % customer_data_version
    
    try:
        # Attempt to load data from delta location
        AllCustomers = spark.read.load(delta_location)
    except:
        # Read CSV data and apply SQL operations
        spark.read.format('csv') \
            .options(header=True, inferSchema=False) \
            .option("sep", ",") \
            .load(csv_location).createOrReplaceTempView("tmp_data")

        # Define schema and select desired columns
        sql_query = """
        SELECT 
            CAST(customer_id AS STRING) AS customer_id,
            CAST(latitude AS FLOAT) AS latitude,
            CAST(longitude AS FLOAT) AS longitude,
            CAST(GEOID AS STRING) AS GEOID
        FROM tmp_data
        WHERE customer_flag = 'True'
        """
        
        AllCustomers = spark.sql(sql_query)

        # If generate request is in place, convert to delta
        if generateDelta:
            AllCustomers = convert_sdf_to_delta(AllCustomers, delta_location)

    # If one state is selected, narrow down data to that state
    if state_fips != 'All':
        AllCustomers.createOrReplaceTempView("tmp_customers")
        
        sql_query = f"""
        SELECT * FROM tmp_customers
        WHERE CAST(SUBSTRING(GEOID, 1, 2) AS INT) = {state_fips}
        """
        
        AllCustomers = spark.sql(sql_query)

    return AllCustomers.select('customer_id', 'GEOID')


def read_products_data(product_data_version, state_fips = 'All', generateDelta = False):
    
    """
    This function reads business data for a specific version of the dataset and state. 

    INPUTS:
    product_data_version: The products data version! 
    state_fips: this is to ensure processing can be narrowed down to a specific state. 
    Default is All means the entire USA but can be any fips code like 11, 55, etc.
    generateDelta: if delta doesnt exists, this allows the user to generate it and take advantage of it!

    OUTPUTS: 
    a spark dataframe containing the product details for the desired version of dataset. 
    """
    
    ##### Load product detailed Data #####
    delta_location = deltaPath + '/product%s.delta'%product_data_version
    csv_location = productDataCSV_location + "/product_*.csv"

    try:
        productSDF = spark.read.load(delta_location)
    except: 
        #read columns and customize schema 
        cols = spark.read.format('csv') \
                    .options(header = True, inferSchema = False) \
                    .option("sep", ",") \
                    .load(csv_location).columns
        #product_tier: 1 means product has a warranty and 0 means it does not come with a warranty.
        dtype_dict= {'business_id': StringType(),'customer_id': StringType(), 'block_geoid': StringType(), 
                          'product_model': IntegerType(), 'product_max_power': IntegerType(),'product_min_power': IntegerType(), 
                          'product_tier': IntegerType()}

        df_schema = StructType([StructField(c, dtype_dict[c]) for  c in cols]) 

        productSDF = spark.read.format('csv') \
                    .schema(df_schema) \
                    .option("mode", "FAILFAST") \
                    .options(header = True, inferSchema = False) \
                    .option("quote", "\"") \
                    .option("escape", "\"") \
                    .load(csv_location) 
                    

        #now if generate request is in place: 
        if generateDelta:
            productSDF = convert_sdf_to_delta(productSDF, delta_location)
    #if one state is selected, then need to narrow down data to one state! 

    if state_fips != 'All':
        
        productSDF.createOrReplaceTempView("tmp_products")
        
        sql_query = f"""
        SELECT * FROM tmp_products
        WHERE CAST(SUBSTRING(GEOID, 1, 2) AS INT) = {state_fips}
        """
        productSDF = spark.sql(sql_query)
        
    return productSDF.select('customer_id', 'block_geoid', 'product_model', 'product_max_power',
                            'product_min_power', 'product_tier')


def process_subgeo(subgeo):
    
    """
    This function processes the product quality and availibility for a selected Census subgeography level!  

    INPUTS:
    subgeo: the target subgeography from a list of keys in subgeo_dict! 

    OUTPUTS: 
    a spark dataframe at the subgeo level containing the aggregated fields from products  
    data in addition to the baseline information such as socio economic factors like ACS poverty, income etc. 
    """
    
    start = time.time()  
    
    # Create a temporary view for AllCustomers
    AllCustomers.createOrReplaceTempView("tmp_AllCustomers")
    
    # Calculate total customers for the selected subgeography
    total_customers_query = f"""
        SELECT DISTINCT substring(GEOID, 1, {subgeo_dict[subgeo]}) as GEOID
        FROM tmp_AllCustomers
    """
    AllCustomers_subgeo = spark.sql(total_customers_query)
    
    # Calculate the total number of customers per GEOID
    total_customers_query = """
        SELECT GEOID, COUNT(customer_id) as TotalCustomers
        FROM tmp_AllCustomers
        GROUP BY GEOID
    """
    TotalServices_subgeo = spark.sql(total_customers_query)
    
    #create a list to store all the triples of sdf! TotalCustomers should be there already!
    sdfs = [[TotalServices_subgeo]]
    
    #now go through all the tech codes from model_dict and generate sdfs! 
    for product_model in ['All'] + list(product_model_dict.keys()):
        #build the A/B/C triple!
        quality_sdfs = quality_based_serving(TotalServices_subgeo, product_model)
        #add it to the list!
        sdfs.append(quality_sdfs)

    #flatten the list of lists! 
    sdfs = list(chain(*sdfs))
    #read in the base data for the subgeo!
    base_sdf = read_base_data(subgeo, generateDelta)
    #go in a loop and join all the sdfs 
    for sdf in sdfs:
        base_sdf = base_sdf.join(sdf, on = 'GEOID', how = 'left')
    #now write the output CSV to Azure blob storage!
    _ = generate_output(base_sdf, subgeo)
    
    end = time.time()  
    deltaT = end - start
    print('product data processing for %s was accomplished in %s minutes!'%(
        subgeo, round(deltaT/60, 1)))
    return deltaT


def quality_slicer(sdf, level='Bronze', model_code='All'):
    
    """
    This function prepares a quality filter (A/B/C) using any specific spark df!

    INPUTS:
    sdf: the spark df to slice!
    level: e.g. Gold! It can be Bronze, Silver, or Gold from the lowest to highest quality products. Default is Bronze. 
    model_code: the product model code! See the product_model_dict dictionary! The default is All meaning all models!

    OUTPUTS: 
    A logical selector that helps with slicing the products data. 
    """
    
    # Create a temporary view for the input DataFrame
    sdf.createOrReplaceTempView("tmp_sdf")
    
    # Apply filters based on the input parameters
    model_filter = "" if model_code == 'All' else f"AND product_model = '{model_code}'"
    
    # Define quality level logics by min and max power
    power_level_dict = {'Bronze': (100, 50), 'Gold': (1000, 200)}
    
    # Build the SQL query based on selected level and filters
    quality_query = f"""
        SELECT *
        FROM tmp_sdf
        WHERE product_max_power >= {power_level_dict[level][0]}
        AND product_min_power >= {power_level_dict[level][1]}
        AND product_tier = 1
        {model_filter}
    """
    
    return spark.sql(quality_query)


def model_based_serving(AllCustomers_subgeo, model = 'All'):
    
    """
    This function prepares a triple of serving sdfs (Bronze/Silver/Gold) for each model (A/B/C). For instance, 
    GoldB, SilverB, and BronzeB are built when model = B. See product_model_dict for all models and their code! 

    INPUTS:
    AllBSLs_subgeo: a spark df containing all customers at the selected subgeography level!
    model: the product model code! See the tech_dict dictionary! Default is All models meaning all models!

    OUTPUTS: 
    three sdfs for the desired model (e.g. GoldB, SilverB, and BronzeB)! 
    """
    
    #look up the model name for use in attribute name! 
    model_name = '' if model == 'All' else product_model_dict[model]
    ## BronzeCustomers: customers with Bronze products! 
    BronzeCustomers = AllCustomers_subgeo.join(AllCustomers.filter(quality_slicer(AllCustomers, 'Bronze', model_code)),
                                how = 'left_anti',
                                on = 'customer_id') \
                                .groupBy('GEOID') \
                                .agg(f.count('customer_id').alias('BronzeCustomers%s'%model_name))
    ## SilverCustomers: customers with silver products! 
    SilverCustomers = AllCustomers_subgeo.join(AllCustomers.filter(quality_slicer(AllCustomers, 'Bronze', model_code)),
                                how = 'left_semi',
                                on = 'customer_id') \
                                    .join(AllCustomers.filter(quality_slicer(AllCustomers, 'Gold', model_code)),
                                        how = 'left_anti',
                                        on = 'customer_id') \
                                    .groupBy('GEOID') \
                                    .agg(f.count('customer_id').alias('SilverCustomers%s'%tech_name))
    ## GoldCustomers
    GoldCustomers = AllCustomers_subgeo.join(AllCustomers.filter(quality_slicer(AllCustomers, 'Gold', tech_code)),
                                how = 'left_semi',
                                on = 'customer_id') \
                                .groupBy('GEOID') \
                                .agg(f.count('customer_id').alias('GoldCustomers%s'%tech_name))
    return [BronzeCustomers, SilverCustomers, GoldCustomers]   


def generate_output(sdf, subgeo, writeDelta =False, writeCSV = True, splitByState = False):
    
    """
    This function prepares a serving filter (BronzeCustomers, SilverCustomers, GoldCustomers) using any specific spark df!

    INPUTS:
    sdf: spark df to write as CSV!
    subgeo: the target subgeography from a list of keys in subgeo_dict! 
    writeCSV: if false it writes it as Delta tables!
    OUTPUTS: 
    returns the location of the saved CSV file for the selected subgeography!
    """
    
    file_location_national = outputCSV_location + '/CustomerProduct%s%s%s.csv'%(product_data_version, subgeo, state_fips)
    file_location_bystate = file_location_national[:-4] + '_ByState.csv'
    #get a list of integer count columns! 
    customer_cols = [c for c in sdf.columns if 'Customer' in c]
    #fill null values with zero and get ready for saving! 
    sdf_final = sdf.fillna(0, subset = bsl_cols).coalesce(1)

    if writeCSV:
        #now save the national file!
        sdf_final.write.mode("overwrite") \
                                .option("header", True) \
                                .option("delimiter", ",") \
                                .csv(file_location_national)
    if writeDelta:
        file_location_national = deltaPath + '/%s/CustomerProduct%s%s%s.delta'%(product_data_version, product_data_version,
                                                                                subgeo, state_fips)
        _ = convert_sdf_to_delta(sdf_final, file_location_national)

     
    if splitByState:
        #and save a parititioned by state version of it!
        sdf_final.write \
                    .option("header", True) \
                    .option("delimiter", ",") \
                    .partitionBy("StateName") \
                    .mode("overwrite") \
                    .csv(file_location_bystate)
    return file_location_national


if __name__ == '__main__':
    print('----Process Started----')
    start = time.time()
    #define below parameter to keep track of total processing time! 
    totalT = 0  
    
    AllCustomers = read_customer_data(customer_data_version, state_fips, generateDelta).cache()
    print('customer locations data version %s data was read as a spark dataframe!'%customer_data_version)
    
    productSDF = read_products_data(product_data_version, state_fips,  generateDelta).cache()
    print('products data version %s data was read as a spark dataframe!'%product_data_version)

    #loop through all subgeos and generate results!
    for subgeo in subgeo_dict.keys():
        
        print('Data processing for %s started!'%subgeo)
        totalT += process_subgeo(subgeo)

    end = time.time()  
    deltaT = end - start
    print('----Process Ended----')
    print('----The sub-geography processing for products %s was %s minutes!'%(
        product_data_version, round(totalT/60, 1)))
    print('----The entire products %s process was accomplished in %s minutes!'%(
        product_data_version, round(deltaT/60, 1)))




