# Geographic Aggregation of Customer Product Data Analysis on Azure Databricks

## Overview

This project involves the analysis of customer product data using Azure Databricks. The goal is to gain insights into the quality of products experienced by customers across different Census geographic levels (e.g., State, County, Tract, Block Group, and Block) in the USA. I have performed the following tasks:

- Imported necessary libraries and set up the environment.
- Defined product models with their corresponding quality codes (A > B > C) and geographic levels.
- Specified the versions of the product and customer datasets.
- Created summary CSV tables of customer experiences in various Census gepgraphic levels

## High-Performance Big Data Analysis on Azure Databricks¶
Suppose you are a business owner with several product categories used by numerous customers across various locations within the USA. Assuming you have a table containing the geographic coordinates of your customers and another table summarizing all the services available for those customers, the following script provides insights into the product quality experienced by your customers at different Census geographic levels (i.e., State, County, Tract, Block Group, and Block). This template is designed in a modular form to be executed as a scheduled notebook in Azure Databricks, utilizing the high-performance Delta table format. In this scenario, it is assumed that the products are categorized as A, B, or C, with A being the highest quality and C being the lowest. The customer_id is a unique identifier representing individual customers across the USA, identified by their latitude and longitude, facilitating the association between the product and customer tables before aggregation and analysis. The product power is considered an indicator of its quality for each product (A, B, or C), and we set the following standards for products consumed by users: Bronze products have a maximum power greater than or equal to 100 and a minimum power greater than or equal to 50, Gold products have a maximum power greater than or equal to 1000 and a minimum power greater than or equal to 200, while Silver products fall between Bronze and Gold. The script has been tested on terabyte-sized data tables running on a 16-node cluster with 128 GB RAM on Azure Databricks, and the entire aggregation process for all Census geographic levels, including the time required to write the CSV output results to Azure Blob storage, was completed in approximately 45 minutes.

### Data Processing

The data processing involves the following steps:

1. Created a Spark dataFrame containing state names, abbreviations, and Census FIPS codes.
2. Read and processed customer location data, including filtering by state if specified.
3. Read and processed product data, including filtering by state if specified.
4. Aggregated the data at various geographic levels (State, County, Tract, Block Group, and Block).
5. Calculated the number of customers experiencing products of different quality levels (Bronze, Silver, Gold) for each product model (A, B, C).

### Output

The results of the data analysis are stored in CSV files in Azure blob storage. The CSV files are organized by Census geographic levels. These files contain information about the number of customers experiencing different product qualities at each geographic level.

The output data can be further analyzed and visualized to gain insights into the product quality experienced by customers across different regions in the USA.

Please note that this project has been designed to run on Azure Databricks with the option to generate Delta tables for improved performance.

For any inquiries or further information, please contact Babak K. Roodsari at bkasaeer@gmail.com.
