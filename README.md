# Customer Product Data Analysis on Azure Databricks

## Overview

This project involves the analysis of customer product data using Azure Databricks. The goal is to gain insights into the quality of products experienced by customers across different Census geographic levels (e.g., State, County, Tract, Block Group, and Block) in the USA. I have performed the following tasks:

- Imported necessary libraries and set up the environment.
- Defined product models with their corresponding quality codes (A > B > C) and geographic levels.
- Specified the versions of the product and customer datasets.
- Created summary CSV tables of customer experiences in various Census gepgraphic levels 

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
