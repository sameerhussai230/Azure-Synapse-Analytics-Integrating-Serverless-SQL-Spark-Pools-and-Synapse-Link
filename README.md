# Unified_Data_Processing_Framework_on_Azure_Synapse: Integrating Serverless SQL, Spark Pools, and Synapse Link

## Project Overview 

The project begins by sourcing data from the NYC Taxi website (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), which provides a diverse range of files crucial for analysis. The files are converted into CSV, JSON, TSV and Parquet formats to facilitate real data engineering tasks, containing detailed information about New York City taxi trips, including trip duration, zones, and payment types. To efficiently query this data, the project utilizes the Serverless SQL pool in Azure Synapse Analytics, leveraging the OpenRowSet function to directly query files stored in the raw folder of the Data Lake. Data pruning techniques are applied using the filename() function to optimize queries and improve performance.

To facilitate effective data processing and management, tables are created using the CETAS (Create Table As) statements for both the silver and gold layers. The silver layer undergoes transformation using the Spark Pool to enhance computing processes, particularly for complex computation and aggregation tasks. Additionally, user-defined stored procedures (USPs) are created to automate the table creation process, ensuring consistency and efficiency. Implemented parametrized pipelines streamlines workflow orchestration by facilitating dynamic folder paths and user-defined stored procedure (USP) names through a JSON configuration file.

The project extends its capabilities by seamlessly querying Cosmos DB data in Azure Synapse Analytics using Serverless SQL Pool and Spark Pool. This approach enhances operational efficiency by leveraging near real-time OLAP and OLTP analytics without the need for complex ETL processes. Finally, Azure Synapse is seamlessly connected with Power BI via SQL Endpoint, enabling the creation of insightful visualizations for analyzing demand patterns and payment types, enhancing the overall data analysis and reporting capabilities of the project.

![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/732e2dd2-5420-4ed5-989c-98ede929e275)

 ------
 
 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/c612723a-7256-4c6d-879d-93218dc44f80)


### Utilizing Serverless SQL Pool for Efficient Data Querying

To efficiently query this data, the project utilizes the Serverless SQL pool in Azure Synapse Analytics. Leveraging the OpenRowSet function, to directly queries files stored in the raw folder of the Data Lake, enabling efficient data retrieval and analysis.

![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/93410a28-2781-40a4-8079-c96bcbfdfa44)
 

### Implementing Data Pruning Techniques for Query Optimization

Data pruning techniques are applied using the filename() function to optimize queries and improve performance. This ensures that only relevant data is processed, leading to faster query execution and enhanced efficiency in data analysis.

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/e195e2f0-bf33-47f2-b434-d248bca174a8)



### Transforming Data Using Spark Pool for Enhanced Computing

The silver layer undergoes transformation with multiple join conditions and aggregations to generate tables in the gold layer, leveraging the Spark pool to enhance computing processes. This is especially advantageous for handling intricate computation and aggregation tasks, thereby enhancing the overall efficiency of data processing.
 
![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/7e082d1f-3563-4bd0-bb53-b2f11fa3467f)



### Automating Table Creation with User-Defined Stored Procedures (USPs)

User-defined stored procedures (USPs) are created to automate the table creation process. This ensures consistency and efficiency in managing data tables, reducing manual intervention and streamlining the data processing workflow.

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/58765135-be5f-47d8-a33b-47e757a1e733)

### Streamlining Workflow Orchestration with Parametrized Pipelines

Parametrized pipelines are implemented to streamline workflow orchestration. This allows for dynamic folder paths and USP names through a JSON configuration file, enhancing the flexibility and scalability of the data processing pipeline.

##### Trial Silver Pipeline for USP
![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/5a1fcd06-d86c-4ebc-b984-0d16e10aa448)

##### Dynamic Silver Pipeline Execution

Firstly, Script Activity is executed for getting all file names, then in For Each Activity we will use stored procedure on this file , then we will create view on top of all files of silver folder 

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/a718f49f-819b-4ff0-b205-ffa7ef006d15)


##### Json File for providing Folder path and USP in parameter value of main pipeline

![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/b5e2164b-8dec-49dd-ac0c-4beee8e68ae2)

##### Executing all Pipelines 
 
![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/4f272285-1a22-4898-84df-b1278f43f2ee)


### Connecting to Cosmos DB with Azure Synapse Link

Connecting to Cosmos DB with Azure Synapse Link for querying Cosmos DB data and integrating it seamlessly within Azure Synapse Analytics offers significant benefits. Azure Synapse Link eliminates the need for complex ETL processes or data replication, allowing for real-time access to operational data. With automatic data synchronization in just a few minutes, it streamlines the conversion of row data into columnar format data. This enables near real-time analytical insights, empowering organizations to make data-driven decisions swiftly and effectively.

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/1aeb006a-aeaf-4b46-9766-280c0f3584cc)


##### Querying Cosmos DB Data in Synapse with Serverless SQL Pool

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/a842dcf7-e156-42c6-9e42-f1b6a96ad78b)


##### Querying Cosmos DB Data in Synapse Using Spark Pool (OLAP and OLTP Versions)

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/7f639355-1eac-4679-94e7-5191c15fa616)




### Connecting Azure Synapse with Power BI for Data Visualization

Finally, Azure Synapse is seamlessly connected with Power BI via SQL Endpoint, enabling the creation of insightful visualizations for analyzing demand patterns and payment types. This enhances the overall data analysis and reporting capabilities of the project.


##### Taking SQL Endpoint of Servless SQL Pool from Synapse Workspace 
 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/5278b46c-4e83-4039-a1d2-7f25fb6d2555)

##### Using SQL Endpoint for connecting to Power BI.

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/52ba5618-dc79-4b02-af5b-09ab27a42956)

 ![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/6543116f-0411-4a65-bcf5-3bdcd085f14c)


### Results Visualizations

![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/73d5d31c-36dd-4b32-9fe4-86461a423479)

![image](https://github.com/sameerhussai230/Unified_Data_Processing_Framework_on_Azure_Synapse/assets/85198601/c08e6974-bfcd-4317-8866-4d233f5c0d8b)



 

 



