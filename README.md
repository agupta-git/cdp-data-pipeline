# A Data Pipeline in Cloudera Data Platform (CDP)
## Use Case - Accelerate COVID-19 outreach programs using Data Services in CDP
As a healthcare provider / public health official, I want to respond equitably to the COVID-19 pandemic as quickly as possible, and serve all the communities that are adversely impacted in the state of California.  
I want to use health equity data reported by California Department of Public Health (CDPH) to **identify impacted members** and accelerate the launch of outreach programs.
## Design
**Collect** - Ingest data from https://data.chhs.ca.gov/dataset/covid-19-equity-metrics using NiFi.  
**Enrich** - Transform the dataset using Spark.  
**Report** - Gather insights from the dataset using Hive tables and Data Visualization.  

![CDP_Data_Services_Covid_Demo drawio](https://user-images.githubusercontent.com/2523891/160536124-6aab8eeb-0db2-4a61-b8ec-c287cfed881d.png)
