# A Data Pipeline in Cloudera Data Platform (CDP)
# WORK IN PROGRESS
## Use Case - Accelerate COVID-19 outreach programs using Data Services in CDP
As a healthcare provider / public health official, I want to respond equitably to the COVID-19 pandemic as quickly as possible, and serve all the communities that are adversely impacted in the state of California.  
I want to use health equity data reported by California Department of Public Health (CDPH) to **identify impacted members** and accelerate the launch of outreach programs.
## Design
**Collect** - Ingest data from https://data.chhs.ca.gov/dataset/covid-19-equity-metrics using NiFi.  
**Enrich** - Transform the dataset using Spark.  
**Report** - Gather insights from the dataset using Hive tables and Data Visualization.  

![CDP_Data_Services_Covid_Demo drawio](https://user-images.githubusercontent.com/2523891/160536124-6aab8eeb-0db2-4a61-b8ec-c287cfed881d.png)

## Implementation
**Prerequisites:**  
- A modern browser such as Google Chrome and Firefox.
- An existing CDP environment.  
- Add member_profile.csv to your storage bucket. TODO - add this file.

Steps to create this data pipeline, are as follows:  
> Please note that this data pipeline's documentation is in accordance with CDP Runtime Version 7.2.12.

**Step #1 - Setup NiFi Flow**
- Create or use a Data Hub Cluster with NiFi.  
Following Data Hub Cluster type was used in this exercise - "7.2.12 - Flow Management Light Duty with Apache NiFi, Apache NiFi Registry".
- Go to NiFi user interface and import NiFi-CDPH.json flow. Right click anywhere in the pane and select Upload Template to do this. TODO - add JSON file.
- NiFi-CDPH.json uses PutS3Object processor to connect to an existing Amazon S3 bucket. **Please change the properties in this processor to use your own bucket.**
- If you don't use Amazon S3 storage, please replace PutS3Object processor with a processor of your own choice. Refer [NiFi docs](https://nifi.apache.org/docs.html) for details.  
For quick reference, here are the frequently used processors to write to a file system - 
  * PutAzureDataLakeStorage for Azure Data Lake Storage
  * PutGCSObject for Google Cloud Storage
  * PutHDFS for Hadoop Distributed File System (HDFS)
- Execute the flow and ensure InvokeHTTP processors are able to get [covid19case_rate_by_social_det.csv](https://data.chhs.ca.gov/dataset/f88f9d7f-635d-4334-9dac-4ce773afe4e5/resource/11fa525e-1c7b-4cf5-99e1-d4141ea590e4/download/covid19case_rate_by_social_det.csv) and [covid19demographicratecumulative.csv](https://data.chhs.ca.gov/dataset/f88f9d7f-635d-4334-9dac-4ce773afe4e5/resource/b500dae2-9e58-428e-b125-82c7e9b07abb/download/covid19demographicratecumulative.csv). Verify that these files are added to your storage bucket.
- Once you're satisfied with functions of this NiFi flow, download the flow definition by right clicking anywhere in the pane and selecting "Download flow definition".
- For reference, here's a picture of the flow in NiFi user interface -
  ![NiFi Flow](https://user-images.githubusercontent.com/2523891/160719482-1245dff8-7593-4b5b-890e-a74f25ba2332.png)

**Step #2 - Setup Cloudera DataFlow (CDF)**
- Now that we have the NiFi flow ready, it's time to deploy it in your CDP environment. Go to CDF user interface, select Environments & ensure CDF service is enabled in your CDP environment.
- Select Catalog, and import the flow definition.
  ![CDF - Import Flow](https://user-images.githubusercontent.com/2523891/160722873-9cd85f05-f5a5-4fff-8f66-2aa879e15eb5.png)
- Select imported flow, click on Deploy and follow the wizard to complete the deployment. Please note that Extra Small NiFi node size is enough for this data ingestion.
  ![CDF - Flow Deploy](https://user-images.githubusercontent.com/2523891/160723115-46107191-991c-45ba-bf7d-9ca091078528.png)
- After deployment is done, you would see the flow in Dashboard. You will be able to manage deployment of your flow in the Dashboard and perform functions like start/stop flow, view flow, change runtime, view KPIs, view Alerts, etc.

More to come...
