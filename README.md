# A Data Pipeline in Cloudera Data Platform (CDP)
## Use Case - Accelerate COVID-19 outreach programs using Data Services in CDP
As a healthcare provider / public health official, I want to respond equitably to the COVID-19 pandemic as quickly as possible, and serve all the communities that are adversely impacted in the state of California.  
I want to use health equity data reported by California Department of Public Health (CDPH) to **identify impacted members** and accelerate the launch of outreach programs.
## Design
**Collect** - Ingest data from https://data.chhs.ca.gov/dataset/covid-19-equity-metrics using NiFi.  
**Enrich** - Transform the dataset using Spark.  
**Report** - Gather insights from the dataset using Hive tables and Data Visualization.  
**Predict** - Connect to Hive tables and build Machine Learning (ML) models of your choice.

![Design - CDP Data Pipeline](/assets/Design_CDP_Data_Pipeline.png)

## Implementation
**Prerequisites:**  
- A modern browser such as Google Chrome and Firefox.
- An existing CDP environment and knowledge of its basic functions.  
- Add data/member_profile.csv to your storage bucket.
- Add data/covid-19-equity-metrics-data-dictionary.csv to your storage bucket. 

**Steps to create this data pipeline, are as follows:**  
> Please note that this data pipeline's documentation is in accordance with CDP Runtime Version 7.2.12. 
### Step #1 - Setup NiFi Flow
- Create or use a Data Hub Cluster with NiFi.  
Following Data Hub Cluster type can be used for this exercise - "7.2.12 - Flow Management Light Duty with Apache NiFi, Apache NiFi Registry".
- Go to NiFi user interface and import NiFi-CDPH.json flow.
- NiFi-CDPH.json uses PutS3Object processor to connect to an existing Amazon S3 bucket. **Please change the properties in this processor to use your own bucket.**
- If you don't use Amazon S3 storage, please replace PutS3Object processor with a processor of your own choice. Refer [NiFi docs](https://nifi.apache.org/docs.html) for details.  
For quick reference, here are the frequently used processors to write to a file system - 
  * PutAzureDataLakeStorage for Azure Data Lake Storage
  * PutGCSObject for Google Cloud Storage
  * PutHDFS for Hadoop Distributed File System (HDFS)
- Execute the flow and ensure InvokeHTTP processors are able to get [covid19case_rate_by_social_det.csv](https://data.chhs.ca.gov/dataset/f88f9d7f-635d-4334-9dac-4ce773afe4e5/resource/11fa525e-1c7b-4cf5-99e1-d4141ea590e4/download/covid19case_rate_by_social_det.csv) and [covid19demographicratecumulative.csv](https://data.chhs.ca.gov/dataset/f88f9d7f-635d-4334-9dac-4ce773afe4e5/resource/b500dae2-9e58-428e-b125-82c7e9b07abb/download/covid19demographicratecumulative.csv). Verify that these files are added to your storage bucket.
- Once you're satisfied with functions of this NiFi flow, download the flow definition.
- For reference, here's a picture of the flow in NiFi user interface -

  ![NiFi Flow](https://user-images.githubusercontent.com/2523891/160719482-1245dff8-7593-4b5b-890e-a74f25ba2332.png)
---
### Step #2 - Setup Cloudera DataFlow (CDF)
- Now that NiFi flow is ready, it's time to deploy it in your CDP environment. Go to CDF user interface, and ensure CDF service is enabled in your CDP environment.
- Import flow definition.
- Select imported flow, click on Deploy and follow the wizard to complete the deployment. Please note that Extra Small NiFi node size is enough for this data ingestion.
- After deployment is done, you would see the flow in Dashboard. You will be able to manage deployment of your flow in the Dashboard and perform functions like start/terminate flow, view flow, change runtime, view KPIs, view Alerts, etc.
- In Step #1, you've already executed the NiFi flow to add the source files to your storage bucket. So, you don't need to execute it again from CDF. But even if you do, it's going to just overwrite the files and not hurt anything.
---
### Step #3 - Setup Cloudera Data Engineering (CDE)
- Go to CDE user interface, and ensure CDE service is enabled in your CDP environment & a virtual cluster is available for use.
- Create a Spark job. In the wizard, upload enrich.py program and leave other options as default. **Please change the _fs_ variable in enrich.py program to point to your bucket**.
- Execute the job and monitor logs to ensure it's finished successfully. It takes approx. 4 minutes to finish.
- Following Hive tables are created by this job:
  - cdph.data_dictionary
  - cdph.covid_rate_by_soc_det
  - cdph.covid_demo_rate_cumulative
  - member.member_profile
  - member.target_mbrs_by_income
  - member.target_mbrs_by_age_group
---
### Step #4 - Setup Cloudera Data Warehouse (CDW)
- Go to CDW user interface. Ensure CDW service is activated in your CDP environment, and a Database Catalog & a Virtual Warehouse compute cluster are available for use.
- Open Hue editor and explore the Hive tables created by CDE job.
  ```sql
  -- Raw Data
  select * from cdph.data_dictionary a;
  select * from cdph.covid_rate_by_soc_det a;
  select * from cdph.covid_demo_rate_cumulative a;
  select * from member.member_profile a;
  ```
---
### Step #5 - Setup Cloudera Data Visualization (Data VIZ) Dashboard
- Go to Data VIZ user interface.
- Under the DATA tab, create the following Datasets:
  - **COVID Rate by Social Determinants**  
    Dataset Details:
    
    <img width="400" alt="Dataset - COVID Rate by Social Determinants" src="https://user-images.githubusercontent.com/2523891/160923268-a4521a2c-38c1-41d9-ae0f-7f1c8ac8ba09.png">

    Update Dimensions & Measures to look like below:
    
    <img width="1089" alt="Dataset - COVID Rate by Social Determinants - Fields" src="https://user-images.githubusercontent.com/2523891/160922294-31d3e399-62fe-47a7-a6ff-582e5fe0288d.png">
    
  - **COVID Demographic Rate Cumulative**  
    Dataset Details:
    
    <img width="400" alt="Dataset - COVID Demographic Rate Cumulative" src="https://user-images.githubusercontent.com/2523891/160923119-6b99c029-69a2-4186-95cd-54fbf238eea0.png">

    Update Dimensions & Measures to look like below:
    
    <img width="1096" alt="Dataset - COVID Demographic Rate Cumulative - Fields" src="https://user-images.githubusercontent.com/2523891/160921967-d2bb0612-47ca-4858-84c7-931e1b0bc26d.png">
   
- Once Datasets are available, go to VISUALS tab and create a new dashboard.
- Let's create first visual in the dashboard, to show **COVID-19 cases by income-groups**. Select Default Hive VW and COVID Rate by Social Determinants from drop down menus, and create a new visual. Set the following parameters - 
  - Visual Type - Combo (Combined Bar/Line)
  - Dimension - priority_sort
  - Bar Measure - avg(case_rate_per_100k)
  - Tooltips - max(social_tier)
  - Filters - social_det in ('income')  
  <br>
  <img width="1434" alt="Visual 1" src="https://user-images.githubusercontent.com/2523891/160934018-1687220e-4dd7-4662-9d64-cb007dc88b8f.png">

- Let's create second visual in the dashboard, to show **COVID-19 related deaths by age-groups**. Select Default Hive VW and COVID Demographic Rate Cumulative from drop down menus, and create a new visual. Set the following parameters - 
  - Visual Type - Lines
  - X Axis - demographic_set_category. Go to Field Properties, and select "Ascending" under "Order and Top K".
  - Y Axis - avg(metric_value_per_100k)
  - Filters - 
    - demographic_set in ('age_gp4')
    - metric in ('deaths')
    - county in ('Alameda', 'Contra Costa', 'Los Angeles', 'San Diego', 'San Francisco', 'Ventura'). To see data for all counties in California, USA, remove this filter.   
  <br>
  <img width="1435" alt="Visual 2" src="https://user-images.githubusercontent.com/2523891/160934593-bce0f433-c854-42c5-ac5e-a370fd00036d.png">

- For reference, here's the complete dashboard:

  <img width="1412" alt="Dashboard" src="https://user-images.githubusercontent.com/2523891/160933858-6a6db82d-883d-4631-90a6-50cfb09e81c6.png">
---
### Step #6 - Identify impacted members in Hue editor
- As you can see in the visuals, **below $40K** is the most impacted income group in terms of COVID-19 cases, and **65+** is the most impacted age group in terms of COVID-19 related deaths. You can now use this information, to filter members that are in these categories.
- Open Hue editor and execute the following queries to get impacted members:
  ```sql
  select * from member.target_mbrs_by_income a where social_tier = 'below $40K';
  select * from member.target_mbrs_by_age_group a where demographic_set_category = '65+';
  ```
---
### Step #7 - View Hive tables in Cloudera Data Catalog
- Go to Data Catalog user interface. Select any Hive table created in this exercise and see its lineage, schema, audits, etc.
---
### Step #8 - Setup Cloudera Machine Learning (CML)
- Go to CML user interface. Under ML Workspaces menu item, provision a new workspace. While provisioning a new workspace, enable Advanced Options and check "Enable Public IP Address for Load Balancer". This could take ~45 minutes to finish.
- Once workspace is available, create a New Project. Under Initial Setup, Template tab is selected by default, that works for most users. But you also have options to start from scratch (Blank), use existing Applied Machine Leaning Prototypes (AMPs - see AMPs navigation menu item for details), use local files (Local Files) or Git repository (Git).
- Download covid_outreach.ipynb and upload it in your project.
- If multiple people are going to work on this project, add them as collaborators with the right role under Collaborators menu item.
- Once you have the project setup, start a New Session. Select JupyterLab in Editor dropdown and check Enable Spark.

  ![CML - Start Session](https://user-images.githubusercontent.com/2523891/162040113-7c086c16-090b-4543-bc8e-281bfcc4909e.png)
- In your session, select covid_outreach.ipynb notebook.

  ![CML - Notebook](https://user-images.githubusercontent.com/2523891/162041307-0cc29a2b-1ed0-43b4-8028-4fe220b6d39e.png)
- Execute the notebook and see data in Hive tables.
- Now, you're ready to play around with the datasets and build your ML models.
- Run Experiment under Experiments menu item when you have a draft model ready.
- When you're ready to deploy the model, go to Models menu item and select New Model.
- Once you're satisfied with the results of your model, create a New Job under Jobs menu item to setup arguments, schedule, notifications & so on.
- For reference, please see menu items highlighted in Blue box that are referred in prior bullet points.

  ![CML - Menu Items](https://user-images.githubusercontent.com/2523891/162044517-9b949487-4f88-4457-8aa2-d2f348aaf255.png)
---
