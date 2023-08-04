# FACTORED DATATHON - BIFROST ANALYTICS
<hr>

## CHALLENGE 
<p> Create an innovative data solution (web apps, chatbots, dashboards, 
model interfaces...) to empower businesses with insights from product 
reviews.
</br>
</br>
With an Amazon products review dataset you will embody a product 
company's data team and develop an end-to-end data solution, to 
generate actionable insights to inform stakeholders' decisions.
</br>
</br>
● What has the most significant impact on customer satisfaction?
</br>
● Can we infer customer segment based on their review patterns?
</br>
● It's possible to build a recommendation system based on 
recommendations?
</p>

<hr>

![Datathon Challenge](docs/img/datathon-challenge.png)

<hr>

### 1. DATA ARCHITECTURE

![Project Architecture](docs/img/datathon-architecture.png)

<hr>
<p> The approach that Bifrost Analytics used to provide insights from de Data Sources was creating a Lambda Architecture Solution, 
    because the source of Amazon Reviews and Amazon Metadata was from Batch and Streaming process. </p>

#### BATCH AND STREAMING
<p>
<b>BATCH:</b> We've developed a Python application for handling Batch data, specifically extracting data from Amazon-metadata and Amazon-reviews. This data originates from Azure Data Lake Storage. To automate this process seamlessly, we used the power of Astro CLI, a user-friendly tool from Astronomer that allows us to use Apache Airflow through the command line and then deploy in AWS with Astro Cloud and we send this data to an AWS S3 bucket.

<b>STREAMING:</b> Azure Event Hub is the Amazon streaming data source. A well-coordinated combination of Amazon SQS and AWS Lambda was used to provide seamless data extraction and processing. This dynamic extracts data from Azure Event Hub and orchestrates its arrival into AWS S3 Bucket efficiently.

To both Batch and Streaming, we established a Storage Integration to connect the S3 buckets to the robust Data Platform <b>Snowflake</b>. Within Snowflake, we created External Stages, Tasks, and Snowpipes to automate the ingestion of Amazon data into RAW tables. Additionally, we worked with dbt core (Data Build Tool) within Astro CLI and Soda to perform data transformations through the bronze, silver, and gold layers, while maintaining data quality throughout the entire process.

This approach enabled us to provide Data Scientist and Data Analyst with access to high-quality, readily available data to drive meaningful decisions, discoveries and valuable insights.
</p> 

### 2. DATA QUALITY

<p>

 With [Soda](https://www.soda.io/) we could be able to deliver trust data. During the process we discovered some inconsistences throught this wonderful tool.
##### 1. Percentage of Duplicate data in Amazon Metadata: 
 <p>
 Thought this warn check we created an incident and discovered that some data were duplicated since the origin, with the same data in the same file. It was possible to discover that 1,62% of amazon's metadata data was duplicated (almost 245k records) and have a treatment for this mitigating those duplicate data.
</p>

 ![Percentage of Duplicate Metadata](docs/img/percent_duplicate_metadata.png)

##### 2. Null Values in price column in Amazon Metadata: 
 <p>
 Thought this fail check we were able to see that almost 55% of Metadata values were without price and developed a strategy to step over this problem. Our Strategy was update the null prices with the average price of the product's category. So we transformed this useless column for almost 8.3 million of records into a rich source to provide some insights
</p>

 ![Null Values in Price Column Metadata](docs/img/null_price_value_metadata.png)

##### 3. Percentage of Duplicate data in Amazon Reviews: 
 <p>
 We were able to identify with this warn bellow that we had almost 1% of the total amazon reviews data duplicated, with a main why - Reviews that were spammed with like more than 1 equal review (same overall, same text, same user, same product) with one tiny difference the summary, so for 1 product we could have like 12 same reviews differing just the summary and it could spoil the final analysis.

</p>

 ![Percentage of Duplicate Reviews](docs/img/percent_duplicate_reviews.png)

### 3. PRODUCT 
(To be updated)
 - Consumer Recommendation
 - Seller Recommendation
 
### 4. TEAM 🇧🇷
 - Jessica Caroline Costa e Silva - Data Engineer | <b> [LinkedIn](https://www.linkedin.com/in/jessicaccostaesilva/) </b> | <b> [Github](https://github.com/jess197) </b> 
 - Luan José de Almeida Cardoso - Data Engineer | <b> [LinkedIn](https://www.linkedin.com/in/luanjosecar/) </b> | <b> [Github](https://github.com/luanjosecar) </b>
  - Brunno Kalyxton Sousa Ramos - Data Scientist | <b> [LinkedIn](https://www.linkedin.com/in/brunno-kalyxton-sousa-ramos-79a37817b/) </b> | <b> [Github](https://github.com/bksramos) </b>
 - Gabrielle Moura - Data Analyst |<b> [LinkedIn](https://www.linkedin.com/in/gabrielle-moura-a3a782156/) </b> | <b> [Github](https://github.com/gabymoura) </b>
    

### 5. TECH STACK  
<img src="./docs/img/azure.png" alt="azure" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/aws.png" alt="aws" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/python.png" alt="python" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/dbt.png" alt="dbt" style="vertical-align:top; margin:4px; height:40px; width:100px">
<img src="./docs/img/airflow.png" alt="airflow" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/docker.png" alt="docker" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/snowflake.png" alt="snowflake" style="vertical-align:top; margin:4px; height:40px; width:40px">
<img src="./docs/img/pandas.png" alt="pandas" style="vertical-align:top; margin:4px; height:40px; width:100px">
<img src="./docs/img/metabase.png" alt="metabase" style="vertical-align:top; margin:4px; height:40px; width:40px">
