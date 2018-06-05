> <img src="src/banner.png" alt="CTGrazer" height="20%" align="middle"/>

# CTGrazer #
> **CTGrazer** is code you can use to create an *AWS Lambda* Function that will collect all of your *AWS CloudTrail* logs and efficiently send them to your *Splunk HEC (HTTP Event Collector)* server.

> #### Why?
> Using **CTGrazer** to port your AWS CloudTrail logs into Splunk has many advantages
> * **Speed** *CloudTrail* logs are processed as soon as they become available
> * **Security** All data is **encrypted** in transit and it does not rely on AWS IAM Access Keys
> * **Scalable** CTGrazer will automatically **scale up** and down according to your needs
> * **Reliable** If CTGrazer can't get your logs to their destination, it will **automatically retry** until it can
> * **Cost Effectiveness** Pulling in **400K objects** a month will cost you about the same as a **cheeseburger!**

> #### Pre-requisites
>
>| **AWS**       | **Splunk**     | **Python**     |
>| :------------ | :------------- | :------------- |
>| **IAM Role Permissions:** <br/> Invoke *Lambda* and fetch *S3 objects* | **HTTP Event Collector (HEC):** <br/> *Endpoint* and *Authentication Token* | Requests Module |

> #### Splunk Cloud | HEC VPC Configurations
> <p>If you are a <b>Splunk Cloud Customer</b> and plan to use splunk indexers as HEC endpoint, you should NOT set any VPC settings for Lambda to be able to stream data to your indexers. Only setting required here is to open inbound SG's on Splunk Cloud to allow this traffic. If no VPC Settings are enabled, Lambda uses it's own endpoint which has internet access by default to connect to resources outside of your own VPC.
</p>

> #### Configure, build and install
> ##### 1. Project Structure
> 
> <img src="src/ctgrazer-folder-structure.png" alt="CTGrazer Project Structure"/>

> ##### 2. Download Requests Module 
> CTGrazer uses Requests Module to perform HTTP calls. Download python Requests module and copy them under /lib folder.
> ##### 3. Config.ini
> [REQUIRED PARAMETERS]
> 
> |**Parameter**|**Description**    |
> |:------------|:------------------|
> |**aws_s3_bucket_name**|Specify an AWS S3 bucket name to collect the CloudTrail logs|
> |**aws_s3_bucket_prefix**|Specify the S3 subfolder location where the CloudTrail file object(s) are location|
> |**splunk_hec_endpoint**|Specify splunk HTTP Event Collector endpoint of format https://[HEC_HOST]:[PORT]/services/collector/event|
> |**splunk_hec_key**|Specify splunk HTTP Event Collector Key|
> |**splunk_source_type**|Specify a value for the splunk sourcetype|
> 
> [OPTIONAL PARAMETERS]
> 
> |**Parameter**|**Description**    |
> |:------------|:------------------|
> |**batch_thread_size**|Specify the number threads to be used for event batch|
> |**retry_sleep_time**|Time (in seconds) to sleep. Used when event is triggered but the object isn't there yet. Sleeps for the time specified and retries to get the object from S3|
> |**minutes_to_process**|Time (in minutes) to process events that are older than the below specified minutes|
> |**log_destination**|Time (in minutes) to process events that are older than the below specified minutes|
> |**log_message_prefix**|Specify log message format prefix.|
> |**debug**|Turn ON or OFF debugging|
> |**splunk_debug_sourcetype**|Splunk sourcetype to use when logging debug messages|
> 
> ##### 4. Deployment Package
> * Create a zip file to be uploaded as a AWS Lambda Function. You can use Ant builds to achieve this in Jenkins.
> * Upload Zip file as a Lambda Function
> * Configure S3 Put Trigger for the Cloudtrail Bucket - Event Trigger
> * Configure CloudWatch Event Rule (Eg: 30 min) - Scheduled Trigger
> * Configure applicable VPC, Security Group, Role Settings
> * Set Memory and Timeout limits (256MB , 5 mins)

