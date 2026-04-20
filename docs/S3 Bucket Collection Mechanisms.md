```
Article Number
4234
```
```
Category
All Versions, BMC, Data Loading, Illumio App for QRadar,
Illumio App for ServiceNow, Illumio App for Splunk, Logging,
PCE (Cloud)
Last Modified
2024-09-
```
# S3 BUCKET COLLECTION MECHANISMS: GENERIC S

# VERSUS SQS-BASED S

## SUMMARY

This Article explains the differences between pulling data from S3 bucket using generic S3 or
SQS-Based S3. It also explains the different requirements for each of the pulling
mechanisms.

##  LEE, CHIA HAO 


## DESCRIPTION

To configure a Generic S3 bucket into Splunk please refer to the following knowledge base
Article: 

https://support.illumio.com/knowledge-base/articles/How-to-configure-Splunk-Add-on-for-
AWS-S3.html

To configure an SQS-Based S3 bucket into Splunk please refer to the following knowledge
base Article: 


How would you rate this article? *

https://support.illumio.com/knowledge-base/articles/How-to-configure-SQS-Based-S3-
Bucket-Illumio-Data-Input-into-Splunk.html

For more information on troubleshooting S3 data input, please visit the following knowledge
base article:

https://support.illumio.com/knowledge-base/articles/Troubleshooting-S3-bucket-Illumio-
data-inputs-in-Splunk-data-suddenly-stops-flowing.html

## DETAILS

The key distinction between generic S3 and SQS-based S3 lies in their operational nature.
Generic S3 operates synchronously, requiring a polling interval that must be configured. In
contrast, SQS-based S3 is asynchronous, which significantly enhances efficiency and
optimizes data handling processes. This shift to an asynchronous model streamlines
operations, allowing for smoother workflows and improved responsiveness.

Using SQS (Simple Queue Service) with S3 (Simple Storage Service) offers several
advantages over standard S3 access:

1. **Decoupling** : SQS allows for decoupled architecture, meaning your data processing can be
    managed independently from data ingestion. This enhances system resilience and
    flexibility.
2. **Asynchronous Processing** : With SQS, you can process S3 events asynchronously, which
    improves the efficiency of data handling and allows your applications to continue
    functioning smoothly even under high loads.
3. **Load Management** : SQS helps manage traffic spikes by queuing requests, allowing you to
    control the rate of processing and prevent overloading downstream services.
4. **Error Handling** : SQS provides built-in error handling through dead-letter queues, enabling
    better management of failed message processing.
5. **Scalability** : SQS automatically scales to handle varying workloads, making it easier to
    adapt to changing demands without manual intervention.

Overall, integrating SQS with S3 enhances reliability, scalability, and efficiency in data
processing workflows.

## FEEDBACK


```
Not helpful Somewhat helpful
Very helpful
1 2 3 4
5
```
Please give us feedback about this article

```
CONTACT SUPPORT
Create a new case
Email Support at support@illumio.com
Call Support
```
© 2026 Illumio 920 De Guigne Drive, Sunnyvale, CA 94085

```
Submit »
```
```
About Support EULA Privacy Policy Sub-Processor Notifications 
```

