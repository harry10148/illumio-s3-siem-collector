#### Article Number

#### 3216

#### Category

#### Illumio App for Splunk, Logging, Traffic Events

#### Last Modified

#### 2025-07-

# FLOW LOGS AND AUDITABLE EVENT LOGS FOR

# ILLUMIO SAAS CORE PCE

## SUMMARY

### Learn how Illumio delivers VEN flow logs and auditable event logs to Illumio SaaS Core PCE

### customers, enabling them to raise alerts on blocked traffic, audit administrative events, and

### perform long-term traffic analytics.

## DESCRIPTION

### Learn how Illumio delivers VEN flow logs and auditable event logs to Illumio SaaS Core PCE

### customers, enabling them to raise alerts on blocked traffic, audit administrative events, and

### perform long-term traffic analytics.

### How do we send the data?

### What data is sent?

### How is this implemented?

### What does Illumio need from you?

### Limitations

### How do we send the data?

### Our current implementation is to send the data to a customer owned AWS S3 bucket. This

### approach offers several benefits:

### Cost effective

### Highly elastic, allowing for buffering of large amounts of data

### Well-run and durable, thanks to AWS's reliable infrastructure

### Easy to access

##  LEE, CHIA HAO 


### Splunk is a popular log aggregator among Illumio customers and has built-in support for

### pulling logs from an S3 bucket. It's only a few clicks to install the official AWS integration,

### and straightforward to point it at logs. See How to configure Splunk Add-on for AWS S3 (KB

### 3502) for additional information.

### Other log aggregator systems should work as long as they have AWS pull methods/data

### inputs integration. The data format is syslog JSON. Contact Illumio Technical Support for

### further discussions on how to get your preferred log aggregation system supported.

### What data is sent?

### Illumio sends the following data:

### Accepted traffic

### Potentially blocked traffic

### Blocked traffic

### Auditable events

### Service logging is not included, nor is server status logging.

### How is this implemented?

### To send data, a customer needs to:

### 1. Create an AWS S3 bucket

### 2. Create an AWS IAM Role, restricted to a provided Illumio AWS account:

### Allowing the following permissions to the above bucket

### List bucket

### List bucket versions

### Allowing the following permissions to the objects in the above bucket

### Get objects

### Put objects

### Two AWS CloudFormation templates are provided below. One for legacy SCP1-17 clusters,

### and one for Unified Console clusters.

### Template for SCP1-17 clusters:


##### {

"AWSTemplateFormatVersion": "2010-09-09",
"Description": "Flow log bucket",
"Parameters": {
"Bucketname": {
"Type": "String"
},
"Externalid": {
"Type": "String",
"Default": "528298"
}
},
"Resources": {
"FlowbucketAwsS3Bucket": {
"Type": "AWS::S3::Bucket",
"Properties": {
"BucketName": {
"Ref": "Bucketname"
}
}
},
"IllumioFlowLogsAwsIamRole": {
"Type": "AWS::IAM::Role",
"Properties": {
"RoleName": "illumio-flow-logs",
"AssumeRolePolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Principal": {
"AWS": "857003445768"
},
"Action": [
"sts:AssumeRole"
],
"Condition": {
"StringEquals": {
"Sts:ExternalId": {
"Ref": "Externalid"
}
}
}
}
},
"Policies": [
{
"PolicyName": "can-see-bucket",
"PolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Sid": "illumioCanSeeBucket",
"Action": [
"s3:ListBucket",
"s3:ListBucketVersions"
],
"Resource": {
"Fn::Join": [
"",


##### [

```
"arn:aws:s3:::",
{
"Ref": "Bucketname"
} ] ] } } }
```
##### },

##### {

```
"PolicyName": "can-use-bucket",
"PolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Sid": "illumioCanPutAndGet",
"Action": [
"s3:PutObject",
"s3:GetObject"
],
"Resource": {
"Fn::Join": [
"",
[
"arn:aws:s3:::",
{
"Ref": "Bucketname"
},
"/*"
] ] } } } } ] } } } }
```
### Template for Unified Console clusters:


##### {

"AWSTemplateFormatVersion": "2010-09-09",
"Description": "Flow log bucket",
"Parameters": {
"Bucketname": {
"Type": "String"
},
"Externalid": {
"Type": "String",
"Default": "528298"
}
},
"Resources": {
"FlowbucketAwsS3Bucket": {
"Type": "AWS::S3::Bucket",
"Properties": {
"BucketName": {
"Ref": "Bucketname"
},
"VersioningConfiguration": {
"Status": "Enabled"
}
}
},
"FlowbucketPolicy": {
"Type": "AWS::S3::BucketPolicy",
"Properties": {
"Bucket": {
"Ref": "FlowbucketAwsS3Bucket"
},
"PolicyDocument": {
"Version": "2012-10-17",
"Statement": [
{
"Effect": "Allow",
"Action": [
"s3:ReplicateObject",
"s3:ReplicateDelete"
],
"Resource": {
"Fn::Join": [
"",
[
"arn:aws:s3:::",
{
"Ref": "Bucketname"
},
"/*"
]
]
},
"Principal": {
"AWS": "arn:aws:iam::857003445768:role/magneto/us-west-2/ilopce-s3-replication-role"
}
},
{
"Effect": "Allow",
"Action": [
"s3:PutBucketVersioning",


"s3:GetBucketVersioning"
],
"Resource": {
"Fn::Join": [
"",
[
"arn:aws:s3:::",
{
"Ref": "Bucketname"
}
]
]
},
"Principal": {
"AWS": "arn:aws:iam::857003445768:role/magneto/us-west-2/ilopce-s3-replication-role"
}
}
]
}
}
},
"IllumioFlowLogsAwsIamRole": {
"Type": "AWS::IAM::Role",
"Properties": {
"RoleName": "illumio-flow-logs",
"AssumeRolePolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Principal": {
"AWS": "857003445768"
},
"Action": [
"sts:AssumeRole"
],
"Condition": {
"StringEquals": {
"Sts:ExternalId": {
"Ref": "Externalid"
}
}
}
}
},
"Policies": [
{
"PolicyName": "can-see-bucket",
"PolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Sid": "illumioCanSeeBucket",
"Action": [
"s3:ListBucket",
"s3:ListBucketVersions"
],
"Resource": {
"Fn::Join": [
"",
[


```
"arn:aws:s3:::",
{
"Ref": "Bucketname"
} ] ] } } }
```
##### },

##### {

```
"PolicyName": "can-use-bucket",
"PolicyDocument": {
"Version": "2012-10-17",
"Statement": {
"Effect": "Allow",
"Sid": "illumioCanPutAndGet",
"Action": [
"s3:PutObject",
"s3:GetObject"
],
"Resource": {
"Fn::Join": [
"",
[
"arn:aws:s3:::",
{
"Ref": "Bucketname"
},
"/*"
] ] } } } } ] } } } }
```
## DETAILS

### Process to use the above CloudFormation Stack template:

### 1. Save the template to a JSON file (e.g., illumio-flow-logs-template.json)

### 2. From the AWS console, navigate to the CloudFormation Services page and select Stacks

### 3. Note the current region, as the S3 bucket will be created in this region

### 4. Select Create Stack, with new resources

### 5. Select template is ready, upload a template file

### 6. Upload the above created JSON file, select Next


### 7. Enter a Stack name (e.g., IllumioFlowLogsS3BucketAndRole)

### 8. Enter a unique Bucketname (must be unique across all S3 buckets in that region for all

### AWS customers)

### 9. Enter an External Id. Click the following link for more details on how and why to use an

### External Id

### 10. Keep the 'Configure stack options' defaults, select Next

### 11. Review and check acknowledgement, select Submit

### The provided bucket will be created, along with a role 'illumio-flow-logs' with the appropriate

### permissions to the provided Illumio AWS account. Similarly, create a role for your SIEM to

### read objects from the above bucket. For example, see official Splunk documentation.

### What does Illumio need from you?

### The following information will need to be provided to Illumio, to start sending flow data and

### events:

#### S3 Bucket Type Information Required

#### Customer

#### Provided S

#### Bucket

### The AWS S3 Bucket Name

### Your AWS Account ID

### The External ID

### The Role Name (default: illumio-flow-logs)

### The AWS Region where the bucket exists

#### Illumio Managed

#### S3 Bucket

#### Subscription

### Domain Name

### What data region to create the bucket in (EU, US, UK, etc.)

### Limitations

### Illumio does not support sending logs to an S3 bucket that is in an AWS region that is not

### "Active by default". For a complete list of AWS regions that are “Active by default”, consult

### this link from AWS:

### https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_region-

### endpoints.html

## FEEDBACK


#### Not helpful Somewhat helpful

#### Very helpful

#### 1 2 3 4

#### 5

### How would you rate this article? *

### Please give us feedback about this article

#### CONTACT SUPPORT

#### Create a new case

#### Email Support at support@illumio.com

#### Call Support

© 2026 Illumio 920 De Guigne Drive, Sunnyvale, CA 94085

### Submit »

```
About Support EULA Privacy Policy Sub-Processor Notifications 
```

