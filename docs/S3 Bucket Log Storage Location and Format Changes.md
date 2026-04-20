```
Article Number
4403
```
```
Category
All Versions, BMC, Illumio App for QRadar, Illumio App for
Splunk, Logging, PCE (Cloud), Traffic Events
Last Modified
2025-06-
```
# S3 BUCKET LOG STORAGE LOCATION AND FORMAT

# CHANGES: ILLUMIO CORE VS. UNIFIED CONSOLE

## SUMMARY

This article explores the differences in S3 bucket log storage locations between Illumio Core
and the Illumio Unified Console. It also covers the changes in traffic summaries highlighting
that traffic is now stored on different folders/paths based on policy decision.

## DESCRIPTION

On Illumio Core (scp1-scp16 clusters), S3 bucket paths remain/are:

```
illumio/auditable_events/
illumio/summaries/
```
On Unified Console clusters, S3 bucket new paths are:

```
<pce_fqdn>/org_id=<org_id>/auditable/
<pce_fqdn>/org_id=<org_id>/summaries/pd=0/
<pce_fqdn>/org_id=<org_id>/summaries/pd=1/
<pce_fqdn>/org_id=<org_id>/summaries/pd=2/
<pce_fqdn>/org_id=<org_id>/summaries/pd=3/
```
Example:

```
scp3.illum.io/org_id=123456/summaries/pd=1/
```
This path will only be pulling Traffic Summaries with Policy Decision: Potentially Blocked

##  LEE, CHIA HAO 


```
pd=0 ‒ Allowed traffic
pd=1 ‒ Potentially blocked. Allowed traffic which will be blocked after enforcement
pd=2 ‒ Blocked traffic
pd=3 ‒ Unknown (Traffic from CloudSecure)
```
For additional information on traffic summaries format please refer to the following KB
article: Illumio Traffic Format

**Implications**

```
SIEM integrations and other data retrieval mechanisms relying on the S3 bucket will be
impacted by these changes. To ensure seamless data access, new prefixes must be
configured to pull data from the correct location.
Additionally, traffic summaries are now stored based on policy decisions, introducing a
more structured approach to data organization. The advantage of this change is that
traffic can now be retrieved using filters based on the policy decisions reported by the
VEN.
As for SQS S3 polling, the impact may vary. Whether it is affected will depend on how
intelligently the polling mechanism can interpret different log types and folder
structures.
```
## DETAILS

The following screenshots show the different configurations for the Key Prefix on the Splunk
Data Inputs to ensure TA-AWS is pulling from the correct location and successfully retrieve
the desired logs.

**AWS S3 Bucket Path Changes (Auditable Events)**

**BEFORE (Illumio Core only)
Clusters: scp1 to scp**


**AFTER (Illumio Unified Console)
Clusters: UC Clusters**

**AWS S3 Bucket Path Changes (Traffic Events)**

**BEFORE (Illumio Core only)
Clusters: scp1 to scp**


**AFTER (Illumio Unified Console)
Clusters: UC Clusters**

If there are problems with data not coming in please refer to the following KB Article:
Troubleshooting S3 bucket Data input on TA-AWS in Splunk


```
Not helpful Somewhat helpful
Very helpful
1 2 3 4
5
```
How would you rate this article? *

Please give us feedback about this article

```
CONTACT SUPPORT
Create a new case
Email Support at support@illumio.com
Call Support
```
© 2026 Illumio 920 De Guigne Drive, Sunnyvale, CA 94085

## FEEDBACK

```
Submit »
```
```
About Support EULA Privacy Policy Sub-Processor Notifications 
```

