# SIEM Custom Parsers for Illumio PCE

These XML parsers tell the SIEM how to extract structured fields from the
syslog-wrapped JSON messages produced by `illumio_s3_collector`.

## Files

| File | Matches | Event format recognizer string |
|---|---|---|
| IllumioPCE_Auditable.xml | Auditable events (PCE admin activity, VEN lifecycle) | `illumio-pce audit auditable` |
| IllumioPCE_Summaries.xml | Traffic summaries (pd=0..3) | `illumio-pce summary` |

## Install

1. SIEM GUI -> Admin -> Device Support -> Parsers
2. Click **New** -> upload the XML
3. Set **Enabled = Yes**
4. Click **Apply** to push to collectors

## Verify

After the collector starts forwarding events:

1. Admin -> Setup -> Reporting Device -> search "Illumio"
2. A new device `Illumio PCE` should appear
3. Analytics -> Event Types = `Illumio-PCE-Audit` or `Illumio-PCE-Flow`
4. Check fields like `srcIpAddr`, `destIpAddr`, `policyDecision` are populated

## Adding fields

The collector flattens nested JSON with `_` separator, so a path like
`created_by.agent.hostname` appears in the syslog message as
`created_by_agent_hostname`. To parse additional fields, add lines like:

    when:$_jsonBody contains "my_field"
    extract:"my_field":"(?<myField>[^\"]+)";
    setEventAttribute($customAttr, $myField);
