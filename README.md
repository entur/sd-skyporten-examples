# sd-skyporten-examples
Examples for how to share data with skyporten (maskinporten)


## Troubleshooting


### pandas_from_share() HTTPError: 400 Client Error:

For newer (ca. March 2014) Unity Catalog versions, this error could occur when reading from a share:

``````bash
HTTPError: 400 Client Error: Bad Request for url: https://europe-west3.gcp.databricks.com/api/2.0/delta-sharing/metastores/.../shares/foo/schemas/bar/tables/lee/query
 Response from server: 
 { 'error_code': 'BAD_REQUEST',
  'message': '\n'
             'Table property\n'
             'delta.enableDeletionVectors\n'
             'is found in table version: 2.\n'
             'Here are a couple options to proceed:\n'
             ' 1. Use DBR version 14.1(14.2 for CDF and streaming) or higher '
             'or delta-sharing-spark with version 3.1 or higher and set option '
             '("responseFormat", "delta") to query the table.\n'
             ' 2. Contact your provider to ensure the table is shared with '
             'full history.\n'
             '[Trace Id: 7ca11da50948c3ff9f0ce0045ec2278d]'}
``````
