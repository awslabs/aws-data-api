#!/bin/bash

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
  aws s3 cp chalicelib/glue_export_dynamo_table.py s3://awslabs-code-$r/AwsDataAPI/glue_export_dynamo_table.py --acl public-read --region $r;
done
