# awsdatapipeline
Data Pipeline Using AWS Services

This Repository consists of following folders:

1. LambdaFunction : This has two lambda_function.py files. One which downloads zip file from https endpoint(external system), in this case https endpoint is pointing to an S3 bucket "awsdpsrc" where original zip file is present in "raw" folder. The function then unzips it. Unzipped file is then written to "unzipped" folder in S3 bucket "iatademoritu" and finally it archives the original file to "archive" folder.
Second lambda function converts unzipped .csv file to parquet file and writes it to same S3 bucket "iatademoritu" in "curated" folder with the partitions created on column "Country". This lambda function gets invoked as soon as there is a .csv file in "unzipped" folder.

2. DeploymentPackages : There are two deployment packages for the above two lambda functions which have lambda_function.py file along with installed libraries required for lambda functions to run such as pandas, pyarrow, requests etc. 

3. CloudFormation : This folder has .yaml file to deploy solution with necessary resources such as S3 bucket with notification configuration to trigger lambda function, IAM role, lambda functions(deployment packages), Glue Database, Glue Catalog Table to AWS. Please note: deployment packages for Lambda functions are hosted in S3 bucket "awsdpsrc" in "code" folder.
