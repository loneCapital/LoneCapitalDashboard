### Pre-requisite 
1. Install AWS CLI
 - Please refer to this [AWS official Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

2. Yahoo Finance
 - `pip install yahoo-finance`

3. Other dependencies 
 - `Pandas`
 - `talib`
 - `boto3`
 - `botocore`

### Code usage
1. Create table
 - Run create table scripts in `/dynamoDBScripts/`, currently the endpoint is set to us-west-2 for all tables

2. Fill in data
 - Run fill data scripts in `/scripts/` currently the endpoint is set to us-west-2 for all tables. The endpoints need to be the same as where you created tables.