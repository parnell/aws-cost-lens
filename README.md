# AWS Cost Lens

A tool for analyzing and visualizing AWS costs by service and usage type with rich formatting.

## Installation

```bash
pip install aws-cost-lens
```

## Usage

After installation, you can run the tool using the `aws-cost-lens` command:

```bash
# Show simple cost breakdown for all services
aws-cost-lens

# Show detailed breakdown for a specific service
aws-cost-lens --service cloudwatch --detailed

# Show costs for a specific date range
aws-cost-lens --start-date 2023-01-01 --end-date 2023-12-31

# List all available AWS services
aws-cost-lens --list-services

# Show top 10 most expensive services
aws-cost-lens --top 10

# Use a named AWS CLI profile (needs ce:GetCostAndUsage)
aws-cost-lens --profile billing-admin
```

### Common Options

- `--service`: Filter by specific AWS service (e.g., `cloudwatch`, `s3`, `ec2`)
- `--start-date`: Start date (YYYY-MM-DD), defaults to 6 months ago
- `--end-date`: End date (YYYY-MM-DD), defaults to today
- `--detailed`: Show detailed breakdown by SERVICE and USAGE_TYPE
- `--region`: Include region breakdown in detailed analysis
- `--profile`: AWS named profile (same as `aws --profile`)
- `--top N`: Show only top N services/usage types
- `--granularity`: Time granularity (DAILY, MONTHLY, HOURLY)
- `--show-all`: Show all items including those with zero costs
- `--version`: Show version information

## AWS Credentials

AWS Cost Lens uses your AWS credentials from the environment. Make sure you have:

1. AWS CLI configured (`aws configure`)
2. Or environment variables set (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.)
3. Or `--profile NAME` to use a named profile from `~/.aws/config`
4. Appropriate IAM permissions:
   - Cost Explorer: `ce:GetCostAndUsage`
   - Amazon Credits inventory (optional): `billing:GetCredits` in `us-east-1`. The account must also have [IAM access to Billing](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/control-access-billing.html) enabled. Without this, the report still runs; the Amazon Credits section is skipped.

## Features

- Rich terminal UI with formatted tables and progress bars
- Detailed cost breakdown by service, usage type, and region
- Identify top cost contributors in your AWS account
- Amazon Credits inventory (remaining / estimated balance, expiration, applicable services) from Billing `GetCredits`, alongside Cost Explorer credits already applied to usage
- Get cost reduction tips for specific services
- Filter by service name with smart alias matching
- Customizable view options (simple/detailed, time range, etc.)