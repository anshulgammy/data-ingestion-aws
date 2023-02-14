# movie-data-ingestion-aws
Implementing Data Ingestion Use Case using Amazon Web Services

# Key : Value
--------------------
Owner : anshulg

Group : CSB_Batch6

### movie-data-producer-lambda-role has policy: s3-Cloudwatch-Lambda-GlueRole.
Link: https://us-east-1.console.aws.amazon.com/iamv2/home#/roles/details/s3-Cloudwatch-Lambda-GlueRole?section=permissions

### Wanted to create new policy `movie-data-producer-s3-policy` which was for PutObject in S3.
Error Note: You do not have the permission required to perform this operation. Ask your administrator to add permissions. Learn more
User: arn:aws:iam::116345898539:user/AnshulG is not authorized to perform: iam:CreatePolicy on resource: policy movie-data-producer-s3-policy because no identity-based policy allows the iam:CreatePolicy action.

Error Note: User: arn:aws:iam::116345898539:user/AnshulG is not authorized to perform: lambda:CreateFunction on resource: arn:aws:lambda:us-east-1:116345898539:function:movie-data-producer-lambda because no identity-based policy allows the lambda:CreateFunction action.
