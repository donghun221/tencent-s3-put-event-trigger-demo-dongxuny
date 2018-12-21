# Simple demo example for S3->Lambda->COS written in JAVA

# How to run it?
- Git clone to local
- Modify LambdaFunctionHandler.java, user need to modify AWS accessId, SecretKey, Bucket etc and Tencent COS secretId, secretKey, Bucket, Region etc.
- Run [mvn clean]
- Run [mvn package]
- You will get new trigger-1.0.0.jar file under target folder
- Upload this jar file to AWS Lambda with Java environment selection in AWS Lambda

