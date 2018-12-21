package com.tencent.demo.dongxuny.trigger;

import java.io.InputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.SdkHttpUtils;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.region.Region;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3;
    private final COSClient cos;
    
    private final String COSSecretId = "AKIDkS4Ys0BjNp6C7ttyLfr2AA2mPvHS2GuO";
    private final String COSSecretKey = "GRVbmqkXBOQHyVs46ygHsrjR0Unz1sY4";
    private final String COSBucketName = "dongxuny-1252246555";
    private final String COSRegion = "ap-chengdu";

    public LambdaFunctionHandler() {
    	// Initialize S3 client, the bucket we want to access is public read
    	// Skip AWS credential
    	s3 = AmazonS3ClientBuilder.standard().build();
    	
    	// Initialize COS client
        COSCredentials cred = new BasicCOSCredentials(COSSecretId, COSSecretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(COSRegion));
        cos = new COSClient(cred, clientConfig);
    }
    
    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);

        //1: Retrieve Bucket and Key from event
        String s3Bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String s3Key = event.getRecords().get(0).getS3().getObject().getUrlDecodedKey();
        
        try {
            // 2: Get input stream with S3Client
            // Fail function if read failed
            //
            // We do not want to download full S3 object into somewhere in Lambda function located.
            // 
            // Instead, we will just extract input stream object and feed it into COS client
            // and lest COSClient and S3Client do the rest of pull and push stuff.
            S3Object s3Response = s3.getObject(new GetObjectRequest(s3Bucket, s3Key));
            InputStream input = s3Response.getObjectContent();            
            
            // 3: Start put to COS
            // Please clone object metadata as needed
            // In this demo, we just clone the content length which is medatory for COS
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(s3Response.getObjectMetadata().getContentLength());
            PutObjectResult cosResponse = cos.putObject(new PutObjectRequest(COSBucketName, s3Key, input, meta));
            context.getLogger().log(String.format("Put to COS with Bucket:%s, Key:%s, RequestId:%s", COSBucketName, s3Key, cosResponse.getRequestId()));
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error GET/PUT object, S3Bucket:%s, S3Key:%s, COSBucket:%s, COSKey:%s", s3Bucket, s3Key, COSBucketName, s3Key));
            throw e;
        }
    }
}