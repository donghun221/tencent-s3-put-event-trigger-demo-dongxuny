package com.tencent.demo.dongxuny.trigger;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.Transfer;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferProgress;
import com.qcloud.cos.transfer.Upload;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	// Currently, the demo bucket is public read, so we did not define any accessId or secretKey
    private final AmazonS3 s3;
    private final COSClient cos;
    
    private final String COSSecretId = "";
    private final String COSSecretKey = "";
    private final String COSBucketName = "dongxuny-public-1252246555";
    private final String COSRegion = "ap-guangzhou";

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
        // Please use url decoded key
        // otherwise, there will be error thrown
        String s3Key = event.getRecords().get(0).getS3().getObject().getUrlDecodedKey();
        
        try {
            // 2: Get input stream with S3Client
            // Fail function if read failed
            //
            // We do not want to download full S3 object into somewhere in Lambda function located.
            // 
            // Instead, we will just extract input stream object and feed it into COS client
            // and let COSClient and S3Client do the rest of pull and push stuff.
            S3Object s3Response = s3.getObject(new GetObjectRequest(s3Bucket, s3Key));
            InputStream input = s3Response.getObjectContent();            
            
            // 3: Start put to COS
            // Please clone object metadata as needed
            // In this demo, we just clone the content length which is medatory for COS
            
            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
            TransferManager transferManager = new TransferManager(cos, threadPool);
            
            try {
                // 返回一个异步结果Upload, 可同步的调用waitForUploadResult等待upload结束, 成功返回UploadResult, 失败抛出异常.
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(s3Response.getObjectMetadata().getContentLength());
                
                long startTime = System.currentTimeMillis();
                
                Upload upload = transferManager.upload(new PutObjectRequest(COSBucketName, s3Key, input, meta));
                showTransferProgress(upload, context);
                UploadResult uploadResult = upload.waitForUploadResult();
                long endTime = System.currentTimeMillis();
                context.getLogger().log("Uplaod Duration: " + (endTime - startTime) / 1000);
                context.getLogger().log(String.format("Put to COS with Bucket:%s, Key:%s, RequestId:%s", COSBucketName, s3Key, uploadResult.getRequestId()));

            } catch (CosServiceException e) {
                e.printStackTrace();
            } catch (CosClientException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            transferManager.shutdownNow();
            cos.shutdown();

            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error GET/PUT object, S3Bucket:%s, S3Key:%s, COSBucket:%s, COSKey:%s", s3Bucket, s3Key, COSBucketName, s3Key));
            throw e;
        }
    }
    
    private void showTransferProgress(Transfer transfer, Context context) {
        long prevBytesTransferred = 0;
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            TransferProgress progress = transfer.getProgress();
            long so_far = progress.getBytesTransferred();
            long total = progress.getTotalBytesToTransfer();
            double pct = progress.getPercentTransferred();
            context.getLogger().log(String.format("bytes_transferred:%d, bytes_to_transfer:%d, progress:%f, throughput:%d MB/s", so_far, total, pct, (so_far-prevBytesTransferred)/1024/1024));

            prevBytesTransferred = so_far;
        } while (transfer.isDone() == false);
    }
}