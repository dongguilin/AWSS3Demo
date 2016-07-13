package com.guilin.aws.s3.demo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.UUID;

/**
 * Created by guilin1 on 16/3/23.
 */
public class CrudDemo {

    private static AmazonS3 s3client = null;

    @BeforeClass
    public static void beforeClass() {
        String filePath = CrudDemo.class.getClassLoader().getResource("").getPath() + File.separator + "credentials";
        s3client = new AmazonS3Client(new ProfileCredentialsProvider(filePath, "default"));
        s3client.setRegion(Region.getRegion(Regions.CN_NORTH_1));
    }

    @Test
    public void testCreateBucket() {
        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();

        try {
            if (!(s3client.doesBucketExist(bucketName))) {
                // Note that CreateBucketRequest does not specify region. So bucket is
                // created in the region specified in the client.
                s3client.createBucket(new CreateBucketRequest(
                        bucketName));
            }
            // Get location.
            String bucketLocation = s3client.getBucketLocation(new GetBucketLocationRequest(bucketName));
            System.out.println("bucket location = " + bucketLocation);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }


    @Test
    public void testListBucket() {
        for (Bucket bucket : s3client.listBuckets()) {
            System.out.println(bucket.getName() + " " + bucket.getOwner() + " " + bucket.getCreationDate());
        }
    }

    @Test
    public void testListObject() {
        String bucketName = "my-first-s3-bucket-73cedc5c-027f-4d53-a9c9-854c58c21615";

        ObjectListing objectListing = s3client.listObjects(new ListObjectsRequest().withBucketName(bucketName));
//        ObjectListing objectListing = s3client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix("My"));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                    "(size = " + objectSummary.getSize() + "bytes)");
        }
    }

    @Test
    public void testUploadFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("hello\n");
        writer.close();

        String bucketName = "my-first-s3-bucket-73cedc5c-027f-4d53-a9c9-854c58c21615";

        //重复上传会更新数据
//        s3client.putObject(new PutObjectRequest(bucketName, "MyObjectKey", file));

        //相对目录
        s3client.putObject(bucketName, "xls/网络设备自定义属性.xls", new File("/Users/guilin1/Downloads/网络设备自定义属性.xls"));

    }

    @Test
    public void testDownObject() throws IOException {
        String bucketName = "my-first-s3-bucket-73cedc5c-027f-4d53-a9c9-854c58c21615";
        String key = "xls/网络设备自定义属性.xls";

        S3Object object = s3client.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());

        if (object.getObjectMetadata().getContentType().equals("text/plain")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;

                System.out.println(line);
            }
        } else {
            FileUtils.copyInputStreamToFile(object.getObjectContent(), new File("/Users/guilin1/Downloads/t.xls"));
        }
    }

    @Test
    public void testDeleteObject() {
        String bucketName = "my-first-s3-bucket-73cedc5c-027f-4d53-a9c9-854c58c21615";
        s3client.deleteObject(bucketName, "网络设备自定义属性.xls");
    }

    @Test
    public void testDeleteBucket() {
        List<Bucket> bucketList = s3client.listBuckets();
        for (Bucket bucket : bucketList) {
            ObjectListing objectListing = s3client.listObjects(bucket.getName());
            if (CollectionUtils.isEmpty(objectListing.getObjectSummaries())) {
                System.out.println(bucket.getName());
                s3client.deleteBucket(bucket.getName());
            }
        }
    }


}
