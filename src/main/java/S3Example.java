import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Scanner;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * This example code does the following 1) Creates 3 local files namely
 * aws_test1, aws_test2, aws_test3. 2) Uploads these files to s3. 3) List your
 * aws bucket to see these files. 4) Read and prints the aws_test1 file from s3.
 * 5) Deletes the files from s3.
 *
 */
public class S3Example {
  // Enter your s3 bucket name you want add/list/read/remove files from.
  private static String S3_BUCKET_NAME = "** ENTER YOUR BUCKET NAME ***";

  public static void main(String[] args) throws IOException {
    /*****************************************/
    /*** Create 3 test files in local disk ***/
    /*****************************************/
    File file1 = new File("aws_test1");
    File file2 = new File("aws_test2");
    File file3 = new File("aws_test3");
    Writer writer = new BufferedWriter(new FileWriter(file1));
    writer.write("test1");
    writer.close();
    writer = new BufferedWriter(new FileWriter(file2));
    writer.write("test2");
    writer.close();
    writer = new BufferedWriter(new FileWriter(file3));
    writer.write("test3");
    writer.close();

    Scanner keyboard = new Scanner(System.in);
    System.out.println("3 test files are created in " + file1.getParent()
        + ". Check the files and then press enter to continue.");

    keyboard.nextLine();

    /************************/
    /*** Create S3 Client ***/
    /************************/
    System.out.println("Creating S3 client.");
    // AmazonS3 object is used to interact with S3 via Java.
    AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());

    /***************************/
    /*** Put the files to S3 ***/
    /***************************/
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadObjSingleOpJava.html

    System.out.println("Putting files to s3.");
    s3client.putObject(new PutObjectRequest(S3_BUCKET_NAME, file1.getName(),
        file1));
    s3client.putObject(new PutObjectRequest(S3_BUCKET_NAME, file2.getName(),
        file1));
    s3client.putObject(new PutObjectRequest(S3_BUCKET_NAME, file3.getName(),
        file1));

    System.out
        .println("3 files are uploaded to s3. Check the files and then press enter to continue.");
    keyboard.nextLine();

    /************************/
    /*** List files in S3 ***/
    /************************/
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html

    System.out.println("Listing files in s3");
    // List all the files whose name starts with aws_test.
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(S3_BUCKET_NAME).withPrefix("aws_test");

    // This is a bit tricky.
    // S3 is a highly-scalable storage service. It means that one can have
    // thousands, even millions of files. Therefore, listObjectRequest does not
    // return all the
    // file names. It returns first 1000 file names, then you have to ask again.
    // Below is the standard
    // procedure of listing files. You list the first 1000, then check whether
    // the list is truncated (i.e., there are more than 1000 files) and then ask
    // again. In our simple case, this loop only iterates once since we have
    // only 3 files.
    ObjectListing objectListing;
    do {
      objectListing = s3client.listObjects(listObjectsRequest);
      for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        System.out.println("- " + objectSummary.getKey());
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());
    System.out.println();

    /**************************/
    /*** Read files from S3 ***/
    /**************************/
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/RetrievingObjectUsingJava.html

    // Read the first test file.
    System.out.println("Reading the file " + file1.getName());
    S3Object s3object = s3client.getObject(new GetObjectRequest(S3_BUCKET_NAME,
        file1.getName()));
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        s3object.getObjectContent()));
    System.out.println("Content of " + file1.getName());
    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
    System.out.println();

    /****************************/
    /*** Delete files from S3 ***/
    /****************************/
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/DeletingOneObjectUsingJava.html

    System.out.println("Deleting files from s3.");
    s3client.deleteObject(new DeleteObjectRequest(S3_BUCKET_NAME, file1
        .getName()));
    s3client.deleteObject(new DeleteObjectRequest(S3_BUCKET_NAME, file2
        .getName()));
    s3client.deleteObject(new DeleteObjectRequest(S3_BUCKET_NAME, file3
        .getName()));

    /**************************/
    /*** Delete Local Files ***/
    /**************************/
    System.out.println("Deleting local test files.");
    file1.delete();
    file2.delete();
    file3.delete();

    keyboard.close();
  }
}
