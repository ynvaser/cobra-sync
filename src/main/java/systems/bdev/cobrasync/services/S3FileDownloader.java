package systems.bdev.cobrasync.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@AllArgsConstructor
public class S3FileDownloader {
    private final AmazonS3 s3Client;

    public Map<String, Date> fetchCubeExportFileNamesAndUpdateDates(String bucketName, String prefix) {
        log.info("Listing files from S3 bucket {}...", bucketName);
        Map<String, Date> result = new HashMap<>();

        ListObjectsV2Result objectsInBucket = s3Client.listObjectsV2(bucketName, prefix);
        if (objectsInBucket.isTruncated()) {
            log.error("Too many objects in response, tool can't handle this!");
            System.exit(3);
        }
        List<S3ObjectSummary> objects = objectsInBucket.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            log.info("* " + os.getKey());
            result.put(os.getKey(), os.getLastModified());
        }
        return result;
    }

    public String getObjectContents(String keyName, String bucketName) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            S3Object actualObject = s3Client.getObject(bucketName, keyName);
            S3ObjectInputStream actualObjectContentInputStream = actualObject.getObjectContent();
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = actualObjectContentInputStream.read(read_buf)) > 0) {
                bos.write(read_buf, 0, read_len);
            }
            actualObjectContentInputStream.close();
        } catch (AmazonServiceException e) {
            log.error(e.getErrorMessage(), e);
            System.exit(1);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            System.exit(2);
        }
        return bos.toString();
    }
}
