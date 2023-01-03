package systems.bdev.cobrasync.services;

import com.dropbox.core.DbxException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DeleteArg;
import com.dropbox.core.v2.files.DeleteBatchLaunch;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class DropboxService {
    private static final String SLASH = "/";
    private final DbxClientV2 client;

    public int countUploadedFilesToFolder(String folder) {
        try {
            return getFolderContents(folder).size();
        } catch (DbxException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void upload(String folder, String fileNameWithExtension, String jsonContents) {
        upload(folder, fileNameWithExtension, jsonContents, false);
    }

    public void upload(String folder, String fileNameWithExtension, String jsonContents, boolean resent) {
        byte[] contents = compress(fileNameWithExtension, jsonContents);
        String fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(contents)) {
            FileMetadata metadata = client.files().uploadBuilder(SLASH + folder + SLASH + fileName + ".7z").uploadAndFinish(inputStream);
            log.info("Upload done: {}", metadata);
        } catch (RateLimitException ex) {
            try {
                if (!resent) {
                    log.info("Too many requests, waiting a bit...");
                    Thread.sleep(10000);
                    upload(folder, fileNameWithExtension, jsonContents, true);
                } else {
                    log.error(ex.getMessage());
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
        } catch (IOException | DbxException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void deleteAll(String folder) {
        log.info("Deleting everything on Dropbox under {}", folder);
        try {
            List<Metadata> entries = getFolderContents(folder);
            List<DeleteArg> filesToDelete = entries.stream().map(entry -> new DeleteArg(entry.getPathLower())).collect(Collectors.toList());
            if (filesToDelete.size() > 0) {
                DeleteBatchLaunch deleteBatchLaunch = client.files().deleteBatch(filesToDelete);
                while (!client.files().deleteBatchCheck(deleteBatchLaunch.getAsyncJobIdValue()).isComplete()) {
                    log.info("Dropbox delete batch job not completed yet, sleeping for 15 seconds...");
                    Thread.sleep(15000);
                }
            }
        } catch (DbxException | InterruptedException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private List<Metadata> getFolderContents(String folder) throws DbxException {
        ListFolderResult listFolderResult = client.files().listFolder(folder);
        List<Metadata> entries = listFolderResult.getEntries();
        return entries;
    }

    private byte[] compress(String fileNameWithExtension, String jsonContents) {
        try {
            byte[] data = jsonContents.getBytes();
            SeekableInMemoryByteChannel channel = new SeekableInMemoryByteChannel(new byte[1024]);
            SevenZOutputFile sevenZOutput = new SevenZOutputFile(channel);

            // Add the data to the 7z file
            SevenZArchiveEntry sevenZArchiveEntry = new SevenZArchiveEntry();
            sevenZArchiveEntry.setName(fileNameWithExtension);
            sevenZOutput.putArchiveEntry(sevenZArchiveEntry);
            sevenZOutput.write(data);
            sevenZOutput.closeArchiveEntry();

            // Close the output file
            sevenZOutput.close();

            // Get the compressed data as a byte array
            return channel.array();
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
