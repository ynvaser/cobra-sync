package systems.bdev.cobrasync.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import systems.bdev.cobrasync.persistence.CubeCobraS3FileManifestEntity;
import systems.bdev.cobrasync.persistence.CubeCobraS3FileManifestRepository;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CobraSyncService {
    public static final String BUCKET_NAME = "cubecobra";
    public static final String DROPBOX_CUBE_EXPORTS = "/cube_exports/";

    private final S3FileDownloader s3FileDownloader;
    private final DropboxService dropboxService;
    private final CubeCobraS3FileManifestRepository cubeCobraS3FileManifestRepository;

    @Value("${forceUpdate}")
    private boolean forceUpdate;
    @Value("${forceUpload}")
    private boolean forceUpload;

    public void run() {
        Map<String, Date> keysAndUpdateDates = s3FileDownloader.fetchCubeExportFileNamesAndUpdateDates(BUCKET_NAME, "cube_exports/");
        if (forceUpdate || determineIfAnUpdateIsNeeded(keysAndUpdateDates)) {
            cubeCobraS3FileManifestRepository.deleteAllInBatch();
            dropboxService.deleteAll(DROPBOX_CUBE_EXPORTS);
            keysAndUpdateDates.forEach((key, date) -> {
                String jsonContents = s3FileDownloader.getObjectContents(key, BUCKET_NAME);
                Pair<String, String> folderAndFileName = getFolderAndFileName(key);
                dropboxService.upload(folderAndFileName.getFirst(), folderAndFileName.getSecond(), jsonContents);
                CubeCobraS3FileManifestEntity entity = new CubeCobraS3FileManifestEntity();
                entity.setId(key);
                entity.setUpdateDate(date);
                entity.setContents(jsonContents);
                cubeCobraS3FileManifestRepository.saveAndFlush(entity);
            });
        } else if (forceUpload) {
            cubeCobraS3FileManifestRepository.streamAllBy().forEach(entity -> {
                Pair<String, String> folderAndFileName = getFolderAndFileName(entity.getId());
                dropboxService.upload(folderAndFileName.getFirst(), folderAndFileName.getSecond(), entity.getContents());
            });
        } else {
            log.info("forceUpdate and forceUpload are false, and no data needs updating!");
        }
    }

    private boolean determineIfAnUpdateIsNeeded(Map<String, Date> keysAndUpdateDates) {
        Map<String, Date> persistedKeysAndUpdateDates = cubeCobraS3FileManifestRepository.findAll().stream().collect(Collectors.toMap(CubeCobraS3FileManifestEntity::getId, CubeCobraS3FileManifestEntity::getUpdateDate));
        for (Map.Entry<String, Date> entry : keysAndUpdateDates.entrySet()) {
            String key = entry.getKey();
            Date remoteUpdateDate = entry.getValue();
            if (!persistedKeysAndUpdateDates.containsKey(key) || persistedKeysAndUpdateDates.get(key).compareTo(remoteUpdateDate) != 0) {
                return true;
            } else {
                persistedKeysAndUpdateDates.remove(key);
            }
        }
        return !persistedKeysAndUpdateDates.isEmpty();
    }

    private Pair<String, String> getFolderAndFileName(String key) {
        int lastSlashIndex = key.lastIndexOf('/');
        String fileNameWithExtension = key.substring(lastSlashIndex).replaceAll("/", "");
        String folder = key.substring(0, lastSlashIndex);
        return Pair.of(folder, fileNameWithExtension);
    }
}
