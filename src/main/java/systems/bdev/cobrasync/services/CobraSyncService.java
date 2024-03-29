package systems.bdev.cobrasync.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import systems.bdev.cobrasync.persistence.CubeCobraS3FileManifestEntity;
import systems.bdev.cobrasync.persistence.CubeCobraS3FileManifestRepository;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
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
    private final JavaMailSender javaMailSender;

    @Value("${forceUpdate}")
    private boolean forceUpdate;
    @Value("${forceUpload}")
    private boolean forceUpload;
    @Value("${spring.mail.username}")
    private String emailAddressFrom;
    @Value("${notificationEmail}")
    private String emailAddressTo;

    public void run() {
        try {
            Map<String, Date> keysAndUpdateDates = fetchFiles();
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
                sendEmail("Cobra-Sync updated Dropbox with new data!", keysAndUpdateDates);
            } else if (forceUpload || dropboxService.countUploadedFilesToFolder(DROPBOX_CUBE_EXPORTS) != keysAndUpdateDates.size()) {
                cubeCobraS3FileManifestRepository.streamAllBy().forEach(entity -> {
                    Pair<String, String> folderAndFileName = getFolderAndFileName(entity.getId());
                    dropboxService.upload(folderAndFileName.getFirst(), folderAndFileName.getSecond(), entity.getContents());
                });
                sendEmail("Cobra-Sync force-uploaded existing data to Dropbox!", keysAndUpdateDates);
            } else {
                log.info("forceUpdate and forceUpload are false, and no data needs updating!");
                sendEmail("Cobra-Sync job ran and found nothing to do!", keysAndUpdateDates);
            }
        } catch (Exception e) {
            log.error("Cobra-sync needs to shut down with an error: {}", e.getMessage(), e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTraceAsString = sw.toString();
            sendEmail("Cobra-Sync failed with an error!", e.getMessage() + "\n" + stackTraceAsString);
        }
    }

    private Map<String, Date> fetchFiles() {
        Map<String, Date> stringDateMap = new HashMap<>();
        stringDateMap.putAll(s3FileDownloader.fetchCubeExportFileNamesAndUpdateDates(BUCKET_NAME, "cubes.json"));
        stringDateMap.putAll(s3FileDownloader.fetchCubeExportFileNamesAndUpdateDates(BUCKET_NAME, "indexToOracleMap.json"));
        stringDateMap.putAll(s3FileDownloader.fetchCubeExportFileNamesAndUpdateDates(BUCKET_NAME, "simpleCardDict.json"));
        return stringDateMap;
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
        if (lastSlashIndex == -1) {
            return Pair.of("cube_exports", key);
        }
        else {
            String fileNameWithExtension = key.substring(lastSlashIndex).replaceAll("/", "");
            String folder = key.substring(0, lastSlashIndex);
            return Pair.of(folder, fileNameWithExtension);
        }
    }

    public void sendEmail(String subject, Map<?, ?> map) {
        String mapAsString = map.entrySet().stream().map(Object::toString).collect(Collectors.joining("\n"));
        sendEmail(subject, mapAsString);
    }

    public void sendEmail(String subject, String text) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(emailAddressFrom);
        message.setTo(emailAddressTo);
        message.setSubject(subject);
        message.setText(text);
        javaMailSender.send(message);
    }
}
