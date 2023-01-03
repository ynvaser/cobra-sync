package systems.bdev.cobrasync.config;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.oauth.DbxRefreshResult;
import com.dropbox.core.v2.DbxClientV2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class DropboxConfig {

    @Bean
    public DbxClientV2 getClient(
            @Value("${dropbox.clientIdentifier}") String clientIdentifier,
            @Value("${dropbox.accessToken}") String accessToken,
            @Value("${dropbox.refreshToken}") String refreshToken,
            @Value("${dropbox.appKey}") String appKey,
            @Value("${dropbox.appSecret}") String appSecret
    ) throws DbxException {
        DbxRequestConfig config = DbxRequestConfig.newBuilder(clientIdentifier).build();
        DbxCredential dbxCredential = new DbxCredential(accessToken, -1L, refreshToken, appKey, appSecret);
        DbxClientV2 dbxClientV2 = new DbxClientV2(config, dbxCredential);
        DbxRefreshResult dbxRefreshResult = dbxClientV2.refreshAccessToken();
        log.info(dbxRefreshResult.toString());
        return dbxClientV2;
    }
}
