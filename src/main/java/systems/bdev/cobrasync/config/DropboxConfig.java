package systems.bdev.cobrasync.config;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DropboxConfig {
    @Bean
    public DbxClientV2 getClient(@Value("${dropbox.clientIdentifier}") String clientIdentifier, @Value("${dropbox.accessToken}") String accessToken) {
        DbxRequestConfig config = DbxRequestConfig.newBuilder(clientIdentifier).build();
        return new DbxClientV2(config, accessToken);
    }
}
