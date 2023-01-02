package systems.bdev.cobrasync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import systems.bdev.cobrasync.services.CobraSyncService;

@SpringBootApplication
public class CobraSyncApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(CobraSyncApplication.class, args);
        context.getBean(CobraSyncService.class).run();
    }
}
