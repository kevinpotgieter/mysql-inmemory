package uk.bluedawnsolutions.mysql.inmemory;

import com.mysql.management.MysqldResource;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;

public class EmbeddedMySqlDatabase extends DriverManagerDataSource {

    private final Logger logger = LoggerFactory.getLogger(EmbeddedMySqlDatabase.class);
    private final MysqldResource mysqldResource;

    EmbeddedMySqlDatabase(MysqldResource mysqldResource) {
        this.mysqldResource = mysqldResource;
    }

    public void shutdown() {
        if (mysqldResource != null) {
            mysqldResource.shutdown();
            if (!mysqldResource.isRunning()) {
                logger.info("Shutting down in memory MySQL");
                logger.debug("DELETING MYSQL BASE DIR [{}]", mysqldResource.getBaseDir());
                try {
                    FileUtils.forceDelete(mysqldResource.getBaseDir());
                } catch (IOException e) {
                    logger.error("Unable to delete temp file for in memory MySQL", e);
                }
            }
        }
    }
}
