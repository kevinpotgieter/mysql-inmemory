package uk.bluedawnsolutions.mysql.inmemory;

import com.mysql.management.MysqldResource;
import com.mysql.management.MysqldResourceI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedMySqlDatabaseBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMySqlDatabaseBuilder.class);

    private final String baseDatabaseDir = System.getProperty("java.io.tmpdir");
    private String databaseName = "test_db_" + System.nanoTime();
    private int port = 6140;
    private final String username = "root";
    private final String password = "";
    private boolean foreignKeyCheck = true;

    private final ResourceLoader resourceLoader;
    private final ResourceDatabasePopulator databasePopulator;

    public EmbeddedMySqlDatabaseBuilder() {
        resourceLoader = new DefaultResourceLoader();
        databasePopulator = new ResourceDatabasePopulator();
    }

    private EmbeddedMySqlDatabase createDatabase(MysqldResource mysqldResource) {
        if (!mysqldResource.isRunning()) {
            LOGGER.error("MySQL instance not found... Terminating");
            throw new RuntimeException("Cannot get Datasource, MySQL instance not started.");
        }
        EmbeddedMySqlDatabase database = new EmbeddedMySqlDatabase(mysqldResource);
        database.setDriverClassName("com.mysql.jdbc.Driver");
        database.setUsername(username);
        database.setPassword(password);
        String url = "jdbc:mysql://localhost:" + port + "/" + databaseName + "?" + "createDatabaseIfNotExist=true";

        if (!foreignKeyCheck) {
            url += "&sessionVariables=FOREIGN_KEY_CHECKS=0";
        }
        LOGGER.debug("database url: {}", url);
        database.setUrl(url);
        return database;
    }

    private MysqldResource createMysqldResource() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("=============== Starting Embedded MySQL using these parameters ===============");
            LOGGER.debug("baseDatabaseDir : " + baseDatabaseDir);
            LOGGER.debug("databaseName : " + databaseName);
            LOGGER.debug("host : localhost (hardcoded)");
            LOGGER.debug("port : " + port);
            LOGGER.debug("username : root (hardcode)");
            LOGGER.debug("password : (no password)");
            LOGGER.debug("=============================================================================");
        }

        Map<String, String> databaseOptions = new HashMap<String, String>();
        databaseOptions.put(MysqldResourceI.PORT, Integer.toString(port));

        MysqldResource mysqldResource = new MysqldResource(new File(baseDatabaseDir, databaseName));
        mysqldResource.start("embedded-mysqld-thread-" + System.currentTimeMillis(), databaseOptions);

        if (!mysqldResource.isRunning()) {
            throw new RuntimeException("MySQL did not start.");
        }

        LOGGER.info("MySQL started successfully @ {}", System.currentTimeMillis());
        return mysqldResource;
    }

    private void populateScripts(EmbeddedMySqlDatabase database) {
        try {
            DatabasePopulatorUtils.execute(databasePopulator, database);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            database.shutdown();
        }
    }

    public EmbeddedMySqlDatabaseBuilder addSqlScript(String script) {
        databasePopulator.addScript(resourceLoader.getResource(script));
        return this;
    }

    public EmbeddedMySqlDatabaseBuilder setForeignKeyCheck(boolean foreignKeyCheck) {
        this.foreignKeyCheck = foreignKeyCheck;
        return this;
    }

    public EmbeddedMySqlDatabaseBuilder withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }


    public EmbeddedMySqlDatabaseBuilder onPort(int port) {
        this.port = port;
        return this;
    }


    public EmbeddedMySqlDatabase build() {
        MysqldResource mysqldResource = createMysqldResource();
        EmbeddedMySqlDatabase database = createDatabase(mysqldResource);
        populateScripts(database);
        return database;
    }
}
