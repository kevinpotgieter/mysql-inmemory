package uk.bluedawnsolutions.mysql.inmemory.rules;

import com.google.common.base.Preconditions;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import uk.bluedawnsolutions.mysql.inmemory.EmbeddedMySqlDatabase;
import uk.bluedawnsolutions.mysql.inmemory.EmbeddedMySqlDatabaseBuilder;

public class EmbeddedMysqlDatabaseIntegrationRule extends ExternalResource {

    private final Logger logger = LoggerFactory.getLogger(EmbeddedMysqlDatabaseIntegrationRule.class);

    private static EmbeddedMySqlDatabase dataSource;
    private static EmbeddedMySqlDatabaseBuilder embeddedMySqlDatabaseBuilder;

    public EmbeddedMysqlDatabaseIntegrationRule(EmbeddedMySqlDatabaseBuilder embeddedMySqlDatabaseBuilder) {
        Preconditions.checkNotNull(embeddedMySqlDatabaseBuilder, "embeddedMySqlDatabaseBuilder cannot be null");

        if (this.embeddedMySqlDatabaseBuilder == null) {
            this.embeddedMySqlDatabaseBuilder = embeddedMySqlDatabaseBuilder;

            dataSource = embeddedMySqlDatabaseBuilder.build();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    if (dataSource != null) {
                        dataSource.shutdown();
                    }
                }
            });
        }
    }

    public void runScripts(String... scripts) {
        Preconditions.checkNotNull(scripts, "argument for scripts cannot be null");
        Preconditions.checkArgument(scripts.length > 0, "provide at least one script file");
        try {
            final ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            final ResourceLoader resourceLoader = new DefaultResourceLoader();
            for (String script : scripts) {
                logger.debug("Adding [{}] script to be executed.");
                populator.addScript(resourceLoader.getResource(script));
            }
            DatabasePopulatorUtils.execute(populator, dataSource);
        } catch (Exception e) {
            logger.error("Error running scripts against database.", e);
        }
    }
}
