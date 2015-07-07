package uk.bluedawnsolutions.mysql.inmemory.example;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import uk.bluedawnsolutions.mysql.inmemory.EmbeddedMySqlDatabaseBuilder;
import uk.bluedawnsolutions.mysql.inmemory.rules.EmbeddedMysqlDatabaseIntegrationRule;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class SimpleSampleIntegrationTest {

    private static final String DATABASE_NAME = "test_database";
    private static final String TABLE_NAME = "sample";

    @Rule
    public EmbeddedMysqlDatabaseIntegrationRule databaseRule = new EmbeddedMysqlDatabaseIntegrationRule(
            new EmbeddedMySqlDatabaseBuilder()
                    .withDatabaseName(DATABASE_NAME)
                    .addSqlScript("initialSchema.sql")
    );

    @Test
    public void ensureThat_inMemoryMySql_canExecute_initialScripts() throws SQLException {
        // Arrange

        DataSource ds = new DriverManagerDataSource("jdbc:mysql://localhost:6140/test_database", "root", "");

        //Act
        Boolean expectedTableExists = new JdbcTemplate(ds)
                .query("SELECT COUNT(*) AS TABLE_EXISTS FROM information_schema.TABLES " +
                        "WHERE TABLE_SCHEMA like ? \n" +
                        "AND TABLE_NAME like ?;", this::getResult, DATABASE_NAME, TABLE_NAME);

        //Assert
        assertTrue(expectedTableExists);
    }

    private Boolean getResult(ResultSet rs) throws SQLException {
        return rs.next() && rs.getLong("TABLE_EXISTS") > 0;
    }
}
