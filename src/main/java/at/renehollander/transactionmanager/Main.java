package at.renehollander.transactionmanager;

import at.renehollander.transactionmanager.manager.Manager;
import at.renehollander.transactionmanager.station.DatabaseConnection;
import at.renehollander.transactionmanager.station.Station;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.stream.Stream;

public class Main {
    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, SQLException, IOException {
        File dbFolder = new File("db/");
        if (dbFolder.exists()) FileUtils.deleteDirectory(dbFolder);
        dbFolder.mkdir();

        Manager manager = new Manager(4000);
        Station station1 = new Station("station1", dbFolder, "localhost", 4000);
        Station station2 = new Station("station2", dbFolder, "localhost", 4000);
        Station station3 = new Station("station3", dbFolder, "localhost", 4000);
        createTable(station1.getDatabaseConnection());
        createTable(station2.getDatabaseConnection());
        createTableOther(station3.getDatabaseConnection());

        Thread.sleep(200);

        LOG.info("Waiting for Statements");
        LOG.info("Example: INSERT INTO test VALUES(2, 'Hallo Welt')");
        LOG.info("For testing purposes id 1 on station 3 is already used!");

        Scanner sc = new Scanner(System.in);
        while (true) {
            // INSERT INTO test VALUES(1, 'Hello World')
            String line = sc.nextLine();
            manager.execute(30, line, (err) -> {
                if (err != null) {
                    Stream.of(err).forEach(LOG::error);
                }
                LOG.info("Finished statement " + line);
            });
        }
    }

    private static void createTableOther(DatabaseConnection databaseConnection) throws SQLException {
        Connection connection = databaseConnection.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, name STRING)");
        statement.execute("INSERT INTO test VALUES(1, 'Hello World')");
        connection.commit();
        statement.close();
    }

    public static void createTable(DatabaseConnection databaseConnection) throws SQLException {
        Connection connection = databaseConnection.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, name STRING)");
        connection.commit();
        statement.close();
    }

}
