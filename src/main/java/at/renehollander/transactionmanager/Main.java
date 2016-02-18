package at.renehollander.transactionmanager;

import at.renehollander.transactionmanager.manager.Manager;
import at.renehollander.transactionmanager.station.DatabaseConnection;
import at.renehollander.transactionmanager.station.Station;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

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

        manager.execute(30, "INSERT INTO test VALUES(1, 'Hello World')", (err) -> {
            if (err != null) throw new RuntimeException(err);
            System.out.println("it worked");
        });
    }

    private static void createTableOther(DatabaseConnection databaseConnection) throws SQLException {
        Connection connection = databaseConnection.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE faggot(id INTEGER, name STRING)");
        statement.close();
    }

    public static void createTable(DatabaseConnection databaseConnection) throws SQLException {
        Connection connection = databaseConnection.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE test(id INTEGER, name STRING)");
        statement.close();
    }

}
