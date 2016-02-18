package at.renehollander.transactionmanager.station;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class StationMain {

    private static Logger LOG = LoggerFactory.getLogger(StationMain.class);

    public static void main(String[] args) {
        File dbFolder = new File("db/");
        if (!dbFolder.exists()) dbFolder.mkdirs();
        Station station = new Station(args[0], dbFolder, args[1], Integer.parseInt(args[2]));

        LOG.info("Waiting for Statements");
        LOG.info("Example: INSERT INTO test VALUES(1, 'Hello World')");

        Scanner sc = new Scanner(System.in);
        while (true) {
            // INSERT INTO test VALUES(1, 'Hello World')
            String line = sc.nextLine();
            station.getDatabaseConnection().execute(30, line, (err, ret) -> {
                if (err != null) {
                    LOG.error("An error occured executing statement", err);
                } else {
                    station.getDatabaseConnection().commit((err2) -> {
                        if (err2 != null) {
                            LOG.error("An error occured commiting", err2);
                        } else {
                            LOG.info("Finished statement " + line);
                        }
                    });
                }
            });
        }
    }

}
