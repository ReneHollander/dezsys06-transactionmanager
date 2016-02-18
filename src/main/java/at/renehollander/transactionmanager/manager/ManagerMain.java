package at.renehollander.transactionmanager.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.stream.Stream;

public class ManagerMain {

    private static Logger LOG = LoggerFactory.getLogger(ManagerMain.class);

    public static void main(String[] args) throws InterruptedException {
        Manager manager = new Manager(Integer.parseInt(args[0]));

        Thread.sleep(200);

        LOG.info("Waiting for Statements");
        LOG.info("Example: CREATE TABLE test(id INTEGER PRIMARY KEY, name STRING)");
        LOG.info("Example: INSERT INTO test VALUES(1, 'Hello World')");

        Scanner sc = new Scanner(System.in);
        while (true) {
            String line = sc.nextLine();
            manager.execute(30, line, (err) -> {
                if (err != null) {
                    Stream.of(err).forEach(LOG::error);
                }
                LOG.info("Finished statement " + line);
            });
        }
    }

}
