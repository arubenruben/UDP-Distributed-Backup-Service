import client.TestApp;
import peer.peer_factory.PeerFactory;

import java.util.Scanner;

/**
 * This class provides test scenarios to run a test suite faster and easier
 */
public class Scripts {

    public static final String LINE_SEPARATOR = "--------------------------------------------------------";
    public static final String PEER_2_BACKUP_PICTURE_JPG_3 = "peer2 BACKUP picture.jpg 3";
    public static final String PEER_2_BACKUP_150KB_PDF_2 = "peer2 BACKUP 150KB.pdf 2";
    public static final String PEER_2_RESTORE_PICTURE_JPG = "peer2 RESTORE picture.jpg";
    public static final String PEER_2_RESTORE_PDF = "peer2 RESTORE 150KB.pdf";
    public static final String RECLAIM = " RECLAIM ";
    public static final String STATE_PEER = "STATE";
    public static final String PEER_2_DELETE_PICTURE_JPG = "peer2 DELETE picture.jpg";
    public static final String PEER_2_DELETE_PDF = "peer2 DELETE 150KB.pdf";
    public static final String PEERS = " 224.0.0.1 5000 224.0.0.2 6000 224.0.0.2 7000 ";

    public static void main(String[] args) throws Exception {
        System.out.println("-------------------------------SDIS PROJECT DEMO-------------------------------");
        System.out.println("At any time during a simulation, you are able to enter the following commands:");
        System.out.println("    done           - enter it to end a simulation");
        System.out.println("    clean          - enter it to end a simulation & clean the filesystem");
        System.out.println("    state_<peerId> - enter it to present a state of the peer\n");
        Scanner scanner = new Scanner(System.in);
        selectScenario(scanner,true);

    }

    private static void selectScenario(Scanner scanner, boolean start) throws InterruptedException {
        System.out.print("Pick a scenario ( backup / restore / reclaim_<peerId>_<size> / delete / state_<peerId> / complex ): ");
        String scenario = scanner.nextLine();
        System.out.print("Clean first ( y / n ): ");
        String clean = scanner.nextLine();

        if (clean.equals("y")){
            Cleaner.main(makeArgs(""));
        }

        if (start) listenForExtraCommands(scanner);

        switch (scenario){
            case "complex":
                complexScenarios(scanner);
                break;
            case "backup":
                backupScenario("1.0");
                break;
            case "restore":
                restoreScenario("1.0");
                break;
            case "delete":
                deleteScenario("1.0");
                break;
            default:
                if (scenario.startsWith("reclaim")){
                    String[] parts = scenario.split("_");
                    int peerId = Integer.parseInt(parts[1]);
                    String size = parts[2];

                    reclaimScenario(size,peerId,"1.0");
                } else if (scenario.startsWith("state")){
                    showStateScenario(Integer.parseInt(scenario.split("_")[1]),"1.0",5);
                } else {
                    System.out.println("404 - scenario not found");
                    selectScenario(scanner,false);
                }
        }
    }

    private static void listenForExtraCommands(Scanner in) {
        new Thread(() -> {
            try {
                cmdListener(in);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void complexScenarios(Scanner scanner) throws InterruptedException {
        System.out.printf("BACKUP\n  " +
                "1.0 Runs BACKUP of an image and pdf with 1.0 protocol with 10 peers\n      Don't forget to call CLEAN!\n  " +
                "1.1 Runs BACKUP of an image and pdf with 1.1 protocol with 10 peers\n");

        System.out.printf("RESTORE\n  " +
                "2.0 Runs RESTORE of the pdf with 1.1 protocol with 10 peers\n");

        System.out.printf("DELETE\n  " +
                "3.0 Runs DELETE of the image and pdf with 1.1 protocol with 9 peers\n  " +
                "3.1 Runs STATE on peer9\n");
        System.out.print("Choose ( 1.0 / 1.1 / 2.0 / 3.0 / 3.1 ): ");
        String option = scanner.nextLine();
        switch (option){
            case "1.0":
                backupMulti("1.0",10);
                break;
            case "1.1":
                backupMulti("1.1",10);
                break;
            case "2.0":
                restorePdf("1.1",10);
                break;
            case "3.0":
                deleteJpgAndPdf("1.1",9);
                break;
            case "3.1":
                showStateScenario(9,"1.1",10);
                break;
        }
    }

    private static void deleteJpgAndPdf(String protocol, int numPeers) throws InterruptedException {
        showContext(PEER_2_DELETE_PICTURE_JPG);
        showContext(PEER_2_DELETE_PDF);

        PeerFactory.main(makeArgs(protocol + peers(numPeers)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_DELETE_PICTURE_JPG));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_DELETE_PDF));
    }

    private static void restorePdf(String protocol, int numPeers) throws InterruptedException {
        showContext(PEER_2_RESTORE_PDF);

        PeerFactory.main(makeArgs(protocol + peers(numPeers)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_RESTORE_PDF));
    }


    private static void deleteScenario(String protocol) throws InterruptedException {
        showContext(PEER_2_DELETE_PICTURE_JPG);

        PeerFactory.main(makeArgs(protocol + peers(5)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_DELETE_PICTURE_JPG));
    }

    private static void showStateScenario(int peerId, String protocol, int numPeers) throws InterruptedException {
        showContext(STATE_PEER + " peer" + peerId);

        PeerFactory.main(makeArgs(protocol + peers(numPeers)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs("peer" + peerId + " " + STATE_PEER));
    }

    private static void restoreScenario(String protocol) throws InterruptedException {
        showContext(PEER_2_RESTORE_PICTURE_JPG);

        PeerFactory.main(makeArgs(protocol + peers(5)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_RESTORE_PICTURE_JPG));
    }


    private static void backupScenario(String protocol) throws InterruptedException {
        showContext(PEER_2_BACKUP_PICTURE_JPG_3);

        PeerFactory.main(makeArgs(protocol + peers(5)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_BACKUP_PICTURE_JPG_3));
    }

    private static void backupMulti(String protocol, int numPeers) throws InterruptedException {
        showContext(PEER_2_BACKUP_PICTURE_JPG_3);
        showContext(PEER_2_BACKUP_150KB_PDF_2);

        PeerFactory.main(makeArgs(protocol + peers(numPeers)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_BACKUP_PICTURE_JPG_3));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs(PEER_2_BACKUP_150KB_PDF_2));
    }

    private static void reclaimScenario(String size, int peerId, String protocol) throws InterruptedException {
        showContext("peer" + peerId + RECLAIM + size);

        PeerFactory.main(makeArgs(protocol + peers(5)));
        Thread.sleep(2*1000);
        TestApp.main(makeArgs("peer" + peerId + RECLAIM  + size));
    }

    // region utils
    private static void showContext(String cmd) {
        System.out.println(LINE_SEPARATOR);
        System.out.println(cmd);
        System.out.println(LINE_SEPARATOR);
    }

    private static void cmdListener(Scanner in) throws Exception {
        Thread.sleep(10*1000);
        while (true){
            String cmd = in.nextLine();
            switch (cmd){
                case "clean":
                    Cleaner.main(makeArgs(""));
                    System.exit(0);
                    break;
                case "done":
                    System.exit(0);
                    break;
                default:
                    if (cmd.startsWith("state")){
                        String peerId = cmd.split("_")[1];
                        TestApp.main(makeArgs("peer" + peerId + " " + STATE_PEER));
                    } else {
                        System.out.println("404 - command not found");
                    }
            }
            System.out.println(LINE_SEPARATOR);
        }
    }

    private static String[] makeArgs(String s) {
        return s.split(" ");
    }

    private static String peers(int numPeers){
        return PEERS + numPeers;
    }

    // endregion
}
