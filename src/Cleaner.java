import java.io.File;


/**
 * This Class cleans the project folder
 */
public class Cleaner {
    /**
     * Removes the project file structure
     * @param args Empty
     */
    public static void main(String[] args) {
        System.out.println("Cleaning..");
        File root = new File("." + File.separator + "build" + File.separator + "peer" + File.separator + "filesystem" + File.separator + "storage");
        for (File file : root.listFiles()) {
            for (File subfolder : file.listFiles()) {
                if (!subfolder.getName().equals("files")){
                    deleteFolder(subfolder);
                }
            }
        }
    }

    /**
     *
     * @param folder Folder to remove. Must be a valid folder
     */
    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
