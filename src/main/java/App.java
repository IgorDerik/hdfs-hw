import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class App {

    public FileSystem configureFilesystem(String coreSitePath, String hdfsSitePath) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            Path hdfsCoreSitePath = new Path(coreSitePath);
            Path hdfsHDFSSitePath = new Path(hdfsSitePath);
            conf.addResource(hdfsCoreSitePath);
            conf.addResource(hdfsHDFSSitePath);
            fileSystem = FileSystem.get(conf);
            return fileSystem;
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            return fileSystem;
        }
    }

    public String writeToHDFS(FileSystem fileSystem, String sourcePath, String destinationPath) {
        try {
            Path inputPath = new Path(sourcePath);
            Path outputPath = new Path(destinationPath);
            fileSystem.copyFromLocalFile(inputPath, outputPath);
            return "YES";
        } catch (IOException ex) {
            System.out.println("Some exception occurred while writing file to hdfs");
            return "NO";
        }
    }

    public String readFileFromHdfs(FileSystem fileSystem, String hdfsStorePath, String localSystemPath) {
        try {
            Path hdfsPath = new Path(hdfsStorePath);
            Path localPath = new Path(localSystemPath);
            fileSystem.copyToLocalFile(hdfsPath, localPath);
            return "YES";
        } catch (IOException ex) {
            System.out.println("Some exception occurred while reading file from hdfs");
            return "NO";
        }
    }

    public void closeFileSystem(FileSystem fileSystem) {
        try {
            fileSystem.close();
        } catch (Exception ex) {
            System.out.println("Unable to close Hadoop filesystem : " + ex);
        }
    }

}
