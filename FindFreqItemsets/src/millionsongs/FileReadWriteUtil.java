package millionsongs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jiachiliu on 11/17/14.
 * Util to support file operation
 */
public class FileReadWriteUtil {

    /**
     * Write the given contents to the folder
     *
     * @param contents a list of content that will be write in separated files
     * @param config   config object
     * @param folder   destination
     * @throws IOException
     */
    public void write(List<String> contents, Configuration config, String folder) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(folder), config);
        Path path = new Path(folder);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        Path clusterPartialFile = new Path(folder + String.format("part-r-%05d",0));
        FSDataOutputStream out = fileSystem.create(clusterPartialFile);

        for (int i = 0; i < contents.size(); i++) {
        	out.write(contents.get(i).getBytes());
        	out.write("\n".getBytes());
        }
        
        out.close();

    }

    /**
     * Read all files from the given folder, return the content of each file
     *
     * @param folder a folder contains partial files
     * @param config config object
     * @return a list of content
     * @throws Exception
     */
    public List<String> read(String folder, Configuration config) throws Exception {
        List<String> contents = new LinkedList<String>();
        FileSystem fileSystem = FileSystem.get(URI.create(folder), config);
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(folder));

        for (FileStatus status : fileStatus) {
            Path filePath = status.getPath();
            if (filePath.getName().matches("^part-[rm]-[0-9]{5}$")) {
                FSDataInputStream in = fileSystem.open(filePath);
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(in));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().length() > 0) {
                        contents.add(line);
                    }
                }
                br.close();
            }
        }
        return contents;
    }

    /**
     * Copy all files from one folder to another one
     *
     * @param srcFolder    the source folder
     * @param destFolder   the destination folder
     * @param config       config object
     * @param deleteOrigin indicate whether or not delete the source folder after copy
     * @throws Exception
     */
    public void copy(String srcFolder, String destFolder, Configuration config, boolean deleteOrigin) throws Exception {
        FileSystem srcFs = FileSystem.get(URI.create(srcFolder), config);
        FileSystem destFs = FileSystem.get(URI.create(destFolder), config);
        FileStatus[] srcFileStatus = srcFs.listStatus(new Path(srcFolder));

        Path destPath = new Path(destFolder);

        if (destFs.exists(destPath)) {
            destFs.delete(destPath, true);
        }

        for (FileStatus status : srcFileStatus) {
            Path filePath = status.getPath();
            String fileName = filePath.getName();
            if (fileName.matches("^part-r-[0-9]{5}$")) {
                FSDataInputStream inputStream = srcFs.open(filePath);
                FSDataOutputStream outputStream = destFs.create(new Path(destFolder + fileName));
                IOUtils.copyBytes(inputStream, outputStream, config);
                inputStream.close();
                outputStream.close();
            }
        }

        if (deleteOrigin) {
            srcFs.delete(new Path(srcFolder), true);
        }

    }
}
