package ru.shaldnikita.hadoop.putmerge;

import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class PutMerge {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        FileSystem local = FileSystem.getLocal(configuration);

        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        FileStatus[] inputFiles = local.listStatus(inputDir);
        @Cleanup FSDataOutputStream outputStream = hdfs.create(hdfsFile);

        for (int i = 0; i < inputFiles.length; i++) {
            Path path = inputFiles[i].getPath();

            System.out.println(path.getName());

            @Cleanup FSDataInputStream in = local.open(path);
            byte buffer[] = new byte[256];
            int bytesRead = 0;

            while ((bytesRead = in.read(buffer)) > 0) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }

    }
}
