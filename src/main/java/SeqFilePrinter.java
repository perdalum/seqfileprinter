import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by pmd on 28/04/14.
 */
public class SeqFilePrinter {
    private static void readSeqFile(Path pathToFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathToFile, conf);

        Text key = new Text(); // this could be the wrong type
        Text val = new Text(); // also could be wrong

        while (reader.next(key, val)) {
            System.out.println(key + ":" + val);
        }
    }

    public static void main(String args[]) throws IOException {
        System.err.println("Will print sequence file " + args[0]);
        readSeqFile(new Path(args[0]));
    }
}
