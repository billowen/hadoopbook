import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Created by billo on 2017/7/6.
 */
public class MaxTemperatureDriverClusterTest {
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.139:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "192.168.1.139:8032");

        Path input = new Path("/users/input/ncdc/all");
        Path output = new Path("/users/output/");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] {input.toString(), output.toString()});
        assertThat(exitCode, is(0));

    }
}
