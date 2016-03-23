package conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This file should only be used in the Driver node (MainApp).
 * The executor directly access the parameter values.
 * @author a0048267
 *
 */
public class AppProperties {
    private static Properties props;
    // ensure this code runs at JVM start-up
    static {
	String filename = "app-config.xml";
	props = new Properties();
	File test_file = new File(filename);
	if (!test_file.exists()) {
	    test_file = new File("/home/wangzk/TrajectoryMining/app-config.xml");
	}
	try {
	    props.loadFromXML(new FileInputStream(test_file));
	} catch (IOException e) {
	    e.printStackTrace();
	}
	if (!props.containsKey("eps")) {
	    props.setProperty("eps", "1000");
	}
	if (!props.containsKey("minPts")) {
	    props.setProperty("minPts", "5");
	}
	if (!props.containsKey("K")) {
	    props.setProperty("K", "50");
	}
	if (!props.containsKey("G")) {
	    props.setProperty("G", "3");
	}
	if (!props.containsKey("L")) {
	    props.setProperty("L", "10");
	}
	if (!props.containsKey("M")) {
	    props.setProperty("M", "10");
	}
	if (!props.contains("kforward_partitions")) {
	    props.setProperty("kforward_partitions", "23");
	}
    }

    public static String getProperty(String prop_name) {
	return props.getProperty(prop_name);
    }
}
