package preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import model.TemporalPoint;

/**
 * specifically transform geolife data
 * 
 * we need to discretize the real-value time into time sequences for these
 * datasets, we round the timesnap at minutes level
 * 
 * During discretization, we treat each object's trajectory at each date as a
 * single trajectory. Thus all trajectories are happening at the same day
 * 
 * @author a0048267
 * 
 */
public class GeolifeTrajectoryTransform {
    private static String path_prefix = "dataset/";
    private static String outputDir = "Geolife/dis_flated.dat";
    private static String inputDir = "Geolife/Data/";
    private static int ID = 0;

    public static void transform() {

	// first retrieve all data folders
	Path data_path = Paths.get(path_prefix + inputDir);
	try {
	    DirectoryStream<Path> ds = Files.newDirectoryStream(data_path);
	    FileWriter fw = new FileWriter(path_prefix + outputDir);
	    BufferedWriter bw = new BufferedWriter(fw);
	    for (Path child : ds) {
		process(child, bw);
	    }
	    bw.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    /**
     * given a child, scan all trajectories inside
     * 
     * @param child
     * @param bw
     */
    private static void process(Path child, BufferedWriter bw) {
	Path data_path = Paths.get(child + "/Trajectory");
	System.out.println(child.getFileName());
	try {
	    DirectoryStream<Path> ds = Files.newDirectoryStream(data_path);
	    for (Path c : ds) {
		// process each file
		FileReader fr = new FileReader(c.toFile());
		BufferedReader br = new BufferedReader(fr);
		ArrayList<TemporalPoint> allTPs = new ArrayList<>();
		String line = null;
		String[] pars;
		int skip = 0;
		while ((line = br.readLine()) != null) {
		    if (skip++ < 6) {
			continue;
		    } else {
			pars = line.split(",");
			double lat = Double.parseDouble(pars[0]);
			double longt = Double.parseDouble(pars[1]);
			String[] times = pars[6].split(":");
			// time is rounded to 30-seconds interval;
			int time = Integer.parseInt(times[0]) * 120
				+ Integer.parseInt(times[1]) * 2
				+ (Integer.parseInt(times[2]) + 15) / 30;
			allTPs.add(new TemporalPoint(lat, longt, time));
		    }
		}
		// post processing, collapse points with identical
		// timestamps by averaging their coordinates
		Collections.sort(allTPs, new Comparator<TemporalPoint>() {
		    @Override
		    public int compare(TemporalPoint arg0, TemporalPoint arg1) {
			return arg0.getTime() - arg1.getTime();
		    }
		});
		int current = 0, next = 1;
		double sumla = allTPs.get(current).getLat();
		double sumlg = allTPs.get(current).getLont();
		while (next < allTPs.size()) {
		    while (next < allTPs.size() 
			    && allTPs.get(next).getTime() == allTPs.get(current)
			    .getTime()) {
			sumla += allTPs.get(next).getLat();
			sumlg += allTPs.get(next).getLont();
			next++;
		    }
		    // at this point, next is different from current
		    bw.write(String.format("%d\t%8.6f\t%8.6f\t%d\n", ID, sumla
			    / (next - current), sumlg / (next - current),
			    allTPs.get(current).getTime()));
		    // reset current
		    sumla = allTPs.get(current).getLat();
		    sumlg = allTPs.get(current).getLont();
		    current = next;
		    next++;
		}
		ID++;
		br.close();
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) {
	transform();
    }
}
