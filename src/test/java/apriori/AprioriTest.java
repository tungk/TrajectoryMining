package apriori;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import scala.Tuple2;

public class AprioriTest {
    
    public static void main(String[] args) {
	//prepare input from file
	String filename = "C:/Users/a0048267/Desktop/input.txt";
	
	try {
	    FileReader fr = new FileReader(filename);
	    BufferedReader br = new BufferedReader(fr);
	    String line;
	    int line_num = 0;
	    int id = 0;
	    Tuple2<Integer, Iterable<Tuple2<Integer,IntSet>>> input;
	    ArrayList<Tuple2<Integer,IntSet>> sets = new ArrayList<>();
	    while((line = br.readLine()) != null) {
		if(line_num == 0) {
		    line_num = 1;
		    id = Integer.parseInt(line);
		} else {
		    String[] parts = line.split("\t");
		    int tid = Integer.parseInt(parts[0]);
		    String[] t_set_strings = parts[1].substring(1,parts[1].length() -1).split(", ");
		    IntSet t_set = new IntOpenHashSet();
		    for(String t_set_string : t_set_strings) {
			t_set.add(Integer.parseInt(t_set_string));
		    }
		    sets.add(new Tuple2<Integer,IntSet>(tid, t_set));
		}
	    }
	    br.close();
	    input = new Tuple2<Integer, Iterable<Tuple2<Integer,IntSet>>>(id, sets);
	    CliqueMiner cm = new CliqueMiner(30, 5, 5, 5);
	    Iterable<IntSet> output = cm.call(input);
	    for(IntSet out : output) {
		System.out.println(out);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
