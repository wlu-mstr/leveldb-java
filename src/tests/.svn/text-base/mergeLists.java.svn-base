package tests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 * a demo of merge to Lists, whose values are both sorted 
 */
public class mergeLists {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// two test lists with sequential int values
		List<Integer> added_list = new ArrayList<Integer>();
		List<Integer> base_list = new ArrayList<Integer>();
		for(int i = 0; i < 20; i+=2){
			base_list.add(i);
		}
		for(int i = -3; i < 30; i+=3){
			added_list.add(i);
		}
		System.out.println("base " +base_list);
		System.out.println("add " + added_list);
		Iterator<Integer> added_iter = added_list.iterator();
		Iterator<Integer> base_iter = base_list.iterator();
		int lbase = 0;
		// since there is no "Prev" function for Interator,
		// we need to use this boolean to record whether
		// it needs to move next
		boolean base_added = true;
		// begin to merge
		for(;added_iter.hasNext();){
        	int ladded = added_iter.next();
        	for(;(base_added && base_iter.hasNext()) || !base_added;){
        		if(base_added){
        			lbase = base_iter.next();
        		}
        		if(ladded > lbase){
        			System.out.print("b"  + lbase + ", ");
        			base_added = true;
        		}
        		else{
        			base_added = false;
        			System.out.print("k, ");
        			break;
        		}
        	}
        	System.out.print("a" + ladded + ", ");
        }
		// deal with left data in list b 
		for(;(base_added && base_iter.hasNext()) || !base_added;){
			if(base_added){
    			lbase = base_iter.next();
    		}
			System.out.print("b"  + lbase + ", ");
			base_added = true;
		}

	}

}
