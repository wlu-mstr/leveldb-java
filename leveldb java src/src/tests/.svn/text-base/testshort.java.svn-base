package tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.leveldb.util.util;

public class testshort {
	static class inner{
		StringBuffer i = new StringBuffer();
		public StringBuffer get(){
			return i;
		}
	}
	
	static void testadd(List<String> lst){
		lst.add("added");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		inner ine = new inner();
		ine.get().append("s");
		System.out.println(ine.get().toString());
		
		Set<Integer> s = new HashSet<Integer>();
		s.add(100);
		System.out.println(s);
		s.remove(100);
		System.out.println(s);
		
		SortedSet<String> allSet=new TreeSet<String>();
        allSet.add("A");
        allSet.add("B");
        allSet.add("B");
        allSet.add("B");
        allSet.add("C");
        allSet.add("D");
        allSet.add("E");
        System.out.println(allSet);
        System.out.println("第一个元素："+allSet.first());
        System.out.println("最后一个元素："+allSet.last());
        System.out.println("headSet元素："+allSet.headSet("C"));//返回从第一个元素到指定元素的集合
        System.out.println("tailSet元素："+allSet.tailSet("C"));    //返回从指定元素到最后
        System.out.println("subSet元素："+allSet.subSet("B", "D"));//指定区间元素
        Iterator ii = allSet.iterator();
        for(;ii.hasNext();){
        	System.out.println(ii.next());
        }
        
        List<List<String>> ll = new ArrayList<List<String>>();
        List<String> l = new ArrayList<String>();
        ll.add(l);
        testadd(ll.get(0));
        System.out.println(ll.get(0));
        
        TreeMap<String, String> ts = new TreeMap<String, String>();
        ts.put("b", "b");
        ts.put("z", "b");
        ts.put("c", "b");
        ts.put("a", "b");
        ts.put("m", "b");
        ts.put("g", "b");
        for(String ss:ts.keySet()){
        	System.out.println(ss);
        }
        
        String str = "abc";
        System.out.println((str + (char)('1' - 1)));
        
        System.out.println(Integer.parseInt("a", 16));
        
	}

}
