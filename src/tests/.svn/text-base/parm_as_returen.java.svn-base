package tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.leveldb.common.db.SkipList;
import com.leveldb.common.options.Options.CompressionType;

class dummy{
	int i;
	String str;
	byte[] b;
	public void p(){
		System.out.println(i + ", " + str + "  " + b[0] );
	}
	public String toString(){
		return i + ", " + str;
	}
}

abstract class adummy{
	public  static String sfoo(){return "abstract";}
}

class cdummy extends adummy{
	public static String sfoo(){return "concret";}
}


class sel_dump {
	public String s;
	public sel_dump(String is){
		s = is;
	}
	
	public void set(String is){
		s = is;
	}
	public void dump() throws Throwable{
		System.out.println(s);
	}
}



public class parm_as_returen {
	static void ch_parm(dummy d){
		d.i = 10;
		d.str = "sfa";
		byte b[] = {0x1, 0x2};
		d.b = b;
	}
	
	static void ch_parm2(dummy id){
		id = new dummy();
		id.i= 10;
		id.str = "sfa";
	}
	

	
	public static void main(String args[]) throws Throwable{
//		dummy ld = new dummy();
//		ch_parm2(ld);
//		//ld.p();
//		//System.out.println(ld);
//		ch_parm(ld);
//		ld.p();
//		//System.out.println(ld);
//		
//		cdummy c = new cdummy();
//		System.out.println (c.sfoo());
//		
//		long l = 20;
//		long l2 = l << 8 | 1;
//		System.out.println (l2);
//		System.out.println(  l2 >> 8);
//		byte b1 = (byte) ( 23);
//		System.out.println(b1 & 128);
		
		
		String s1 = "asfs";
		String s2 = "asfs1";
		System.out.println(s1.indexOf("."));
		
		Queue<String> q = new ConcurrentLinkedQueue<String>();
		q.add("a");
		q.add("b");
		System.out.println(q.peek());
		System.out.println(q.size());
		System.out.println(q.peek());
		System.out.println(q.size());
		
		
		sel_dump sd = new sel_dump("");
		
		try {
			sd.dump();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(sd==null);
		
		
		
		
//		List<sel_dump> as = new ArrayList<sel_dump>(11);
//		as.set(10, new sel_dump(""));
//		
//		//as.add(10, new sel_dump(""));
//		as.get(10).set("AS");
//		for(sel_dump s_:as){
//			if(s_ != null){
//				s_.dump();
//			}
//		}
		
		Random r = new Random(); 
		for(int i = 0; i < 100; i++){
			int len = 1;
			while(true){
				int ri = Math.abs(r.nextInt());
				if(ri % 4 == 0){
					len++;
				}
				else{
					break;
				}
			}
			System.out.print(len + "\t");
			if(i % 10 ==0){
				System.out.println();
			}
			
		}
		
		SkipList.Iterator i;
		
	}
}
