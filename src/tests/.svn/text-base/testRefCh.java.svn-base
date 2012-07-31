package tests;

public class testRefCh {

	static class inner{
		StringBuffer str;
		inner(StringBuffer s){
			str = s;
		}
		void change(){
			//str = "changed";
			str.append("append");
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		inner in1 = new inner();
//		in1.str = "inner1";
//		inner in1r = in1;
//		in1r.str = "inner ref";
//		System.out.println(in1.str + "  " + in1r.str);

		StringBuffer org = new StringBuffer("orginal");
		inner in1 = new inner(org);
		in1.change();
		System.out.println(org + ", " + in1.str);
		
		System.out.println(new String(new char[]{'a' + 1}));
		
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < 10; j++) {
			sb.append(new String(new char[]{(char) ('a' + 1)}));
		}
		System.out.println(sb);
		
	}

}
