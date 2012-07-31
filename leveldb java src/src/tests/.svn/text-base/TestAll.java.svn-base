package tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class TestCalcuator extends TestCase {
	public TestCalcuator(){
		super();
	}
    public void testAdd(){
        Calcuator calcuator=new Calcuator();
        double result=calcuator.add(1,2);
        assertEquals(3,result,0);
    }

}

class TestCalcuator2 extends TestCase {
	public void testAdd2() {
		Calcuator calcuator = new Calcuator();
		double result = calcuator.add(1, 2);
		assertEquals(3, result, 0);
	}
}


public class TestAll extends TestSuite {
	public static Test suite() {
		TestSuite suite = new TestSuite("TestSuite Test");
		suite.addTestSuite(TestCalcuator.class);
		suite.addTestSuite(TestCalcuator2.class);
		return suite;
	}

	public static void main(String args[]) {
		TestRunner.run(suite());
	}
}