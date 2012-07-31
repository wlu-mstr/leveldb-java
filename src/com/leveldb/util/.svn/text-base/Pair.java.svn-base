package com.leveldb.util;

public class Pair<A, B> {
	private A first;
	private B second;
	
	public Pair(A f, B s){
		first = f;
		second = s;
	}

	public A getFirst(){
		return first;
	}
	
	public B getSecond(){
		return second;
	}
	
	public String toString(){
		return "[" + first + ", " + second + "]";
	}
	
	@Override
	public boolean equals(Object o){
		Pair other = (Pair)o;
		return first.equals(other.getFirst()) &&
		second.equals(other.getSecond());
	}
	
	@Override
	public int hashCode(){
		return first.hashCode() + second.hashCode();
		
	}
}
