package com.wyeknot.serendiptwitty;


public class Pair<F, S> {
	private F first;
	private S second;

	public Pair(F f, S s){
		this.first = f;
		this.second = s;
	}

	public F getFirst() { return first; }
	public S getSecond() { return second; }

	public int hashCode() {
		return first.hashCode() + second.hashCode();
	}

	public boolean equals(Object o) {
		if (!(o instanceof Pair<?,?>)) {
			return false;
		}

		@SuppressWarnings("unchecked")
		Pair<F,S> obj = (Pair<F,S>)o;
		if (first.equals(obj.first) && second.equals(obj.second)) {
			return true;
		}

		return false;
	}
}