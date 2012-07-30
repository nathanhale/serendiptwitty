package com.wyeknot.serendiptwitty;

import java.util.Comparator;


public class User {
	public long id;
	public String name;
	
	public static int NO_ID_AVAILABLE = -1;	
	
	static final Comparator<User> NAME_ORDER = new Comparator<User>() {
        public int compare(User u1, User u2) {
        	return u1.name.compareToIgnoreCase(u2.name);
        }
    };
    
    User(long id, String name) {
    	this.id = id;
    	this.name = name;
    }
	
	
	User(String name) {
		this.name = name;
		this.id = NO_ID_AVAILABLE;
	}
	
	public String toString() {
		return name;
	}
}

