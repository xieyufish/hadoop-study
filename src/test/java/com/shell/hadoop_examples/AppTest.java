package com.shell.hadoop_examples;

import java.text.ParseException;
import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest {
	public static void main(String[] args) throws ParseException {
		System.out.println("9899821411,\"Burke, Honorato U.\",Alaska,Eu Incorporated".split(",").length);
		System.out.println(Arrays.toString("9899821411,\"Burke, Honorato U.\",Alaska,Eu Incorporated".split(",")));
		//System.out.println(new SimpleDateFormat("dd/MMM/yyyy:HH:00:00 Z", Locale.ENGLISH).parse("16/Dec/2015:05:32:50 -0500"));
		System.out.println(Arrays.toString("d001	Marketing".split("\t")));
	}
}
