package com.wordnik.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PrintFormat {
	public static NumberFormat LONG_FORMAT = new DecimalFormat("###,###");
	public static NumberFormat NUMBER_FORMAT = new DecimalFormat("###.##");
	public static NumberFormat PERCENT_FORMAT = new DecimalFormat("#.##%");
}
