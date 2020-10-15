package com.appiancorp.tools.cdth;

import java.sql.Date;

public class CDTHelper extends AbstractCDTHelper {
	public CDTHelper(Long typeOf, String[] names, Long[] types) {
		super(typeOf, names, types);
	}

	// Main Types (String, Long, Double, Date, CDTHelper)
	// setters
	public void setValue(String name, String value) {
		super.setValue(name, value);
	}

	public void setValue(String name, Long value) {
		super.setValue(name, value);
	}

	public void setValue(String name, Double value) {
		super.setValue(name, value);
	}

	public void setValue(String name, Date value) {
		super.setValue(name, value);
	}

	public void setValue(String name, CDTHelper value) {
		super.setValue(name, value);
	}

	// adders
	public void addValue(String name, String value) {
		super.addValue(name, value);
	}

	public void addValue(String name, Long value) {
		super.addValue(name, value);
	}

	public void addValue(String name, Double value) {
		super.addValue(name, value);
	}

	public void addValue(String name, Date value) {
		super.addValue(name, value);
	}

	public void addValue(String name, CDTHelper value) {
		super.addValue(name, value);
	}

	// Primitive Types (int)
	// setters
	public void setValue(String name, int value) {
		setValue(name, new Long(value));
	}

	// adders
	public void addValue(String name, int value) {
		addValue(name, new Long(value));
	}
}
