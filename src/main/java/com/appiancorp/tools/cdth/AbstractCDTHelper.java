package com.appiancorp.tools.cdth;

import java.util.ArrayList;
import java.util.List;

import com.appiancorp.suiteapi.type.TypedValue;

public abstract class AbstractCDTHelper {
	private ArrayList<String> indexNames = new ArrayList<String>();
	private ArrayList<Object> values = new ArrayList<Object>();

	private Long typeOf;
	private Long[] types;

	public AbstractCDTHelper(Long typeOf, String[] names, Long[] types) {
		for (int i=0; i<names.length; i++) {
			indexNames.add(names[i]);	// initialise name look up
			values.add(null);			// initialise values array
		}

		this.typeOf = typeOf;
		this.types = types;
	}

	/**
	 * Returns an Object, Object[] or null
	 * 
	 * An Object is returned when it's a specific value (String, Long, CDTHelper, etc)
	 * An Object[] is returned for a multiple parameter (String[], Long[], CDTHelper[], etc)
	 * 
	 * @param name
	 * @return
	 */
	public Object getValue(String name) {
		if (name.contains(".")) {
			CDTHelper helper = getSubCDTSingle(name);
			return helper.getValue(name.substring(name.indexOf('.') + 1));
		}

		// TODO: properly throw name not found exception
		Object obj = values.get(indexNames.indexOf(name));

		if (obj instanceof CDTHelper) {
			return obj;
		} else if (obj instanceof List<?>) {
			Object[] objs = ((List<?>)obj).toArray();

			if (objs[0] instanceof CDTHelper) {
				return (Object)((CDTHelper[])((List<?>)obj).toArray(new CDTHelper[] {}));
			}
		}

		return toObject(obj);
	}

	private CDTHelper getSubCDTSingle(String name) {
		String[] parts = name.split("\\.");
		CDTHelper subCDT = null;

		for (int i=0; i<parts.length-1; i++) {
			if (subCDT == null) {
				subCDT = (CDTHelper) getValue(parts[i]);
				continue;
			}

			subCDT = (CDTHelper) subCDT.getValue(parts[i]);
		}

		return subCDT;
	}

	public void setValue(String name, Object value) {
		if (name.contains(".")) {
			CDTHelper helper = getSubCDTSingle(name);

			helper.setValue(name.substring(name.indexOf('.') + 1), value);

			return;
		}

		// TODO: properly throw name not found exception
		values.set(indexNames.indexOf(name), value);
	}

	public void addValue(String name, Object value) {
		// TODO: support mutli.b.c.d
		Object obj = values.get(indexNames.indexOf(name));

		if (obj == null) {
			obj = new ArrayList<Object>();
		}

		if (obj instanceof List<?>) {
			// multiple pv
			ArrayList<Object> objects = (ArrayList<Object>)obj;

			objects.add(value);

			setValue(name, objects);
		} else {
			// TODO: cause exception, this must be array list!
			System.out.println(name + " IS NOT AN ARRAY LIST: " + obj.getClass().getName());
		}
	}

	public void removeValue(String name, Object value) {
		// TODO: support mutli.b.c.d
		Object obj = values.get(indexNames.indexOf(name));

		if (obj instanceof List<?>) {
			// multiple pv
			ArrayList<Object> objects = (ArrayList<Object>)obj;

			objects.remove(value);
		} else {
			// TODO: cause exception, this must be array list!
			System.out.println(name + " IS NOT AN ARRAY LIST: " + obj.getClass().getName());
		}
	}

	public Long getType(String name) {
		// TODO: support nesting (e.g project.bob.bill.item)

		return types[indexNames.indexOf(name)];
	}

	public Long getTypeOf() {
		return typeOf;
	}
	
	public String[] getNames() {
		return indexNames.toArray(new String[] {});
	}

	public Object toObject() {
		return toObject(values.toArray());
	}

	private Object toObject(Object obj) {
		if (obj == null) {
			return null;
		}

		Class<?> type = obj.getClass();

		if (type.isArray()) {
			// CDTHelper values array converted to Object[]
			Object[] objs = (Object[])obj;

			for (int i=0; i<objs.length; i++) {
				objs[i] = toObject(objs[i]); 
			}

			return objs;
		} else if (obj instanceof CDTHelper) {
			// Single CDTHelper
			return ((CDTHelper)obj).toObject();
		} else if (obj instanceof List<?>) {
			Object[] objs = ((List<?>)obj).toArray();

			if (objs[0] instanceof CDTHelper) {
				// List of CDTHelper (needs to become an array of arrays)
				List<CDTHelper> helpers = (List<CDTHelper>)obj;

				return CDTHelperUtils.getObjectMultiCDT(helpers);
			} else {
				// List of normal types
				return objs;
			}
		} else {
			// Single normal type
			return obj;
		}
	}

	public TypedValue toTypedValue() {
		TypedValue tv = new TypedValue();

		tv.setValue(
			new TypedValue(getTypeOf(), toObject())
		);

		return tv;
	}
}