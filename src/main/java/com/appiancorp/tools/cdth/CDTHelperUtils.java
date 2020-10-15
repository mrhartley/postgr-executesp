package com.appiancorp.tools.cdth;

import java.util.List;

import javax.xml.namespace.QName;

import com.appiancorp.suiteapi.process.ProcessVariableInstance;
import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.Datatype;
import com.appiancorp.suiteapi.type.NamedTypedValue;
import com.appiancorp.suiteapi.type.TypeService;
import com.appiancorp.suiteapi.type.TypedValue;
import com.appiancorp.suiteapi.type.exceptions.InvalidTypeException;

public class CDTHelperUtils {
	private CDTHelperUtils() { }

	public static final CDTHelper fromNamedTypedValue(Long type, NamedTypedValue[] ntvs) {
		String[] names = new String[ntvs.length];
		Long[] types = new Long[ntvs.length];

		for (int i=0; i<ntvs.length; i++) {
			names[i] = ntvs[i].getName();
			types[i] = ntvs[i].getInstanceType();
		}

		return new CDTHelper(type, names, types);
	}

	public static final CDTHelper fromQualifiedName(TypeService ts, String namespace, String typeName) throws InvalidTypeException {
		Datatype dtype = ts.getTypeByQualifiedName(new QName(namespace, typeName));

		return fromDatatype(dtype);
	}

	public static final CDTHelper fromTypeId(TypeService ts, Long typeId) throws InvalidTypeException {
		return fromDatatype(ts.getType(typeId));
	}

	public static final CDTHelper fromProcessVariableInstance(TypeService ts, ProcessVariableInstance pvi) throws InvalidTypeException {
	    Long dataType = pvi.getInstanceType();

		Object[] values = (Object[]) pvi.getRunningValue();

		return getCDTHelperWithValues(ts, dataType, values);
	}

	public static final CDTHelper fromDatatype(Datatype dtype) throws InvalidTypeException {
		return fromNamedTypedValue(dtype.getTypeof(), dtype.getInstanceProperties());
	}

	public static final CDTHelper fromTypedValue(TypedValue tv) {
		return null;
	}
	
	public static Object[][] getObjectMultiCDT(List<CDTHelper> obj) {
		return getObjectMultiCDT(obj.toArray(new CDTHelper[] {}));
	}

	public static Object[][] getObjectMultiCDT(CDTHelper[] obj) {
		Object[][] multiObjs = new Object[obj.length][];

		for (int i=0; i<obj.length; i++) {
			multiObjs[i] = (Object[])((CDTHelper)obj[i]).toObject();
		}

		return multiObjs;
	}

	private static CDTHelper getCDTHelperWithValues(TypeService ts, Long typeId, Object[] values) throws InvalidTypeException {
		if (values == null) {
			// Return a blank CDTHelper
			return fromTypeId(ts, typeId);
		}

	    Datatype dType = ts.getType(typeId);

		NamedTypedValue[] ntvs = dType.getInstanceProperties();

		CDTHelper helper = fromNamedTypedValue(dType.getTypeof(), ntvs);

		for (int i=0; i<ntvs.length; i++) {
			if (values[i] == null) {
				continue;
			}

			Class<?> type = values[i].getClass();

			if (ntvs[i].getInstanceType() < AppianType.INITIAL_CUSTOM_TYPE) {
				if (type.isArray()) {
					Object[] subObjects = (Object[])values[i];

					for (int k=0; k<subObjects.length; k++) {
						helper.addValue(ntvs[i].getName(), subObjects[k]);
					}
				} else {
					helper.setValue(ntvs[i].getName(), values[i]);
				}
			} else {
				boolean isMultiCDT = values[i] instanceof Object[][];

				if (isMultiCDT) {
					Object[] subObjects = (Object[])values[i];
					Datatype multiType = ts.getType(ntvs[i].getInstanceType());
					Long singleType = multiType.getTypeof();

					for (int k=0; k<subObjects.length; k++) {
						helper.addValue(ntvs[i].getName(), getCDTHelperWithValues(ts, singleType, (Object[])subObjects[k]));
					}
				} else {
					helper.setValue(ntvs[i].getName(), getCDTHelperWithValues(ts, ntvs[i].getInstanceType(), (Object[])values[i]));
				}
			}
		}

		return helper;
	}
}
