package ca.uhn.fhir.mdm.rules.matcher;

import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r5.model.BaseDateTimeType;
import org.hl7.fhir.r5.model.DateTimeType;
import org.hl7.fhir.r5.model.DateType;

public class HapiDateMatcherR5 {
	public boolean match(IBase theLeftBase, IBase theRightBase) {
		if (theLeftBase instanceof BaseDateTimeType && theRightBase instanceof BaseDateTimeType) {
			BaseDateTimeType leftDate = (BaseDateTimeType) theLeftBase;
			BaseDateTimeType rightDate = (BaseDateTimeType) theRightBase;
			int comparison = leftDate.getPrecision().compareTo(rightDate.getPrecision());
			if (comparison == 0) {
				return leftDate.getValueAsString().equals(rightDate.getValueAsString());
			}
			BaseDateTimeType leftPDate;
			BaseDateTimeType rightPDate;
			//Left date is coarser
			if (comparison < 0) {
				leftPDate = leftDate;
				if (rightDate instanceof DateType) {
					rightPDate = new DateType(rightDate.getValue(), leftDate.getPrecision());
				} else {
					rightPDate = new DateTimeType(rightDate.getValue(), leftDate.getPrecision());
				}
				//Right date is coarser
			} else {
				rightPDate = rightDate;
				if (leftDate instanceof DateType) {
					leftPDate = new DateType(leftDate.getValue(), rightDate.getPrecision());
				} else {
					leftPDate = new DateTimeType(leftDate.getValue(), rightDate.getPrecision());
				}
			}
			return leftPDate.getValueAsString().equals(rightPDate.getValueAsString());
		}

		return false;
	}
}
