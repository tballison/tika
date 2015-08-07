package org.apache.tika.metadata.values;


import org.apache.tika.metadata.MetadataValue;

public class PhoneNumberValue extends MetadataValue {

    //the raw phone number is stored in parent MetadataValue's string
    //the normalized phone number is stored here in "normalizedPhoneNumber".
    //As of this writing, we aren't actually doing any normalizing yet.

    private final String normalizedPhoneNumber;
    private final String countryCode;
    private final String numberType;

    public PhoneNumberValue(String phoneNumber, String countryCode, String numberType) {
        super(phoneNumber);
        this.normalizedPhoneNumber = normalize(phoneNumber);
        this.countryCode = countryCode;
        this.numberType = numberType;
    }

    private String normalize(String phoneNumber) {
        return phoneNumber;
    }

    public String getPhoneNumber() {
        return normalizedPhoneNumber;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getNumberType() {
        return numberType;
    }
}
