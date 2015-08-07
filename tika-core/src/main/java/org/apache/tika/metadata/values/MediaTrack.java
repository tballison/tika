package org.apache.tika.metadata.values;

//TODO: This is a demo class not to be committed!!!
//This is meant to demo the possibility of adding pbcore data elements
//to Tika.
//Someone who actually understands pbcore needs to implement this
//and get rid of all the strings!
//Please see https://github.com/AlfrescoLabs/tika-ffmpeg


public class MediaTrack {
    String essenceTrackType;
    String essenceTrackIdentifier;
    String essenceTrackStandard;
    String essenceTrackEncoding;
    //...yada, yada


    public String getEssenceTrackType() {
        return essenceTrackType;
    }

    public void setEssenceTrackType(String essenceTrackType) {
        this.essenceTrackType = essenceTrackType;
    }

    public String getEssenceTrackIdentifier() {
        return essenceTrackIdentifier;
    }

    public void setEssenceTrackIdentifier(String essenceTrackIdentifier) {
        this.essenceTrackIdentifier = essenceTrackIdentifier;
    }

    public String getEssenceTrackStandard() {
        return essenceTrackStandard;
    }

    public void setEssenceTrackStandard(String essenceTrackStandard) {
        this.essenceTrackStandard = essenceTrackStandard;
    }

    public String getEssenceTrackEncoding() {
        return essenceTrackEncoding;
    }

    public void setEssenceTrackEncoding(String essenceTrackEncoding) {
        this.essenceTrackEncoding = essenceTrackEncoding;
    }
}
