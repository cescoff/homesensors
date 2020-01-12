package com.desi.data.bean;

import com.desi.data.utils.JAXBUtils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.*;
import java.util.List;
import java.util.Objects;

@XmlRootElement(name = "annotated-image")
@XmlAccessorType(XmlAccessType.FIELD)
public class AnnotatedImage {

    @XmlTransient
    private LocalDateTime dateTaken;

    @XmlElement(name = "date-taken")
    private String dateTakenStr;

    @XmlElement(name = "file-name")
    private String fileName;

    @XmlElement(name = "value") @XmlElementWrapper(name = "annotations")
    private List<String> textElements = Lists.newArrayList();

    @XmlElement(name = "latitude-ref")
    private String latitudeRef;

    @XmlElement(name = "latitude")
    private String latitude;

    @XmlElement(name = "longitude-ref")
    private String longitudeRef;

    @XmlElement(name = "longitude")
    private String longitude;

    @XmlElement(name = "altitude")
    private String altitude;

    public LocalDateTime getDateTaken() {
        return dateTaken;
    }

    public void setDateTaken(LocalDateTime dateTaken) {
        this.dateTaken = dateTaken;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<String> getTextElements() {
        return textElements;
    }

    public String getLatitudeRef() {
        return latitudeRef;
    }

    public void setLatitudeRef(String latitudeRef) {
        this.latitudeRef = latitudeRef;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitudeRef() {
        return longitudeRef;
    }

    public void setLongitudeRef(String longitudeRef) {
        this.longitudeRef = longitudeRef;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getAltitude() {
        return altitude;
    }

    public void setAltitude(String altitude) {
        this.altitude = altitude;
    }

    boolean beforeMarshal(Marshaller marshaller) {
        if (this.dateTaken == null) return true;
        this.dateTakenStr = this.dateTaken.toString();
        return true;
    }

    void afterUnmarshal(Unmarshaller unmarshaller, Object parent) {
        if (StringUtils.isNotEmpty(this.dateTakenStr)) {
            this.dateTaken = new LocalDateTime(this.dateTakenStr);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnnotatedImage)) return false;
        AnnotatedImage that = (AnnotatedImage) o;
        return Objects.equals(getDateTaken(), that.getDateTaken()) &&
                Objects.equals(dateTakenStr, that.dateTakenStr) &&
                Objects.equals(getFileName(), that.getFileName()) &&
                Objects.equals(getTextElements(), that.getTextElements()) &&
                Objects.equals(getLatitudeRef(), that.getLatitudeRef()) &&
                Objects.equals(getLatitude(), that.getLatitude()) &&
                Objects.equals(getLongitudeRef(), that.getLongitudeRef()) &&
                Objects.equals(getLongitude(), that.getLongitude()) &&
                Objects.equals(getAltitude(), that.getAltitude());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDateTaken(), dateTakenStr, getFileName(), getTextElements(), getLatitudeRef(), getLatitude(), getLongitudeRef(), getLongitude(), getAltitude());
    }

    public static void main(String[] args) throws JAXBException {
        final AnnotatedImage test = new AnnotatedImage();
        test.setDateTaken(new LocalDateTime("1980-5-18T07:00:00"));
        test.setFileName("Birth.jpg");
        Iterables.addAll(test.getTextElements(), Lists.newArrayList("Text1", "Text2", "Text3"));
        final String xml = JAXBUtils.marshal(test, true);
        System.out.println(xml);
        System.out.println(JAXBUtils.unmarshal(AnnotatedImage.class, xml).getDateTaken());
    }

}
