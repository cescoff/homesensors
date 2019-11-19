package com.desi.data.utils;

import com.google.common.collect.Maps;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.util.Map;

public class JAXBUtils {
    private static Map<Class<?>, JAXBContext> contexts = Maps.newHashMap();

    private JAXBUtils() {}

    public static <T> String marshal(T object) throws JAXBException {
        return marshal(object, false);
    }

    public static <T> String marshal(T object, boolean formated) throws JAXBException {
        StringWriter stringWriter = new StringWriter();
        marshal(object, stringWriter, formated);
        return stringWriter.toString();
    }

    public static <T> void marshal(T object, File file) throws JAXBException, IOException {
        marshal(object, file, false);
    }

    public static <T> void marshal(T object, File file, boolean formated) throws JAXBException, IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        marshal(object, fileOutputStream, formated);
        fileOutputStream.close();
    }

    public static <T> void marshal(T object, OutputStream outputStream) throws JAXBException, IOException {
        marshal(object, outputStream, false);
    }

    public static <T> void marshal(T object, OutputStream outputStream, boolean formated) throws JAXBException, IOException {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
        marshal(object, outputStreamWriter, formated);
        outputStreamWriter.close();
        outputStream.close();
    }

    public static <T> T unmarshal(Class<T> aClass, String string) throws JAXBException {
        StringReader stringReader = new StringReader(string);
        return unmarshal(aClass, stringReader);
    }

    public static <T> T unmarshal(Class<T> aClass, File file) throws JAXBException, IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        T result = unmarshal(aClass, fileInputStream);
        fileInputStream.close();
        return result;
    }

    public static <T> T unmarshal(Class<T> aClass, InputStream inputStream) throws JAXBException, IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        T result = unmarshal(aClass, inputStreamReader);
        inputStreamReader.close();
        return result;
    }

    public static <T> T unmarshal(Class<T> aClass, Reader reader) throws JAXBException {
        JAXBContext jaxbContext = getContext(aClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        @SuppressWarnings("unchecked")
        T result = (T) unmarshaller.unmarshal(reader);
        return result;
    }

    public static <T> void marshal(T object, Writer writer) throws JAXBException {
        marshal(object, writer, false);
    }

    public static <T> void marshal(T object, Writer writer, boolean formated) throws JAXBException {
        JAXBContext jaxbContext = getContext(object.getClass());
        Marshaller marshaller = jaxbContext.createMarshaller();
        if (formated) marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(object, writer);
    }

    private static <T> JAXBContext getContext(Class<T> aClass) throws JAXBException {
        if (contexts.containsKey(aClass)) return contexts.get(aClass);
        else {
            JAXBContext jaxbContext = JAXBContext.newInstance(aClass);
            contexts.put(aClass, jaxbContext);
            return jaxbContext;
        }
    }

    public static <T> boolean validateClass(Class<T> aClass) throws JAXBException {
        getContext(aClass);
        return true;
    }

}