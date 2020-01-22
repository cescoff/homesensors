package com.desi.data.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class JAXBStreamUtils {

    private static Logger logger = LoggerFactory.getLogger(JAXBStreamUtils.class);

    public static <T> Iterable<T> unmarshal(final Class<T> itemClass, final File sourceFile, final String token) throws JAXBException, IOException {
//        final ImmutableList.Builder<T> result = ImmutableList.builder();
//        final String content = readLineByLineJava(sourceFile);

/*        final String startToken1 = "<" + token + " ";
        final String startToken2 = "<" + token + ">";
        final String endToken = "</" + token + ">";

        final FileInputStream fileInputStream = new FileInputStream(sourceFile);
        final LineIterator lineIterator = new LineIterator(new InputStreamReader(fileInputStream));

        try {
        } finally {
            fileInputStream.close();
        }
/*
        int position = 0;

        while (position < content.length()) {
            int nexStart = StringUtils.indexOf(content, "<" + token + " ", position);
            if (nexStart < 0) {
                nexStart = StringUtils.indexOf(content, "<" + token + ">", position);
            }
            if (nexStart >= 0) {
                int nextEnd = StringUtils.indexOf(content, "</" + token + ">", nexStart);
                if (nextEnd >= 0) {
                    String xml = StringUtils.substring(content, nexStart, nextEnd + token.length() + 3);
                    result.add(JAXBUtils.unmarshal(itemClass, xml));
                    position = nextEnd + token.length() + 3;
                } else {
                    throw new IllegalStateException("Malformed xml file '" + sourceFile.getAbsolutePath() + "' at position " + position);
                }
            } else {
                //throw new IllegalStateException("Malformed xml file '" + sourceFile.getAbsolutePath() + "' at position " + position);
                position = content.length();
            }
        }
        return result.build();*/
        return new JAXBIterable<T>(sourceFile, token, itemClass);
    }

    private static class JAXBIterable<T> implements Iterable<T> {

        private final File sourceFile;

        private final String token;

        private final Class<T> unmarshalClass;

        private final String startToken1;
        private final String startToken2;
        private final String endToken;

        private final FileInputStream fileInputStream;
        private final LineIterator lineIterator;

        private T nextValue = null;

        private int lineNumber = 0;

        private JAXBIterable(File sourceFile, String token, Class<T> unmarshalClass) {
            this.sourceFile = sourceFile;
            this.token = token;
            this.unmarshalClass = unmarshalClass;
            this.startToken1 = "<" + token + " ";
            this.startToken2 = "<" + token + ">";
            this.endToken = "</" + token + ">";
            try {
                this.fileInputStream = new FileInputStream(sourceFile);
            } catch (FileNotFoundException e) {

                throw new IllegalStateException("Cannot open file '" + sourceFile + "'", e);
            }
            this.lineIterator = new LineIterator(new InputStreamReader(fileInputStream));
            this.nextValue = nextValue();
        }



        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return nextValue != null;
                }

                @Override
                public T next() {
                    final T result = nextValue;
                    nextValue = nextValue();
                    return result;
                }
            };
        }

        @Override
        public void forEach(Consumer<? super T> action) {
            if (nextValue == null) {
                return;
            }
            action.accept(nextValue);
            nextValue = nextValue();
        }

        @Override
        public Spliterator<T> spliterator() {
            return new Spliterator<T>() {
                @Override
                public boolean tryAdvance(Consumer<? super T> action) {
                    return false;
                }

                @Override
                public Spliterator<T> trySplit() {
                    return null;
                }

                @Override
                public long estimateSize() {
                    return 0;
                }

                @Override
                public int characteristics() {
                    return 0;
                }
            };
        }

        private T nextValue() {
            StringBuilder contentBuilder = new StringBuilder();
            boolean started = false;
            while (lineIterator.hasNext()) {
                lineNumber++;
                final String line = lineIterator.nextLine();
                if (StringUtils.contains(line, startToken1) || StringUtils.contains(line, startToken2)) {
                    started = true;
                }
                if (started) {
                    contentBuilder.append(line);
                }
                if (StringUtils.contains(line, endToken)) {
                    try {
                        return JAXBUtils.unmarshal(unmarshalClass, contentBuilder.toString());
                    } catch (JAXBException e) {
                        logger.error("Cannot unmarshall xml split at line " + lineNumber + " in file " + this.sourceFile.getAbsolutePath() + "'", e);
                        return null;
                    }
                }
            }
            return null;
        }

    }

    private static String readLineByLineJava(File aFile) {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(aFile.toURI()), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }

}
