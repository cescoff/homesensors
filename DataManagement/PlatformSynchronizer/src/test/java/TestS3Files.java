import com.amazonaws.services.s3.model.CSVInput;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class TestS3Files {

    public static void main(String[] args) throws IOException {
        final File s3FilesListsFile = new File("/Users/corentin/Downloads/s3_object_keys.txt");
        final File sFAttachements = new File("/Users/corentin/Downloads/Attachments_On_EmailMessage_MarkedAsArchived_2020-2-18_18h06.csv");
        final File sFEmailMessages = new File("/Users/corentin/Downloads/EmailMessage_MarkedAsArchived_2020-2-18_17h46_Backup.csv");

        final File emailMessageIdsToDeleteFile = new File("/Users/corentin/Documents/Customers/Evernex/2020-2-18_Cleanup/ArchivedEmailMessageIds.csv");
        final File attachmentIdsToDeleteFile = new File("/Users/corentin/Documents/Customers/Evernex/2020-2-18_Cleanup/ArchivedAttachmentsIds.csv");

        final CSVParser attachmentsParser = CSVParser.parse(sFAttachements, StandardCharsets.ISO_8859_1, CSVFormat.EXCEL.withHeader().withDelimiter(','));
        final CSVParser emailMessagesParser = CSVParser.parse(sFEmailMessages, StandardCharsets.ISO_8859_1, CSVFormat.EXCEL.withHeader().withDelimiter(','));

        final FileInputStream s3ileInputStream = new FileInputStream(s3FilesListsFile);
        final LineIterator lineIterator = new LineIterator(new InputStreamReader(s3ileInputStream, "UTF-8"));

        final Set<String> emailMessageIds2Delete = Sets.newHashSet();
        final Set<String> attachementIds2Delete = Sets.newHashSet();

        final LineIterator emailMessageIds2DeleteLI = new LineIterator(new InputStreamReader(new FileInputStream(emailMessageIdsToDeleteFile)));
        while (emailMessageIds2DeleteLI.hasNext()) {
            emailMessageIds2Delete.add(StringUtils.remove(emailMessageIds2DeleteLI.nextLine(), "\""));
        }

        final LineIterator attachmentIds2DeleteLI = new LineIterator(new InputStreamReader(new FileInputStream(attachmentIdsToDeleteFile)));
        while (attachmentIds2DeleteLI.hasNext()) {
            attachementIds2Delete.add(StringUtils.remove(attachmentIds2DeleteLI.nextLine(), "\""));
        }



        final Set<String> s3FilePaths = Sets.newHashSet();

        while (lineIterator.hasNext()) {
            final String line = lineIterator.nextLine();
            final String s3Path = StringUtils.substring(line, StringUtils.indexOf(line, "/Email/"));
            s3FilePaths.add(s3Path);
            //System.out.println("Adding s3 path '" + s3Path + "'");
        }

        lineIterator.close();

        System.out.println("S3 File path loaded");

        int missingPDFCount = 0;
        int missingAttachmentCount = 0;

        final Set<String> wrongIds = Sets.newHashSet();

        final FileOutputStream fileOutputStream = new FileOutputStream("/Users/corentin/Documents/Customers/Evernex/missing_pdf.txt");

        Set<String> attachmentsKnownAsToBeDelete = Sets.newHashSet();

        for (final CSVRecord csvRecord : attachmentsParser.getRecords()) {
            final String emailMessageId = csvRecord.get("PARENTID");
            final String fileName = csvRecord.get("NAME");
            final String attachmentId =  csvRecord.get("ID");

            final String expectedFilePath = new StringBuilder("/Email/").append(emailMessageId).append("/Attachments/").append(fileName).toString();

            if (!s3FilePaths.contains(expectedFilePath)) {
//                fileOutputStream.write(new String("\"" + emailMessageId + "\",\"false\"\n").getBytes());
                System.out.println("Missing file '" + expectedFilePath + "' (" + csvRecord.get("CREATEDDATE") + ")");
                wrongIds.add(emailMessageId);
                if (attachementIds2Delete.contains(attachmentId)) {
                    System.out.println("Attachement Id marked as to be delete and should not be '" + attachmentId + "'");
                }
                missingAttachmentCount++;
            } else {
                attachmentsKnownAsToBeDelete.add(attachmentId);
            }

        }

        LocalDateTime minDate = null;
        LocalDateTime maxDate = null;

        Set<String> emailMessageIdsKnownAsToBeDelete = Sets.newHashSet();

        final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
        int numberOfProcesedEmail = 0;
        for (final CSVRecord csvRecord : emailMessagesParser) {
            numberOfProcesedEmail++;
            final String emailMessageId = csvRecord.get("ID");
            final String caseClosedDate = csvRecord.get("CASECLOSEDDATE__C");

            final String expectedFilePath1 = new StringBuilder("/Email/").append(emailMessageId).append("/").append(emailMessageId).append("html.pdf").toString();
            final String expectedFilePath2 = new StringBuilder("/Email/").append(emailMessageId).append("/").append(emailMessageId).append(".pdf").toString();
            if (!s3FilePaths.contains(expectedFilePath1) && !s3FilePaths.contains(expectedFilePath2)) {
                final LocalDateTime localDateTime = dateTimeFormat.parseLocalDateTime(caseClosedDate);
//                fileOutputStream.write(new String("\"" + emailMessageId + "\",\"false\"\n").getBytes());
//                System.out.println("Missing file '" + expectedFilePath + "'");
                wrongIds.add(emailMessageId);
                missingPDFCount++;
                if (missingPDFCount % 100 == 0) {
                    System.out.println("Missing PDF count " + missingPDFCount + "/" + numberOfProcesedEmail);
                }

                if (minDate == null || localDateTime.isBefore(minDate)) {
                    minDate = localDateTime;
                }

                if (maxDate == null || maxDate.isBefore(localDateTime)) {
                    maxDate = localDateTime;
                }
            } else {
                emailMessageIdsKnownAsToBeDelete.add(emailMessageId);
            }
        }

//        final FileOutputStream validEmailMessageIdOut = new FileOutputStream("/Users/corentin/Documents/Customers/Evernex/2020-2-18_Cleanup/ArchivedEmailMessageIds.csv");

        for (final String emailMessageThatWillBeDelete : emailMessageIds2Delete) {
            if (!emailMessageIdsKnownAsToBeDelete.contains(emailMessageThatWillBeDelete) || wrongIds.contains(emailMessageThatWillBeDelete)) {
                System.out.println("Email message id '" + emailMessageThatWillBeDelete + "' will be deleted but is not knwn as to be delete");
            } else {
//                validEmailMessageIdOut.write(new String("\"" + emailMessageThatWillBeDelete + "\"\n").getBytes());
            }
        }
//        validEmailMessageIdOut.close();

 //       final FileOutputStream validAttachmentIdOut = new FileOutputStream("/Users/corentin/Documents/Customers/Evernex/2020-2-18_Cleanup/ArchivedAttachmentsIds.csv");

        for (final String attachmentThatWillBeDelete : attachementIds2Delete) {
            if (!attachmentsKnownAsToBeDelete.contains(attachmentThatWillBeDelete)) {
                System.out.println("Attachment id '" + attachmentThatWillBeDelete + "' will be deleted but is not knwn as to be delete");
            } else {
//                validAttachmentIdOut.write(new String("\"" + attachmentThatWillBeDelete + "\"\n").getBytes());
            }
        }
//        validAttachmentIdOut.close();

        for (final String emailMessageId : wrongIds) {
            fileOutputStream.write(new String("\"" + emailMessageId + "\",\"false\"\n").getBytes());
            if (emailMessageIds2Delete.contains(emailMessageId)) {
                System.out.println("Id marked as delete and should not be '" + emailMessageId + "'");
            }
        }

        fileOutputStream.close();



// 02s0X00001AkGbFQAV
        System.out.println("Email error missing PDF file : " + missingPDFCount);
        System.out.println("Missing PDF min date : " + minDate);
        System.out.println("Missing PDF max date : " + maxDate);
        System.out.println("Attachment error missing attachment : " + missingAttachmentCount);
        System.out.println("Wrong ids : " + wrongIds.size());
    }

}
