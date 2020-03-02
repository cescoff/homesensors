import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

public class TestMailArchiver {

    public static void main(String[] args) throws Exception {
        final File emailMessagesFile = new File("/Users/corentin/Documents/Customers/Evernex/DeleteJob/EmailMessages.csv");
        final File archivedMailFile = new File("/Users/corentin/Documents/Customers/Evernex/DeleteJob/EmailArchives.csv");
        final File attachmentsFile = new File("/Users/corentin/Documents/Customers/Evernex/DeleteJob/Attachments.csv");

        final Set<String> uniqueEmailMessageIds = Sets.newHashSet();

        final LineIterator emailMessagesLineIterator = new LineIterator(new InputStreamReader(new FileInputStream(emailMessagesFile)));

        while (emailMessagesLineIterator.hasNext()) {
            uniqueEmailMessageIds.add(StringUtils.remove(emailMessagesLineIterator.nextLine(), '"'));
        }

        System.out.println("found " + uniqueEmailMessageIds.size() + " email ids");

        final LineIterator archivedEmailMessagesLineIterator = new LineIterator(new InputStreamReader(new FileInputStream(archivedMailFile)));

        final Set<String> joinArchivedMailIds = Sets.newHashSet();
        while (archivedEmailMessagesLineIterator.hasNext()) {
            final String id = StringUtils.remove(archivedEmailMessagesLineIterator.nextLine(), '"');
            if (uniqueEmailMessageIds.contains(id)) {
                joinArchivedMailIds.add(id);
            }
        }

        System.out.println("found " + joinArchivedMailIds.size() + " matching email ids");

        final LineIterator attachmentsLineIterator = new LineIterator(new InputStreamReader(new FileInputStream(attachmentsFile)));

        final List<String> matchingAttachmentsEmailIds = Lists.newArrayList();
        while (attachmentsLineIterator.hasNext()) {
            final String emailMessageId = StringUtils.remove(attachmentsLineIterator.nextLine(), '"');
            if (joinArchivedMailIds.contains(emailMessageId)) {
                matchingAttachmentsEmailIds.add(emailMessageId);
            }
        }

        System.out.println("found " + matchingAttachmentsEmailIds.size() + " attachments");
    }

}
