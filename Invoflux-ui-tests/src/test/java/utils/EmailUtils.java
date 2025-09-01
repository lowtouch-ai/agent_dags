package utils;

import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.mail.search.FlagTerm;
import java.io.IOException;

public class EmailUtils {

    private String email;
    private String appPassword;

    public EmailUtils(String email, String appPassword) {
        this.email = email;
        this.appPassword = appPassword;
    }

    /**
     * Send an email with optional attachment
     */
    public void sendEmail(String to, String subject, String body, String attachmentPath) throws Exception {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(email, appPassword);
            }
        });

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(email));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
        message.setSubject(subject);

        // Create message parts
        MimeBodyPart textPart = new MimeBodyPart();
        textPart.setText(body);

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(textPart);

        // Add attachment if provided
        if (attachmentPath != null && !attachmentPath.isEmpty()) {
            MimeBodyPart attachmentPart = new MimeBodyPart();
            attachmentPart.attachFile(attachmentPath);
            multipart.addBodyPart(attachmentPart);
        }

        message.setContent(multipart);
        Transport.send(message);

        System.out.println("Email sent successfully to " + to + " with attachment: " + attachmentPath);
    }

    /**
     * Read the latest unread email content
     */
    public String readLatestUnreadEmail() throws Exception {
        Properties props = new Properties();
        props.put("mail.store.protocol", "imaps");

        Session session = Session.getInstance(props);
        Store store = session.getStore("imaps");
        store.connect("imap.gmail.com", email, appPassword);

        Folder inbox = store.getFolder("INBOX");
        inbox.open(Folder.READ_WRITE);

        // Fetch unread messages
        Message[] messages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));

        if (messages.length == 0) {
            System.out.println("No new unread emails found.");
            return null;
        }

        Message latest = messages[messages.length - 1];

        // Extract actual text content
        String content = getTextFromMessage(latest);

        System.out.println("Received Email: " + content);

        // Mark as read
        latest.setFlag(Flags.Flag.SEEN, true);

        inbox.close(false);
        store.close();

        return content;
    }

    /**
     * Extract text from any Message (plain, HTML, or multipart)
     */
    public static String getTextFromMessage(Message message) throws MessagingException, IOException {
        if (message.isMimeType("text/plain")) {
            return message.getContent().toString();
        } else if (message.isMimeType("text/html")) {
            return stripHtml(message.getContent().toString());
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            return getTextFromMimeMultipart(mimeMultipart);
        }
        return "";
    }

    /**
     * Recursively extract text from MimeMultipart
     */
    private static String getTextFromMimeMultipart(MimeMultipart mimeMultipart) throws MessagingException, IOException {
        StringBuilder result = new StringBuilder();
        int count = mimeMultipart.getCount();
        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            if (bodyPart.isMimeType("text/plain")) {
                result.append(bodyPart.getContent());
            } else if (bodyPart.isMimeType("text/html")) {
                result.append(stripHtml(bodyPart.getContent().toString()));
            } else if (bodyPart.getContent() instanceof MimeMultipart) {
                result.append(getTextFromMimeMultipart((MimeMultipart) bodyPart.getContent()));
            }
        }
        return result.toString();
    }

    /**
     * Simple HTML tag stripper
     */
    private static String stripHtml(String html) {
        return html.replaceAll("\\<.*?\\>", ""); // remove HTML tags
    }
}
