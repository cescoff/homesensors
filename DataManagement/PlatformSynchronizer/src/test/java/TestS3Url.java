import org.apache.commons.lang.StringUtils;

import java.net.URLEncoder;

public class TestS3Url {

    // https://signin.aws.amazon.com/signin?redirect_uri=
    // https://066100256781.signin.aws.amazon.com/console

    private static final String ORG_CONSOLE_SIGNIN_SIGNIN_URL = "https://066100256781.signin.aws.amazon.com/console/signin";
    private static final String ORG_CONSOLE_SIGNIN_URL = "https://066100256781.signin.aws.amazon.com/console";
    private static final String ORG_SIGNIN_SIGNIN_URL = "https://066100256781.signin.aws.amazon.com/signin";

    private static final String RESOURCE_URL = "https://s3.console.aws.amazon.com/s3/object/evernex-email-archives-prod/2018/8/31/00011964/Email/02s0X00001Baq0fQAB/Attachments/image001.png";

    // https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fs3.console.aws.amazon.com%2Fs3%2Fobject%2Fevernex-email-archives-prod%2F2018%2F8%2F31%2F00011964%2FEmail%2F02s0X00001Baq0fQAB%2FAttachments%2Fimage001.png%3Fstate%3DhashArgs%2523%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fs3&forceMobileApp=0&account=066100256781

    private static final String OAUTH_URL = "https://signin.aws.amazon.com/oauth?redirect_uri=<S3_URL>&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fs3&response_type=code&iam_user=true&account=desi-sas";

    // https://signin.aws.amazon.com/oauth?redirect_uri=https%3A%2F%2Fs3.console.aws.amazon.com%2Fs3%2Fobject%2Fevernex-email-archives-prod%2F2018%2F8%2F31%2F00011964%2FEmail%2F02s0X00001Baq0fQAB%2FAttachments%2Fimage001.png%3Fstate%3DhashArgs%2523%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fs3&response_type=code&iam_user=true&account=desi-sas

    public static void main(String[] args) throws Exception {
        System.out.println(ORG_CONSOLE_SIGNIN_URL + "?redirect_uri=" + URLEncoder.encode(RESOURCE_URL, "UTF-8"));
        System.out.println(ORG_SIGNIN_SIGNIN_URL + "?redirect_uri=" + URLEncoder.encode(RESOURCE_URL, "UTF-8"));
        System.out.println(ORG_CONSOLE_SIGNIN_SIGNIN_URL + "?redirect_uri=" + URLEncoder.encode(RESOURCE_URL, "UTF-8"));
        System.out.println(ORG_CONSOLE_SIGNIN_SIGNIN_URL + "?redirect_uri=" + URLEncoder.encode(RESOURCE_URL, "UTF-8"));
        System.out.println(StringUtils.replace(OAUTH_URL, "<S3_URL>", URLEncoder.encode(RESOURCE_URL, "UTF-8")));
    }


}
