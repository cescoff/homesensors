package com.desi.data.config;

import com.desi.data.PlatformClientId;
import com.desi.data.utils.JAXBUtils;
import com.google.common.collect.Lists;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "platform-credentials")
@XmlAccessorType(XmlAccessType.FIELD)
public class PlatformCredentialsConfig {

    @XmlElementWrapper(name = "services")
    @XmlElement(name = "credentials")
    private List<Credentials> credentials = Lists.newArrayList();

    public List<Credentials> getCredentials() {
        return credentials;
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Credentials {

        @XmlElement(name = "access-key")
        private String accessKey;

        @XmlElement(name = "secret-key")
        private String secretKey;

        @XmlElement(name = "key-file-path")
        private String keyFilePath;

        @XmlAttribute(name = "service")
        private PlatformClientId id;

        public Credentials() {
        }

        public Credentials(String accessKey, String secretKey, PlatformClientId id) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.id = id;
        }

        public Credentials(String keyFilePath, PlatformClientId id) {
            this.keyFilePath = keyFilePath;
            this.id = id;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public String getKeyFilePath() { return keyFilePath; }

        public PlatformClientId getId() {
            return id;
        }
    }

    public static void main(String[] args) throws JAXBException {
        final PlatformCredentialsConfig config = new PlatformCredentialsConfig();
        config.getCredentials().add(new Credentials("<KEY>", "<SECRET>", PlatformClientId.S3Bridge));
        System.out.println(JAXBUtils.marshal(config, true));
    }

}
