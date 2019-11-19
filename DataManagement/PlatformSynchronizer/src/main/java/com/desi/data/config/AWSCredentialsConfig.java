package com.desi.data.config;

import com.desi.data.AWSClientId;
import com.desi.data.utils.JAXBUtils;
import com.google.common.collect.Lists;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "aws-credentials")
@XmlAccessorType(XmlAccessType.FIELD)
public class AWSCredentialsConfig {

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

        @XmlAttribute(name = "service")
        private AWSClientId id;

        public Credentials() {
        }

        public Credentials(String accessKey, String secretKey, AWSClientId id) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.id = id;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public AWSClientId getId() {
            return id;
        }
    }

    public static void main(String[] args) throws JAXBException {
        final AWSCredentialsConfig config = new AWSCredentialsConfig();
        config.getCredentials().add(new Credentials("AKIAQ6Y7B4AGQDVWRI6T", "QPVt/0ffXlVKu4DTHaNZoGHRH79Hbk0ScO+dtQ/I", AWSClientId.S3Bridge));
        System.out.println(JAXBUtils.marshal(config, true));
    }

}
