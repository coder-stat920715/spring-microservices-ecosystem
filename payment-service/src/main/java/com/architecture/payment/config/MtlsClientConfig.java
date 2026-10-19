package com.architecture.payment.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.web.client.RestClientSsl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

/**
 * RestClient for the payment provider. When payment.provider.ssl-bundle names an SSL bundle
 * (spring.ssl.bundle.pem.*), the client presents its X.509 certificate and validates the
 * provider against the bundle's trust store, i.e. mutual TLS.
 */
@Configuration
public class MtlsClientConfig {

    @Bean
    RestClient paymentProviderRestClient(RestClient.Builder builder, RestClientSsl ssl,
                                         @Value("${payment.provider.base-url}") String baseUrl,
                                         @Value("${payment.provider.ssl-bundle:}") String bundle) {
        RestClient.Builder b = builder.baseUrl(baseUrl);
        if (!bundle.isBlank()) {
            b = b.apply(ssl.fromBundle(bundle));
        }
        return b.build();
    }
}
