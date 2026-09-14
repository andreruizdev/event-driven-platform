package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.StringUtils;
import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Configuration
public class DatabaseConfig {

    @Value("${eventdrivenmicroservices.database.mode:local}")
    private String databaseMode;

    @Value("${spring.r2dbc.url:}")
    private String springR2dbcUrl;

    @Value("${spring.r2dbc.username:}")
    private String springR2dbcUser;

    @Value("${spring.r2dbc.password:}")
    private String springR2dbcPassword;

    @Value("${eventdrivenmicroservices.database.local.url:jdbc:postgresql://localhost:5432/eventdrivenmicroservices_tx}")
    private String localUrl;

    @Value("${eventdrivenmicroservices.database.local.username:eventdrivenmicroservices_admin}")
    private String localUsername;

    @Value("${eventdrivenmicroservices.database.local.password:local-dev-password}")
    private String localPassword;

    @Value("${eventdrivenmicroservices.database.local.driver-class-name:org.postgresql.Driver}")
    private String localDriver;

    @Value("${eventdrivenmicroservices.database.remote.url:jdbc:postgresql://localhost:5432/eventdrivenmicroservices_tx}")
    private String remoteUrl;

    @Value("${eventdrivenmicroservices.database.remote.username:eventdrivenmicroservices_admin}")
    private String remoteUsername;

    @Value("${eventdrivenmicroservices.database.remote.password:}")
    private String remotePassword;

    @Bean
    @Primary
    public ConnectionFactory connectionFactory() {
        boolean isRemote = "remote".equalsIgnoreCase(databaseMode);
        
        String r2dbcUrl;
        String user;
        String password;

        if (StringUtils.hasText(springR2dbcUrl)) {
            // Respect Spring R2DBC configuration / DynamicPropertySource for Testcontainers
            r2dbcUrl = springR2dbcUrl;
            user = StringUtils.hasText(springR2dbcUser) ? springR2dbcUser : localUsername;
            password = StringUtils.hasText(springR2dbcPassword) ? springR2dbcPassword : localPassword;
        } else {
            String url = isRemote ? remoteUrl : localUrl;
            r2dbcUrl = url.startsWith("r2dbc:") ? url : url.replace("jdbc:", "r2dbc:");
            user = isRemote ? remoteUsername : localUsername;
            password = isRemote ? remotePassword : localPassword;
        }
        
        ConnectionFactoryOptions.Builder optionsBuilder = ConnectionFactoryOptions.parse(r2dbcUrl).mutate();
        if (StringUtils.hasText(user)) {
            optionsBuilder.option(USER, user);
        }
        if (StringUtils.hasText(password)) {
            optionsBuilder.option(PASSWORD, password);
        }
        
        ConnectionFactory factory = ConnectionFactories.get(optionsBuilder.build());
        
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(factory)
                .maxSize(10)
                .initialSize(2)
                .maxIdleTime(Duration.ofMinutes(30))
                .build();
                
        return new ConnectionPool(poolConfig);
    }
}
