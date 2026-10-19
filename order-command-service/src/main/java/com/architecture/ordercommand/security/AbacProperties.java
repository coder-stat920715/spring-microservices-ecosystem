package com.architecture.ordercommand.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.LocalTime;
import java.util.List;

/** ABAC policy knobs, bound from the "abac.*" properties (config-server managed). */
@ConfigurationProperties(prefix = "abac")
public record AbacProperties(List<String> allowedLocations, LocalTime windowStart, LocalTime windowEnd, String zone) {
}
