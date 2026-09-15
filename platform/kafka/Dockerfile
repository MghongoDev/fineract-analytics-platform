# =====================================================================
# Kafka Connect (Debezium) with the JMX Prometheus javaagent baked in.
#
# Debezium publishes its operational metrics only over JMX; the stock
# debezium/connect image does not ship the jmx_prometheus_javaagent.jar,
# so without this wrapper image the `kafka-connect` scrape target in
# prometheus.yml (port 9404) serves nothing and the KafkaConnectDown
# alert fires forever.
#
# Ownership notes:
#   * The base image runs as USER kafka (uid 1001), so downloading the
#     agent as `root` here and then restoring that user is required -
#     a file ADD/COPYed in would otherwise be root-owned and the non-root
#     kafka user would fail to chmod it (Operation not permitted).
# =====================================================================
FROM debezium/connect:2.7.3.Final

ARG JMX_AGENT_VERSION=0.20.0

USER root

# curl rather than ADD with a URL: explicit failure propagation
# (curl -f), and we can set ownership in the same step.
RUN curl -fsSL \
        -o /kafka/jmx_prometheus_javaagent.jar \
        "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar" \
    && chmod 644 /kafka/jmx_prometheus_javaagent.jar \
    && chown kafka:kafka /kafka/jmx_prometheus_javaagent.jar \
    && ls -l /kafka/jmx_prometheus_javaagent.jar

# Restore the base image's runtime user.
USER kafka