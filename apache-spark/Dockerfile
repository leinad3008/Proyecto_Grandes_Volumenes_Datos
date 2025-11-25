FROM docker.io/apache/spark:4.0.1-python3

USER root

# Directory to hold additional jars
RUN mkdir -p /opt/spark/aws

# Versions for Hadoop AWS and AWS SDK
ENV HADOOP_AWS_VERSION=3.4.1

# Use this version of AWS SDK v2 compatible with Hadoop 3.4.1 and Spark 4.0.1
ENV AWS_SDK_VERSION=2.25.53

# Download the Hadoop AWS jar
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        -O /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

# Download the AWS SDK v2 jar
RUN wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar \
    -O /opt/spark/aws/aws-sdk-bundle-${AWS_SDK_VERSION}.jar

# Fix ownership for Spark user (UID 185)
RUN chown -R 185:185 /opt/spark/aws

# Set ownership back to spark user
USER 185
