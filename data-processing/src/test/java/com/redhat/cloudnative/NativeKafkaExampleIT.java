package com.redhat.cloudnative;

import io.quarkus.test.junit.NativeImageTest;

@NativeImageTest
public class NativeKafkaExampleIT extends KafkaExampleTest {

    // Execute the same tests but in native mode.
}