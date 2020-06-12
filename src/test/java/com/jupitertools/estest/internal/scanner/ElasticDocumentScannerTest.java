package com.jupitertools.estest.internal.scanner;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.elasticsearch.annotations.Document;

import static org.assertj.core.api.Assertions.assertThat;

class ElasticDocumentScannerTest {

    @Test
    void name() {
        // Arrange
        ElasticDocumentScanner documentScanner =
                new ElasticDocumentScanner(this.getClass().getPackage().getName());
        // Act
        Map<String, Class<?>> documents = documentScanner.scan();
        // Assert
        assertThat(documents).hasSize(2)
                             .containsEntry("com.jupitertools.estest.internal.scanner.ElasticDocumentScannerTest$ElasticTestDocFirst", ElasticTestDocFirst.class)
                             .containsEntry("com.jupitertools.estest.internal.scanner.ElasticDocumentScannerTest$ElasticTestDocSecond", ElasticTestDocSecond.class);
    }

    @Document(indexName = "antkorwin-elastic-test-doc-first")
    private class ElasticTestDocFirst {

    }

    @Document(indexName = "antkorwin-elastic-test-doc-second")
    private class ElasticTestDocSecond {

    }
}