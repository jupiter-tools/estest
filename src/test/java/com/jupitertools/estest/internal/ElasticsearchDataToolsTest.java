package com.jupitertools.estest.internal;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ElasticsearchTestContainer
class ElasticsearchDataToolsTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Nested
    class ExportTests {

        private static final String OUTPUT_FILE_NAME = "./target/ElasticsearchDataToolsTest-ExportTests.json";
        private static final String EXPECTED_RESULT = "/dataset/expected_export_result.json";

        @BeforeEach
        void setUp() {
            dropIndex(Foo.class);
            dropIndex(Bar.class);
        }

        @AfterEach
        void tearDown() {
            dropIndex(Foo.class);
            dropIndex(Bar.class);
        }

        @Test
        void export() throws IOException {
            // Arrange
            Files.deleteIfExists(Paths.get(OUTPUT_FILE_NAME));
            populate();
            // Act
            new ElasticsearchDataTools(elasticsearchTemplate).exportTo(OUTPUT_FILE_NAME);
            // Assert
            String result = getResultFromFile();
            assertThat(result).isEqualTo(getExpectedResult());
        }

        private void populate() {
            Bar a = new Bar(UUID.fromString("c764106e-538e-4b66-bc87-a53be548f2da"), "AAAA");
            Bar b = new Bar(UUID.fromString("3bae2a7b-7dc7-4344-820e-7d97a4d04b3c"), "BB");
            elasticsearchTemplate.index(new IndexQueryBuilder().withObject(a).build());
            elasticsearchTemplate.index(new IndexQueryBuilder().withObject(b).build());
            elasticsearchTemplate.refresh(Bar.class);
        }

        private String getExpectedResult() throws IOException {
            try (InputStream inputStream = getClass().getResourceAsStream(EXPECTED_RESULT)) {
                return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            }
        }

        private String getResultFromFile() throws IOException {
            try (InputStream inputStream = new FileInputStream(OUTPUT_FILE_NAME)) {
                return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            }
        }
    }

    @Nested
    class ImportTests {

        @BeforeEach
        void setUp() {
            dropIndex(Foo.class);
            dropIndex(Bar.class);
        }

        @AfterEach
        void tearDown() {
            dropIndex(Foo.class);
            dropIndex(Bar.class);
        }

        @Test
        void importSimpleDataSet() {
            // Act
            new ElasticsearchDataTools(elasticsearchTemplate).importFrom("/dataset/simple.json");
            // Assert
            List<Bar> documents = getAllDocuments(Bar.class);
            assertThat(documents).isNotNull()
                                 .hasSize(2)
                                 .extracting(Bar::getName)
                                 .containsOnly("FIRST-ONE", "SECOND-ONE");
        }

        @Test
        void importDynamicDates() {
            // Arrange
            Date before = new Date();
            Date beforePlus3Days = new Date(before.getTime() + TimeUnit.DAYS.toMillis(3));
            // Act
            new ElasticsearchDataTools(elasticsearchTemplate).importFrom("/dataset/dynamic_with_dates_it.json");
            // Asserts documents import
            List<Foo> documents = getAllDocuments(Foo.class);
            assertThat(documents).hasSize(2);
            // Assert dates
            Date after = new Date();
            Date afterPlus3Days = new Date(after.getTime() + TimeUnit.DAYS.toMillis(3));

            Foo foo1 = elasticsearchTemplate.queryForObject(GetQuery.getById("1"), Foo.class);
            assertThat(foo1.getTime()).isBetween(before, after, true, true);

            Foo foo2 = elasticsearchTemplate.queryForObject(GetQuery.getById("2"), Foo.class);
            assertThat(foo2.getTime()).isBetween(beforePlus3Days, afterPlus3Days, true, true);
        }

        @Test
        void importWithGroovyDataValues() {
            // Arrange
            Date before = new Date();
            // Act
            new ElasticsearchDataTools(elasticsearchTemplate).importFrom("/dataset/dynamic_groovy_it.json");
            // Asserts
            List<Foo> documents = getAllDocuments(Foo.class);
            assertThat(documents).hasSize(1);
            assertThat(documents.get(0).getId()).isEqualTo("8");
            assertThat(documents.get(0).getTime()).isAfterOrEqualsTo(before);
            assertThat(documents.get(0).getCounter()).isEqualTo(55);
        }


        private <DocumentTypeT> List<DocumentTypeT> getAllDocuments(Class<DocumentTypeT> type) {

            NativeSearchQuery query =
                    new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchAllQuery())
                                                  .build();

            return elasticsearchTemplate.queryForList(query, type);
        }
    }


    private void dropIndex(Class<?> indexClassType) {
        System.out.println("DROP: " + indexClassType.getName());
        elasticsearchTemplate.deleteIndex(indexClassType);
        elasticsearchTemplate.createIndex(indexClassType);
        elasticsearchTemplate.putMapping(indexClassType);
        elasticsearchTemplate.refresh(indexClassType);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(indexName = "elasticsearch-datatoolstest-bar")
    private static class Bar {
        @Id
        private UUID id;
        private String name;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    @Document(indexName = "elasticsearch-datatoolstest-foo")
    private static class Foo {
        @Id
        private String id;
        private Date time;
        private int counter;
    }
}