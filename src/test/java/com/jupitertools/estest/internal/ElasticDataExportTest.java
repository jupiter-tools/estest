package com.jupitertools.estest.internal;

import java.util.List;
import java.util.Map;
import java.util.UUID;


import com.jupitertools.datasetroll.DataSet;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import static org.assertj.core.api.Assertions.assertThat;

@ElasticsearchTestContainer
@SpringBootTest
class ElasticDataExportTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    private static final Foo FIRST = new Foo(UUID.randomUUID(), "FIRST");
    private static final Foo SECOND = new Foo(UUID.randomUUID(), "SECOND");

    @BeforeEach
    void setUp() {
        // init index & mapping
        elasticsearchTemplate.deleteIndex(Foo.class);
        elasticsearchTemplate.createIndex(Foo.class);
        elasticsearchTemplate.putMapping(Foo.class);
        elasticsearchTemplate.refresh(Foo.class);
        // populate
        elasticsearchTemplate.index(new IndexQueryBuilder().withObject(FIRST).build());
        elasticsearchTemplate.index(new IndexQueryBuilder().withObject(SECOND).build());
        // flush
        elasticsearchTemplate.refresh(Foo.class);
    }

    @Test
    void export() {
        // Arrange
        ElasticDataExport dataExport = new ElasticDataExport(elasticsearchTemplate);
        // Act
        DataSet dataset = dataExport.export();
        // Assert
        List<Map<String, Object>> listOfRecords = dataset.read().get(Foo.class.getName());

        assertThat(listOfRecords)
                .isNotNull()
                .extracting(m -> m.get("name"),
                            m -> m.get("id"))
                .containsExactlyInAnyOrder(Tuple.tuple(FIRST.getName(), FIRST.getId().toString()),
                                           Tuple.tuple(SECOND.getName(), SECOND.getId().toString()));
    }

    @Test
    void classReferenceNameInExportedMap() {
        // Arrange
        ElasticDataExport dataExport = new ElasticDataExport(elasticsearchTemplate);
        // Act
        DataSet dataset = dataExport.export();
        // Assert
        Map<String, List<Map<String, Object>>> map = dataset.read();
        assertThat(map).containsKey("com.jupitertools.estest.internal.ElasticDataExportTest$Foo");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(indexName = "foo")
    private static class Foo {
        @Id
        private UUID id;
        private String name;
    }
}