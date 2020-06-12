package com.jupitertools.estest.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


import com.google.common.collect.ImmutableMap;
import com.jupitertools.datasetroll.DataSet;
import com.jupitertools.springtestelasticsearch.ElasticsearchTestContainer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import static org.assertj.core.api.Assertions.assertThat;


@ElasticsearchTestContainer
@SpringBootTest
class ElasticsearchDataImportTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @BeforeEach
    void setUp() {
        elasticsearchTemplate.deleteIndex(Bar.class);
        elasticsearchTemplate.createIndex(Bar.class);
        elasticsearchTemplate.putMapping(Bar.class);
        elasticsearchTemplate.refresh(Bar.class);
    }

    @Test
    void importDataSetWithAllFields() {
        // Act
        new ElasticsearchDataImport(elasticsearchTemplate).importFrom(getDataSet());
        // Assert
        readAndAssertBar("101", "data-1");
        readAndAssertBar("102", "data-2");
    }

    @Test
    void importPartialDataSet() {
        // Act
        new ElasticsearchDataImport(elasticsearchTemplate).importFrom(getPartialDataSet());
        // Assert
        NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchAllQuery())
                                                                .build();
        List<Bar> list = elasticsearchTemplate.queryForList(query, Bar.class);
        assertThat(list).isNotNull()
                        .hasSize(2)
                        .extracting(Bar::getData)
                        .containsOnly("data-1", "data-2");
    }

    @Test
    void wrongDataSetFormat() {
        // Arrange
        ImmutableMap<String, Object> notBar = ImmutableMap.of("uniqueField", "05111987");

        DataSet dataSet = () -> ImmutableMap.of(Bar.class.getName(),
                                                Collections.singletonList(notBar));

        ElasticsearchDataImport dataImport = new ElasticsearchDataImport(elasticsearchTemplate);

        // Act & Assert
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            dataImport.importFrom(dataSet);
        });
    }

    private void readAndAssertBar(String id, String expectedData) {

        GetQuery getQuery = GetQuery.getById(id);
        Bar bar = elasticsearchTemplate.queryForObject(getQuery, Bar.class);
        assertThat(bar).isNotNull()
                       .extracting(Bar::getId, Bar::getData)
                       .containsOnly(id, expectedData);
    }

    private DataSet getDataSet() {

        ImmutableMap<String, Object> bar1 = ImmutableMap.of("id", "101", "data", "data-1");
        ImmutableMap<String, Object> bar2 = ImmutableMap.of("id", "102", "data", "data-2");

        Map<String, List<Map<String, Object>>> map =
                ImmutableMap.of(Bar.class.getName(), Arrays.asList(bar1, bar2));

        return () -> map;
    }

    private DataSet getPartialDataSet() {
        ImmutableMap<String, Object> bar1 = ImmutableMap.of("data", "data-1");
        ImmutableMap<String, Object> bar2 = ImmutableMap.of("data", "data-2");

        Map<String, List<Map<String, Object>>> map =
                ImmutableMap.of(Bar.class.getName(), Arrays.asList(bar1, bar2));

        return () -> map;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(indexName = "bar")
    private static class Bar {
        @Id
        private String id;
        private String data;
    }
}