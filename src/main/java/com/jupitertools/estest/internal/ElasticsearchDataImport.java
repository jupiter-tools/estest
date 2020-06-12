package com.jupitertools.estest.internal;

import java.util.List;
import java.util.Map;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.jupitertools.datasetroll.DataSet;
import com.jupitertools.datasetroll.importdata.DataSetImport;
import com.jupitertools.datasetroll.tools.ClassByReference;

import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

/**
 * Created on 27/11/2019
 * <p>
 * Import data from {@link DataSet} to the ElasticSearch database.
 *
 * @author Korovin Anatoliy
 */
public class ElasticsearchDataImport implements DataSetImport {

    private final ElasticsearchTemplate elasticsearchTemplate;
    private final ObjectMapper objectMapper;

    public ElasticsearchDataImport(ElasticsearchTemplate elasticsearchTemplate) {
        this.elasticsearchTemplate = elasticsearchTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void importFrom(DataSet dataSet) {
        dataSet.read()
               .forEach((key, value) -> {
                   importDocumentCollection(new ClassByReference(key).get(), value);
               });
    }

    private <DocumentTypeT> void importDocumentCollection(Class<DocumentTypeT> documentClass,
                                                          List<Map<String, Object>> recordCollection) {
        recordCollection.forEach(document -> {
            DocumentTypeT convertedDocument = objectMapper.convertValue(document, documentClass);
            elasticsearchTemplate.index(new IndexQueryBuilder().withObject(convertedDocument).build());
        });
        elasticsearchTemplate.refresh(documentClass);
    }

}
