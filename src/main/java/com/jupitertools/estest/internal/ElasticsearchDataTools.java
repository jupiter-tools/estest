package com.jupitertools.estest.internal;

import java.util.Map;
import java.util.Set;


import com.google.common.collect.Sets;
import com.jupitertools.datasetroll.DataSet;
import com.jupitertools.datasetroll.expect.MatchDataSets;
import com.jupitertools.datasetroll.expect.dynamic.value.DateDynamicValue;
import com.jupitertools.datasetroll.expect.dynamic.value.DynamicDataSet;
import com.jupitertools.datasetroll.expect.dynamic.value.DynamicValue;
import com.jupitertools.datasetroll.expect.dynamic.value.GroovyDynamicValue;
import com.jupitertools.datasetroll.expect.dynamic.value.JavaScriptDynamicValue;
import com.jupitertools.datasetroll.exportdata.ExportFile;
import com.jupitertools.datasetroll.exportdata.JsonExport;
import com.jupitertools.datasetroll.exportdata.scanner.AnnotatedDocumentScanner;
import com.jupitertools.datasetroll.importdata.ImportFile;
import com.jupitertools.datasetroll.importdata.JsonImport;

import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

/**
 * Created on 25/11/2019
 * <p>
 * The entry point to work with ElasticSearch in junit5 extensions.
 * This class provide an abilities to export/import/expect data sets.
 *
 * @author Korovin Anatoliy
 */
public class ElasticsearchDataTools {

    //TODO: extract all public methods from this class to the separate interface
    // and use it in different implementations such as mogodb, elastic, rabbitmq, e.t.c.

    private final ElasticsearchTemplate elasticsearchTemplate;

    public ElasticsearchDataTools(ElasticsearchTemplate elasticsearchTemplate) {
        this.elasticsearchTemplate = elasticsearchTemplate;
    }

    /**
     * export data from the Elasticsearch index to selected file
     *
     * @param fileName path to the export file
     */
    public void exportTo(String fileName) {
        new ExportFile(new JsonExport(new ElasticDataExport(this.elasticsearchTemplate).export())).write(fileName);
    }

    /**
     * import data from file to the Elasticsearch index
     *
     * @param fileName path to file with the data set.
     */
    public void importFrom(String fileName) {

        DynamicDataSet dynamicDataSet =
                new DynamicDataSet(new JsonImport(new ImportFile(fileName)),
                                   getDynamicEvaluators());

        new ElasticsearchDataImport(elasticsearchTemplate).importFrom(dynamicDataSet);
    }

    /**
     * Check data in the Elasticsearch database,
     * try to match data from the DB with a loaded from file data set.
     *
     * @param fileName path to file with an expected data set
     */
    public void expect(String fileName) {
        DataSet expectedDataSet = new DynamicDataSet(new JsonImport(new ImportFile(fileName)), getDynamicEvaluators());
        DataSet actualDataSet = new ElasticDataExport(elasticsearchTemplate).export();
        new MatchDataSets(actualDataSet, expectedDataSet).check();
    }

    /**
     * Clean whole elasticsearch database.
     * Be careful this method drop all collections in the database.
     */
    public void cleanDataBase() {
        // find all indexes:
        Map<String, Class<?>> indexes =
                new AnnotatedDocumentScanner("").scan(Document.class)
                                                .mapByAnnotationAttr(Document.class, Document::indexName);
        // drop all founded indexes:
        indexes.forEach((k, v) -> {
            elasticsearchTemplate.deleteIndex(v);
            elasticsearchTemplate.createIndex(v);
            elasticsearchTemplate.putMapping(v);
            elasticsearchTemplate.refresh(v);
        });
    }

    // TODO: should be a mechanics to add one more evaluators
    //  in runtime (something like DI...)
    private Set<DynamicValue> getDynamicEvaluators() {
        return Sets.newHashSet(new GroovyDynamicValue(),
                               new JavaScriptDynamicValue(),
                               new DateDynamicValue());
    }
}
