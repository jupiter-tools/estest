package com.jupitertools.estest.internal;

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
import com.jupitertools.datasetroll.importdata.ImportFile;
import com.jupitertools.datasetroll.importdata.JsonImport;

import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

/**
 * Created on 25/11/2019
 * <p>
 * TODO: replace on the JavaDoc
 *
 * @author Korovin Anatoliy
 */
public class ElasticsearchDataTools {

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
     * Check data in the mongodb,
     * try to match data from the DB to loaded from file data set.
     *
     * @param fileName path to file with an expected data set
     */
    public void expect(String fileName) {
        DataSet dataSet = new DynamicDataSet(new JsonImport(new ImportFile(fileName)), getDynamicEvaluators());
        DataSet mongoData = new ElasticDataExport(elasticsearchTemplate).export();
        new MatchDataSets(mongoData, dataSet).check();
    }

    private Set<DynamicValue> getDynamicEvaluators() {
        return Sets.newHashSet(new GroovyDynamicValue(),
                               new JavaScriptDynamicValue(),
                               new DateDynamicValue());
    }
}
