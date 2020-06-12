package com.jupitertools.estest.junit5;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.UUID;

import com.jupitertools.datasetroll.exportdata.scanner.AnnotatedDocumentScanner;
import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.annotation.ExpectedElasticDataSet;
import com.jupitertools.estest.annotation.ExportElasticDataSet;
import com.jupitertools.estest.internal.ElasticsearchDataTools;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Created on 11/12/2019
 * <p>
 * Junit5 extension to easy work with elasticsearch data sets in your tests.
 *
 * @author Korovin Anatoliy
 */
public class ElasticDataSetExtension implements Extension,
                                                BeforeAllCallback,
                                                BeforeEachCallback,
                                                AfterEachCallback {

	private ElasticsearchTemplate elasticTemplate;
	private ElasticsearchDataTools elasticsearchDataTools;

	public static final ExtensionContext.Namespace NAMESPACE =
			ExtensionContext.Namespace.create("com", "jupiter-tools", "estest", "read-only-dataset");

	/**
	 * check existence of the {@link ElasticsearchTemplate} in the context
	 *
	 * @param context junit5 extension context
	 */
	@Override
	public void beforeAll(ExtensionContext context) throws Exception {

		this.elasticTemplate = SpringExtension.getApplicationContext(context)
		                                      .getBean(ElasticsearchTemplate.class);

		if (elasticTemplate == null) {
			throw new Error("Unable to load ElasticsearchTemplate from the application context");
		}

		this.elasticsearchDataTools = new ElasticsearchDataTools(elasticTemplate);
	}

	/**
	 * Clean a database before the test execution if it needed,
	 * then populate data-set from the file.
	 */
	@Override
	public void beforeEach(ExtensionContext context) throws Exception {

		ElasticDataSet elasticDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);

		if (elasticDataSet == null) {
			return;
		}

		if (elasticDataSet.cleanBefore()) {
			elasticsearchDataTools.cleanDataBase();
		}

		// populate before test invocation
		if (!elasticDataSet.value().isEmpty()) {
			elasticsearchDataTools.importFrom(elasticDataSet.value());
		}

		// if read-only data set is on than we need to save a DB state in temp file before run tests
		if (isReadOnlyDataSet(context)) {
			File tempFile = File.createTempFile("estest-readonly-",  UUID.randomUUID().toString());
			tempFile.deleteOnExit();
			elasticsearchDataTools.exportTo(tempFile.getAbsolutePath());
			context.getStore(NAMESPACE).put("beforeDataSetFile", tempFile.getAbsolutePath());
		}
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		expected(context);
		exportDataSet(context);
		cleanAfter(context);
	}

	//region Expected
	private void expected(ExtensionContext context) {
		if (isReadOnlyDataSet(context)) {
			expectedReadOnly(context);
			return;
		}
		expectedDataSet(context);
	}

	private void expectedReadOnly(ExtensionContext context) {
		String filePath = (String) context.getStore(NAMESPACE)
		                                  .get("beforeDataSetFile");
		try {
			elasticsearchDataTools.expect(filePath);
		} catch (Error e) {
			throw new RuntimeException("Expected ReadOnly dataset, but found some modifications:", e);
		}
	}

	private void expectedDataSet(ExtensionContext context) {
		ExpectedElasticDataSet expectedElasticDataSet = getAnnotationFromCurrentMethod(context, ExpectedElasticDataSet.class);
		if (expectedElasticDataSet == null) {
			return;
		}
		elasticsearchDataTools.expect(expectedElasticDataSet.value());
	}
	//endregion Expected

	private void exportDataSet(ExtensionContext context) {
		ExportElasticDataSet exportElasticDataSet = getAnnotationFromCurrentMethod(context, ExportElasticDataSet.class);
		if (exportElasticDataSet == null) {
			return;
		}
		elasticsearchDataTools.exportTo(exportElasticDataSet.outputFile());
	}

	private void cleanAfter(ExtensionContext context) {
		ElasticDataSet elasticDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);
		if (elasticDataSet != null && elasticDataSet.cleanAfter()) {
			elasticsearchDataTools.cleanDataBase();
		}
	}

	private <AnnotationT extends Annotation> AnnotationT getAnnotationFromCurrentMethod(ExtensionContext context,
	                                                                                    Class<AnnotationT> annotationClass) {
		return context.getRequiredTestMethod()
		              .getAnnotation(annotationClass);
	}

	private boolean isReadOnlyDataSet(ExtensionContext context) {
		ElasticDataSet elasticDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);
		return elasticDataSet != null && elasticDataSet.readOnly();
	}
}
