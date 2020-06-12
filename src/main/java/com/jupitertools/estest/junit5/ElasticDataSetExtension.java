package com.jupitertools.estest.junit5;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.Map;

import com.jupitertools.estest.annotation.ElasticDataSet;
import com.jupitertools.estest.annotation.ExpectedElasticDataSet;
import com.jupitertools.estest.annotation.ExportElasticDataSet;
import com.jupitertools.estest.internal.ElasticsearchDataTools;
import com.jupitertools.datasetroll.exportdata.scanner.AnnotatedDocumentScanner;
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
 * TODO: replace on the JavaDoc
 *
 * @author Korovin Anatoliy
 */
public class ElasticDataSetExtension implements Extension,
                                                BeforeAllCallback,
                                                BeforeEachCallback,
                                                AfterEachCallback {

	private ElasticsearchTemplate elasticTemplate;

	public static final ExtensionContext.Namespace NAMESPACE =
			ExtensionContext.Namespace.create("com", "jupiter-tools", "spring-test-mongo", "read-only-dataset");

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
	}

	/**
	 * Clean a database before the test execution if it needed,
	 * then populate data-set from the file.
	 */
	@Override
	public void beforeEach(ExtensionContext context) throws Exception {

		ElasticDataSet mongoDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);

		if (mongoDataSet == null) {
			return;
		}

		if (mongoDataSet.cleanBefore()) {
			cleanDataBase();
		}

		// populate before test invocation
		if (!mongoDataSet.value().isEmpty()) {
			new ElasticsearchDataTools(elasticTemplate).importFrom(mongoDataSet.value());
		}

		// if read-only data set than we need to save a mongo state before run test in temp file
		if (isReadOnlyDataSet(context)) {
			File tempFile = File.createTempFile("mongo-test-", "-readonly");
			tempFile.deleteOnExit();
			new ElasticsearchDataTools(elasticTemplate).exportTo(tempFile.getAbsolutePath());
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
			new ElasticsearchDataTools(elasticTemplate).expect(filePath);
		} catch (Error e) {
			throw new RuntimeException("Expected ReadOnly dataset, but found some modifications:",e);
		}
	}

	private void expectedDataSet(ExtensionContext context) {
		ExpectedElasticDataSet expectedElasticDataSet = getAnnotationFromCurrentMethod(context, ExpectedElasticDataSet.class);
		if (expectedElasticDataSet == null) {
			return;
		}
		new ElasticsearchDataTools(elasticTemplate).expect(expectedElasticDataSet.value());
	}
	//endregion Expected

	private void exportDataSet(ExtensionContext context) {

		ExportElasticDataSet exportMongoDataSet = getAnnotationFromCurrentMethod(context, ExportElasticDataSet.class);
		if (exportMongoDataSet == null) {
			return;
		}
		new ElasticsearchDataTools(elasticTemplate).exportTo(exportMongoDataSet.outputFile());
	}

	private void cleanAfter(ExtensionContext context) {
		ElasticDataSet mongoDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);
		if (mongoDataSet != null && mongoDataSet.cleanAfter()) {
			cleanDataBase();
		}
	}

	private void cleanDataBase() {

		Map<String, Class<?>> indexes =
				new AnnotatedDocumentScanner("").scan(Document.class)
				                                .mapByAnnotationAttr(Document.class, Document::indexName);

		indexes.forEach((k, v) -> {
			elasticTemplate.deleteIndex(v);
			elasticTemplate.createIndex(v);
			elasticTemplate.putMapping(v);
			elasticTemplate.refresh(v);
		});
	}

	private <AnnotationT extends Annotation> AnnotationT getAnnotationFromCurrentMethod(ExtensionContext context,
	                                                                                    Class<AnnotationT> annotationClass) {
		return context.getRequiredTestMethod()
		              .getAnnotation(annotationClass);
	}

	private boolean isReadOnlyDataSet(ExtensionContext context) {
		ElasticDataSet mongoDataSet = getAnnotationFromCurrentMethod(context, ElasticDataSet.class);
		return mongoDataSet != null && mongoDataSet.readOnly();
	}
}
