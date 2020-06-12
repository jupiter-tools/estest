package com.jupitertools.estest.internal.scanner;

import java.util.Map;


import com.jupitertools.datasetroll.exportdata.scanner.AnnotatedDocumentScanner;
import com.jupitertools.datasetroll.exportdata.scanner.DocumentScanner;

import org.springframework.data.elasticsearch.annotations.Document;

/**
 * Created on 19/11/2019
 * <p>
 * Scans packages of a current repository for classes annotated with the {@link Document}.
 *
 * @author Korovin Anatoliy
 */
public class ElasticDocumentScanner implements DocumentScanner {

    private final AnnotatedDocumentScanner annotatedDocumentScanner;

    public ElasticDocumentScanner(String basePackage) {
        this.annotatedDocumentScanner = new AnnotatedDocumentScanner(basePackage);
    }

    @Override
    public Map<String, Class<?>> scan() {
        return annotatedDocumentScanner.scan(Document.class)
                                       .mapByClassAttr(Class::getName);
    }
}
