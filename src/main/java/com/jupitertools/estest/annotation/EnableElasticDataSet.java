package com.jupitertools.estest.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.jupitertools.estest.junit5.ElasticDataSetExtension;
import org.junit.jupiter.api.extension.ExtendWith;


/**
 * Turn-ON the {@link ElasticDataSetExtension}
 * to use annotation based dataset matching
 *
 * @author Korovin Anatoliy
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(ElasticDataSetExtension.class)
public @interface EnableElasticDataSet {

}
