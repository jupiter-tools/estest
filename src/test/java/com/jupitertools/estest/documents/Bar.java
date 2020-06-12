package com.jupitertools.estest.documents;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "bar-index")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Bar {

    @Id
    private String id;

    private String data;
}