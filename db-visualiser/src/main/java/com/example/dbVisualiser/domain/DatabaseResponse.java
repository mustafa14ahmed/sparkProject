package com.example.dbVisualiser.domain;

import lombok.Data;

import java.sql.DatabaseMetaData;
import java.util.List;

@Data
public class DatabaseResponse {
    private DatabaseMetaData databaseMetaData;
    private List<String> tableNameList;
    private String schema;
    private String datasetType;
    private String databaseType;
    private String host;
    private int port;
    private String username;
    private String database;

}
