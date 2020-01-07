package com.example.dbVisualiser.domain;

import lombok.Data;

/**
 * DatabaseRequest to accommodate request for connecting to db.
 */
@Data
public class DatabaseRequest {

    private String datasetType;
    private String databaseType;
    private String host;
    private int port;
    private String username;
    private String password;
    private String database;
    private String schema;
}
