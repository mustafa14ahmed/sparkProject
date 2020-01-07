package com.example.dbVisualiser.service;

import com.example.dbVisualiser.domain.DatabaseRequest;
import com.example.dbVisualiser.domain.DatabaseResponse;

public interface DatabaseExplorerService {
    DatabaseResponse processDatabaseRequest(DatabaseRequest databaseRequest);
}
