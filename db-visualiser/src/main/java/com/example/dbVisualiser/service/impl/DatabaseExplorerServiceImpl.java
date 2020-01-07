package com.example.dbVisualiser.service.impl;

import com.example.dbVisualiser.domain.DatabaseRequest;
import com.example.dbVisualiser.domain.DatabaseResponse;
import com.example.dbVisualiser.service.DatabaseExplorerService;
import org.springframework.stereotype.Component;

@Component
public class DatabaseExplorerServiceImpl implements DatabaseExplorerService {

    @Override
    public DatabaseResponse processDatabaseRequest(DatabaseRequest databaseRequest) {
        return new DatabaseResponse();
    }
}
