package com.example.dbVisualiser.controller;

import com.example.dbVisualiser.domain.DatabaseRequest;
import com.example.dbVisualiser.domain.DatabaseResponse;
import com.example.dbVisualiser.service.DatabaseExplorerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/databaseExplorer")
public class DatabaseExplorerController {

    @Autowired
    DatabaseExplorerService databaseExplorerService;

    @PostMapping("/processDatabase")
    public DatabaseResponse processDatabase(DatabaseRequest databaseRequest, HttpServletRequest httpServletRequest){
        DatabaseResponse databaseResponse = databaseExplorerService.processDatabaseRequest(databaseRequest);
//        We need to check whether we need to set this in the session or not.
        httpServletRequest.getSession().setAttribute("databaseResponse", databaseResponse);
        httpServletRequest.getSession().setAttribute("databaseRequest", databaseRequest);
        return databaseResponse;
    }

}
