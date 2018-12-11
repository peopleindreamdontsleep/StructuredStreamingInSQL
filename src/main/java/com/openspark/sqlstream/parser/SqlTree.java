package com.openspark.sqlstream.parser;


import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.openspark.sqlstream.util.DtStringUtil.newArrayList;

/**
 * 解析sql获得的对象结构
 */

public class SqlTree {

    private List<CreateFuncParser.SqlParserResult> functionList = newArrayList();

    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = new HashMap<>();

    private Map<String, CreateTableParser.SqlParserResult> preDealSinkMap = new HashMap<>();

    private Map<String, TableInfo> tableInfoMap = new LinkedHashMap();


    private List<InsertSqlParser.SqlParseResult> execSqlList = newArrayList();

    private InsertSqlParser.SqlParseResult execSql;

    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealSinkMap() {
        return preDealSinkMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }

    public void addFunc(CreateFuncParser.SqlParserResult func) {
        functionList.add(func);
    }

    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table) {
        preDealTableMap.put(tableName, table);
    }

    public void addPreDealSinkInfo(String tableName, CreateTableParser.SqlParserResult table) {
        preDealSinkMap.put(tableName, table);
    }

    public void addExecSql(InsertSqlParser.SqlParseResult execSql) {
        execSqlList.add(execSql);
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public InsertSqlParser.SqlParseResult getExecSql() {
        return execSql;
    }

    public void setExecSql(InsertSqlParser.SqlParseResult execSql) {
        this.execSql = execSql;
    }

    public void addTableInfo(String tableName, TableInfo tableInfo) {
        tableInfoMap.put(tableName, tableInfo);
    }
}
