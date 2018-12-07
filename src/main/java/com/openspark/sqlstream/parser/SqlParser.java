package com.openspark.sqlstream.parser;

import com.openspark.sqlstream.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import java.util.List;

import static com.openspark.sqlstream.util.DtStringUtil.*;



public class SqlParser {

    private static final char SQL_DELIMITER = ';';


    private static List<IParser> sqlParserList = newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance(),CreateSinkParser.newInstance());

    /**
     * flink support sql syntax
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.dtstack.com;
     * insert into tb1 select * from tb2;
     * @param sql
     */
    public static SqlTree parseSql(String sql) throws Exception {

        if(StringUtils.isBlank(sql)){
            throw new RuntimeException("sql is not null");
        }


        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        //将整个sql文件按照';'分隔，分成几个sql
        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        SqlTree sqlTree = new SqlTree();

        for(String childSql : sqlArr){
            if(isNullOrEmpty(childSql)){
                continue;
            }
            boolean result = false;
            //sqlParserListh含有三种解析类型，CreateFuncParser——CreateTableParser——InsertSqlParser
            //为每一个sql查找合适的解析类型
            for(IParser sqlParser : sqlParserList){
                if(!sqlParser.verify(childSql)){
                    continue;
                }
                sqlParser.parseSql(childSql, sqlTree);
                result = true;
            }

            if(!result){
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }

        //解析exec-sql
        System.out.println();
        if(sqlTree.getExecSqlList().size() == 0 && sqlTree.getExecSql()==null){
            throw new RuntimeException("sql no executable statement");
        }

//        for(InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()){
//            List<String> sourceTableList = result.getSourceTableList();
//            List<String> targetTableList = result.getTargetTableList();

//            for(String tableName : sourceTableList){
//                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
//                if(createTableResult == null){
//                    throw new RuntimeException("can't find table " + tableName);
//                }
//
//                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SOURCE.getType(),
//                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
//                sqlTree.addTableInfo(tableName, tableInfo);
//            }
//
//            for(String tableName : targetTableList){
//                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
//                if(createTableResult == null){
//                    throw new RuntimeException("can't find table " + tableName);
//                }
//
//                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SINK.getType(),
//                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
//                sqlTree.addTableInfo(tableName, tableInfo);
//            }
//        }

        return sqlTree;
    }
}
