package com.openspark.structured;

import com.openspark.sqlstream.parser.InsertSqlParser;
import com.openspark.sqlstream.parser.SqlTree;

import java.util.List;
import java.util.Set;

public class InsertSqlTest {

    public static void main(String[] args) {
        //String sqlStr = "insert into MyResult select a.channel,b.xccount from MyTable a join sideTable b on a.channel=b.channel where b.channel = 'xc' and a.pv=10";
        String sqlStr = "insert into consle select word,count(wordcout) from SocketTable group by word";
        InsertSqlParser insertSqlParser = InsertSqlParser.newInstance();
        SqlTree sqlTree = new SqlTree();
        boolean verify = insertSqlParser.verify(sqlStr);
        if (verify) {
            insertSqlParser.parseSql(sqlStr, sqlTree);
        }
        Set<InsertSqlParser.SqlParseResult> execSqlList = sqlTree.getExecSqlList();

        for (InsertSqlParser.SqlParseResult result : execSqlList) {
            System.out.println(result.getTargetTable());
            System.out.println(result.getSourceTableList());
            System.out.println(result.getExecSql());
            System.out.println(result.getQuerySql());

        }

        // System.out.println(sqlTree.getPreDealTableMap().get(""));
    }
}
