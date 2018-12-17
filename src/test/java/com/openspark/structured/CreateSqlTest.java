package com.openspark.structured;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.parser.SqlTree;
import com.openspark.sqlstream.source.BaseInput;
import com.openspark.sqlstream.util.DtStringUtil;

import java.util.Map;

public class CreateSqlTest {

    static String sourceBasePackage = "com.openspark.sqlstream.source.";

    public static void main(String[] args) {
//        String sqlStr = "CREATE TABLE MyResult(channel STRING,pv INT)WITH(type='socket',url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName='dtstack',password='abc123',tableName='pv')";
//
//        CreateTableParser createTableParser = CreateTableParser.newInstance();
//        boolean verify = createTableParser.verify(sqlStr);
//        SqlTree sqlTree = new SqlTree();
//
//        if (verify) {
//            createTableParser.parseSql(sqlStr, sqlTree);
//        }
//        Map<String, CreateTableParser.SqlParserResult> preDealTableMap = sqlTree.getPreDealTableMap();
//
//        preDealTableMap.forEach((key, val) -> {
//            System.out.println(key + " " + val.getFieldsInfoStr());
//        });
//
//        for (String mapKey : sqlTree.getPreDealTableMap().keySet()) {
//            System.out.println(mapKey + " = " + sqlTree.getPreDealTableMap().get(mapKey).getFieldsInfoStr());
//            String type = (String) sqlTree.getPreDealTableMap().get(mapKey).getPropMap().get("type");
//            String upperType = DtStringUtil.upperCaseFirstChar(type) + "Input";
//
//            BaseInput inputBase = null;
//            try {
//                inputBase = Class.forName(sourceBasePackage + upperType).asSubclass(BaseInput.class).newInstance();
//            } catch (InstantiationException e) {
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                e.printStackTrace();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//
//            System.out.println(inputBase.getName());
//        }


    }
}
