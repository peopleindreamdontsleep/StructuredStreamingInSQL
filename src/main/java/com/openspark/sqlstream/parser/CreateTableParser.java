package com.openspark.sqlstream.parser;

import com.openspark.sqlstream.util.DtStringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CreateTableParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateTableParser newInstance() {
        return new CreateTableParser();
    }

    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String tableName = matcher.group(1).toUpperCase();
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, Object> props = DtStringUtil.parseProp(propsStr);

            SqlParserResult result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealTableInfo(tableName, result);
        }
    }

    public static class SqlParserResult {

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, Object> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, Object> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, Object> propMap) {
            this.propMap = propMap;
        }
    }
}
