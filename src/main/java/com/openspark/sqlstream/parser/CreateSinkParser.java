package com.openspark.sqlstream.parser;

import com.openspark.sqlstream.util.DtStringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CreateSinkParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+sink\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateSinkParser newInstance() {
        return new CreateSinkParser();
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
            Map<String, Object> props = parseProp(propsStr);

            CreateTableParser.SqlParserResult result = new CreateTableParser.SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealSinkInfo(tableName, result);
        }
    }

    private Map parseProp(String propsStr) {
        String[] strs = propsStr.trim().split("'\\s*,");
        Map<String, Object> propMap = new HashMap<>();
        for (int i = 0; i < strs.length; i++) {
            List<String> ss = DtStringUtil.splitIgnoreQuota(strs[i], '=');
            String key = ss.get(0).trim();
            String value = ss.get(1).trim().replaceAll("'", "").trim();
            propMap.put(key, value);
        }

        return propMap;
    }

}
