package com.openspark.sqlstream.parser;

import com.openspark.sqlstream.util.DtStringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CreateEnvParser implements IParser {

    private static final String PATTERN_STR = "create\\s+env\\s+(\\S+)\\s*\\((.+)\\)\\s*WITH\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateEnvParser newInstance() {
        return new CreateEnvParser();
    }

    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String sparkEnv = matcher.group(2);
            String appConf = matcher.group(3);
            Map<String, Object> props = DtStringUtil.parseProp(sparkEnv);
            sqlTree.addPreDealSparkEnvInfo(props);
            sqlTree.setAppInfo(DtStringUtil.getAppName(appConf));
        }
    }

    public static void main(String[] args) {
        CreateEnvParser createEnvParser = CreateEnvParser.newInstance();
        SqlTree sqlTree = new SqlTree();
        createEnvParser.parseSql("create env spark(" +
                "    spark.default.parallelism='2'," +
                "    spark.sql.shuffle.partitions='2'" +
                ")WITH(" +
                "    appname='sockettest'" +
                ")",sqlTree);
       //
        sqlTree.getPreDealSparkEnvMap().forEach((k,v)->{
            System.out.println(k);
            System.out.println(v.toString());
        });

    }


}
