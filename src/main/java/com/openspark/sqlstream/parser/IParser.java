package com.openspark.sqlstream.parser;


public interface IParser {

    /**
     * 是否满足该解析类型
     *
     * @param sql
     * @return
     */
    boolean verify(String sql);

    /***
     * 解析sql
     * @param sql
     * @param sqlTree
     */
    void parseSql(String sql, SqlTree sqlTree);
}
