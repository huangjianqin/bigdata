package org.kin.hbase.core.domain;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class QueryInfo {
    private String family;
    /**
     * 不设置时默认，检索整个family
     */
    private String qualifier;

    private QueryInfo() {

    }

    private QueryInfo(String family, String qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public static QueryInfo family(String family) {
        return new QueryInfo(family, "");
    }

    public static QueryInfo column(String family, String qualifier) {
        return new QueryInfo(family, qualifier);
    }

    //getter
    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }
}
