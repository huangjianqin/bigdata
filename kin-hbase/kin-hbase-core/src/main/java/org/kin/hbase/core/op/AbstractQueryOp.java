package org.kin.hbase.core.op;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.hbase.core.domain.QueryInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public abstract class AbstractQueryOp<S extends AbstractQueryOp<S>> extends AbstractHBaseOp<S> {
    private final List<QueryInfo> queryInfos = new ArrayList<>();
    private final List<Filter> filters = new ArrayList<>();

    public AbstractQueryOp(String tableName) {
        super(tableName);
    }

    //-------------------------------------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private S addCondition(QueryInfo queryInfo) {
        queryInfos.add(queryInfo);
        return (S) this;
    }

    private S qualifierValueFilter(String family, String qualifier, String value, CompareOperator compareOp) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(family),
                Bytes.toBytes(qualifier),
                compareOp,
                Bytes.toBytes(value));
        filter.setFilterIfMissing(true);

        return addFilter(filter);
    }

    private S qualifierValueFilter(String family, String qualifier, ByteArrayComparable comparable, CompareOperator compareOp) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(family),
                Bytes.toBytes(qualifier),
                compareOp,
                comparable);
        filter.setFilterIfMissing(true);

        return addFilter(filter);
    }

    @SuppressWarnings("unchecked")
    private S addFilter(Filter filter) {
        filters.add(filter);
        return (S) this;
    }

    //---------------------------------------属性----------------------------------------------------------------
    public S family(String family) {
        return families(family);
    }

    @SuppressWarnings("unchecked")
    public S families(String... families) {
        if (families != null && families.length > 0) {
            for (String family : families) {
                addCondition(QueryInfo.family(family));
            }
        }
        return (S) this;
    }

    public S column(String family, String qualifier) {
        return columns(Collections.singletonMap(family, Collections.singletonList(qualifier)));
    }

    @SuppressWarnings("unchecked")
    public S columns(Map<String, List<String>> family2Qualifiers) {
        if (family2Qualifiers != null && family2Qualifiers.size() > 0) {
            for (String family : family2Qualifiers.keySet()) {
                List<String> qualifiers = family2Qualifiers.get(family);
                if (qualifiers != null && qualifiers.size() > 0) {
                    for (String qualifier : qualifiers) {
                        addCondition(QueryInfo.column(family, qualifier));
                    }
                }
            }
        }

        return (S) this;
    }

    //-------------------------------------------------------------------------------------------------------
    //列值过滤器
    public S eq(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.EQUAL);
    }

    public S neq(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.NOT_EQUAL);
    }

    public S gt(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.GREATER);
    }

    public S ge(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.GREATER_OR_EQUAL);
    }

    public S lt(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.LESS);
    }

    public S le(String family, String qualifier, String value) {
        return qualifierValueFilter(family, qualifier, value, CompareOperator.LESS_OR_EQUAL);
    }

    public S regexFilter(String family, String qualifier, String regex) {
        return qualifierValueFilter(family, qualifier, new RegexStringComparator(regex), CompareOperator.EQUAL);
    }

    public S substringFilter(String family, String qualifier, String substring) {
        return qualifierValueFilter(family, qualifier, new SubstringComparator(substring), CompareOperator.EQUAL);
    }

    public S nRegexFilter(String family, String qualifier, String regex) {
        return qualifierValueFilter(family, qualifier, new RegexStringComparator(regex), CompareOperator.NOT_EQUAL);
    }

    public S nSubstringFilter(String family, String qualifier, String substring) {
        return qualifierValueFilter(family, qualifier, new SubstringComparator(substring), CompareOperator.NOT_EQUAL);
    }

    //-------------------------------------------------------------------------------------------------------
    //键值元数据过滤器
    public S familyFilter(String family) {
        FamilyFilter filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(family)));
        return addFilter(filter);
    }

    public S nFamilyFilter(String family) {
        FamilyFilter filter = new FamilyFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(family)));
        return addFilter(filter);
    }

    public S qualifierFilter(String qualifier) {
        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(qualifier)));
        return addFilter(filter);
    }

    public S nQualifierFilter(String qualifier) {
        QualifierFilter filter = new QualifierFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(qualifier)));
        return addFilter(filter);
    }

    public S qualifierPrefixFilter(String prefix) {
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
        return addFilter(filter);
    }

    public S qualifierPrefixFilter(String[] prefixss) {
        byte[][] prefixBytess = new byte[prefixss.length][];
        for (int i = 0; i < prefixss.length; i++) {
            prefixBytess[i] = Bytes.toBytes(prefixss[i]);
        }


        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefixBytess);
        return addFilter(filter);
    }

    public S qualifierRangeFilter(String startQualifier, boolean isIncludeStart, String stopQualifier, boolean isIncludeStop) {
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(startQualifier), isIncludeStart, Bytes.toBytes(stopQualifier), isIncludeStop);
        return addFilter(filter);
    }

    public S dependentQualifierFilter(String family, String qualifier) {
        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return addFilter(filter);
    }

    //-----------------------------------------------行键元数据过滤器--------------------------------------------------------
    public S rowFilter(String rowKey) {
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
        return addFilter(filter);
    }

    public S nRowFilter(String rowKey) {
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
        return addFilter(filter);
    }

    public S pageFilter(int pageSize) {
        PageFilter filter = new PageFilter(pageSize);
        return addFilter(filter);
    }

    public S firstKeyOnlyFilter() {
        FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
        return addFilter(filter);
    }

    public S keyOnlyFilter() {
        KeyOnlyFilter filter = new KeyOnlyFilter();
        return addFilter(filter);
    }

    public S includeStop(String stopRowKey) {
        InclusiveStopFilter filter = new InclusiveStopFilter(Bytes.toBytes(stopRowKey));
        return addFilter(filter);
    }

    public S columnPagingationFilter(int limit, int columnOffset) {
        ColumnPaginationFilter filter = new ColumnPaginationFilter(limit, columnOffset);
        return addFilter(filter);
    }

    //getter
    public List<QueryInfo> getQueryInfos() {
        return queryInfos;
    }

    public List<Filter> getFilters() {
        return filters;
    }
}
