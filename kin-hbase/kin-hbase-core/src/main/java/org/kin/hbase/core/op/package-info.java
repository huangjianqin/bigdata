/**
 * @author huangjianqin
 * @date 2018/5/24
 */
package org.kin.hbase.core.op;
/*
服务端过滤器!!!!!

操作符：
LESS
LESS_OR_EQUAL
EQUAL
NOT_EQUAL
GREATER_OR_EQUAL
GREATER
NO_OP

字节级的比较 && 字符串级的比较:
1.RegexStringComparator
支持正则表达式的值比较

Scan scan = new Scan();
RegexStringComparator comp = new RegexStringComparator("you.");
SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), CompareOp.EQUAL, comp);
scan.setFilter(filter);

2.SubStringComparator
用于监测一个子串是否存在于值中，并且不区分大小写。

Scan scan = new Scan();
SubstringComparator comp = new SubstringComparator("1129");
SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), CompareOp.EQUAL, comp);
scan.setFilter(filter);

3.BinaryPrefixComparator
前缀二进制比较器。与二进制比较器不同的是，只比较前缀是否相同。

Scan scan = new Scan();
BinaryPrefixComparator comp = new BinaryPrefixComparator(Bytes.toBytes("yting"));
SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"),  CompareOp.EQUAL, comp);
scan.setFilter(filter);

4.BinaryComparator
二进制比较器，用于按字典顺序比较 Byte 数据值

Scan scan = new Scan();
BinaryComparator comp = new BinaryComparator(Bytes.toBytes("xmei"));
ValueFilter filter = new ValueFilter(CompareOp.EQUAL, comp);
scan.setFilter(filter);


列值过滤器:
1.SingleColumnValueFilter
SingleColumnValueFilter 用于测试值的情况（相等, 不等, 范围）

Scan scan = new Scan();
SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), CompareOp.EQUAL, Bytes.toBytes("my-value"));
scan.setFilter(filter);

2.SingleColumnValueExcludeFilter
跟 SingleColumnValueFilter 功能一样，只是不查询出该列的值。

Scan scan = new Scan();
SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), CompareOp.EQUAL, Bytes.toBytes("my-value"));
scan.setFilter(filter);


键值元数据过滤器:
row -> ColumnFamily -> Column -> timestamp -> qualifier

1.FamilyFilter
用于过滤列族（通常在 Scan 过程中通过设定某些列族来实现该功能，而不是直接使用该过滤器）。

Scan scan = new Scan();
FamilyFilter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("my-family")));
scan.setFilter(filter);

2.QualifierFilter
用于列名（Qualifier）过滤。

Scan scan = new Scan();
QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("my-column")));
scan.setFilter(filter);

3.ColumnPrefixFilter
用于列名（Qualifier）前缀过滤，即包含某个前缀的所有列名。

Scan scan = new Scan();
ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("my-prefix"));
scan.setFilter(filter);

4. MultipleColumnPrefixFilter
MultipleColumnPrefixFilter 与 ColumnPrefixFilter 的行为类似，但可以指定多个列名（Qualifier）前缀。

Scan scan = new Scan();
byte[][] prefixes = new byte[][]{Bytes.toBytes("my-prefix-1"), Bytes.toBytes("my-prefix-2")};
MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefixes);
scan.setFilter(filter);

5.ColumnRangeFilter
该过滤器可以进行高效的列名内部扫描

Scan scan = new Scan();
boolean minColumnInclusive = true;
boolean maxColumnInclusive = true;
ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("minColumn"), minColumnInclusive, Bytes.toBytes("maxColumn"), maxColumnInclusive);
scan.setFilter(filter);

6.DependentColumnFilter
该过滤器尝试找到该列所在的每一行，并返回该行具有相同时间戳的全部键值对。

Scan scan = new Scan();
DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("family"), Bytes.toBytes("qualifier"));
scan.setFilter(filter);


行键过滤器:
1.RowFilter
行键过滤器，一般来讲，执行 Scan 使用 startRow/stopRow 方式比较好，而 RowFilter 过滤器也可以完成对某一行的过滤。

Scan scan = new Scan();
RowFilter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("my-row-1")));
scan.setFilter(filter);

2.RandomRowFilter
该过滤器是随机选择一行的过滤器。参数 chance 是一个浮点值，介于 0.1 和 1.0 之间。

Scan scan = new Scan();
float chance = 0.5f;
RandomRowFilter filter = new RandomRowFilter(chance);
scan.setFilter(filter);


功能过滤器:
1.PageFilter
用于按行分页。

2.FirstKeyOnlyFilter
该过滤器只查询每个行键的第一个键值对，在统计计数的时候提高效率。（HBase-Coprocessor 做 RowCount 的时候可以提高效率）

Scan scan = new Scan();
FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
scan.setFilter(filter);

3.KeyOnlyFilter
Scan scan = new Scan();
KeyOnlyFilter filter = new KeyOnlyFilter(); // 只查询每行键值对中有 "键" 元数据信息，不显示值，可以提升扫描的效率
scan.setFilter(filter);

4.InclusiveStopFilter
使用该过滤器便可以包含stopRow,但Scan实例不可以setStopRow

Scan scan = new Scan();
InclusiveStopFilter filter = new InclusiveStopFilter(Bytes.toBytes("stopRowKey"));
scan.setFilter(filter);

5.ColumnPaginationFilter
按列分页过滤器，针对列数量很多的情况使用。

Scan scan = new Scan();
int limit = 0;
int columnOffset = 0;
ColumnPaginationFilter filter = new ColumnPaginationFilter(limit, columnOffset);
scan.setFilter(filter);


自定义过滤器:
继承 FilterBase，然后打成 jar 放到 $HBASE_HOEM/lib 目录下去（注意：需要重启 HBase 集群）

 */