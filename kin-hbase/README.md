# kin-hbase  
    对HBase客户端的简单封装
#####1.注解  
    @HBaseEntity标识实体类  
    @RowKey标识行键  
    @Column标识列  
#####2.支持成员域直接赋值  
    2.1判断setter or getter  
    2.2从Field  
        赋值 or 获取值  
#####3.hbase链接池(spring boot配置方式)
#####4.支持实体类（赋值后），自定义序列化和反序列化
#####5.过滤器注入优化，通过函数式定义or添加过滤器
#####6.支持spring boot自动化配置依赖，并加载必要的bean
#####7.进一步工作  
    进一步了解业务需求和熟悉HBase客户端,作出进一步性能改进,代码优化