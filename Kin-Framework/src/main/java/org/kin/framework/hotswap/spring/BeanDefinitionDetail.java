package org.kin.framework.hotswap.spring;

import org.springframework.beans.factory.config.BeanDefinition;

import java.io.File;

/**
 * Created by huangjianqin on 2018/2/1.
 */
public class BeanDefinitionDetail {
    private String beanName;
    private BeanDefinition beanDefinition;
    private File file;

    public BeanDefinitionDetail(String beanName, BeanDefinition beanDefinition, File file) {
        this.beanName = beanName;
        this.beanDefinition = beanDefinition;
        this.file = file;
    }

    public String getBeanName() {
        return beanName;
    }

    public BeanDefinition getBeanDefinition() {
        return beanDefinition;
    }

    public File getFile() {
        return file;
    }
}
