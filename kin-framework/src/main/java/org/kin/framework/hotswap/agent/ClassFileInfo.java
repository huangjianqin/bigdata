package org.kin.framework.hotswap.agent;

import org.kin.framework.utils.ExceptionUtils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Created by huangjianqin on 2019/3/1.
 */
public class ClassFileInfo {
    private final String filePath;
    private final String className;
    private final byte[] bytes;
    private final long lastModifyTime;
    private final String md5;

    public ClassFileInfo(String filePath, String className, byte[] bytes, long lastModifyTime) {
        this.filePath = filePath;
        this.className = className;
        this.bytes = bytes;
        this.lastModifyTime = lastModifyTime;
        this.md5 = this.md5();
    }

    private String md5() {
        try {
            MessageDigest me = MessageDigest.getInstance("MD5");
            me.update(this.bytes);
            BigInteger bi = new BigInteger(1, me.digest());
            return bi.toString(16).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            ExceptionUtils.log(e);
        }

        return "";
    }

    //getter
    public String getFilePath() {
        return filePath;
    }

    public String getClassName() {
        return className;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public long getLastModifyTime() {
        return lastModifyTime;
    }

    @Override
    public String toString() {
        return "ClassFileInfo{" +
                "filePath='" + filePath + '\'' +
                ", className='" + className + '\'' +
                ", bytes=" + Arrays.toString(bytes) +
                ", lastModifyTime=" + lastModifyTime +
                ", md5='" + md5 + '\'' +
                '}';
    }
}
