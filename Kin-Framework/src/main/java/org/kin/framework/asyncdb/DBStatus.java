package org.kin.framework.asyncdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2019/3/31.
 */
public enum DBStatus {
    NORMAL {
        @Override
        public boolean execute(DBSynchronzier DBSynchronzier, AsyncDBEntity asyncDBEntity) {
            return true;
        }
    },
    INSERT {
        @Override
        public boolean execute(DBSynchronzier DBSynchronzier, AsyncDBEntity asyncDBEntity) {
            try{
                DBSynchronzier.insert(asyncDBEntity);
                return true;
            }
            catch (Exception e){
                log.error("", e);
            }

            return false;
        }
    },
    UPDATE {
        @Override
        public boolean execute(DBSynchronzier DBSynchronzier, AsyncDBEntity asyncDBEntity) {
            try{
                DBSynchronzier.update(asyncDBEntity);
                return true;
            }
            catch (Exception e){
                log.error("", e);
            }

            return false;
        }
    },
    DELETED {
        @Override
        public boolean execute(DBSynchronzier DBSynchronzier, AsyncDBEntity asyncDBEntity) {
            try{
                DBSynchronzier.delete(asyncDBEntity);
                return true;
            }
            catch (Exception e){
                log.error("", e);
            }

            return false;
        }
    },;

    private static final Logger log = LoggerFactory.getLogger("asyncDB");
    public abstract boolean execute(DBSynchronzier DBSynchronzier, AsyncDBEntity asyncDBEntity);

}
