package test;

import cn.think.in.java.entity.LogEntry;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

public class RocksDBTest {
    public static void main(String[] args) throws RocksDBException {
        String dbDir[] = new String[]{"./rocksDB-raft/8776","./rocksDB-raft/8777","./rocksDB-raft/8778"} ;
        for (String db:dbDir){
            String stateMachineDir = db + "/logModule";
            RocksDB machineDb;

            RocksDB.loadLibrary();

            byte[] lastIndexKey = "LAST_INDEX_KEY".getBytes();
            File file = new File(stateMachineDir);
            if (!file.exists()) {
                file.mkdirs();
            }
            Options options = new Options();
            options.setCreateIfMissing(true);
            machineDb = RocksDB.open(options, stateMachineDir);
            byte[] lastIndex = machineDb.get(lastIndexKey);
            long inf=Long.valueOf(new String(lastIndex));
            System.out.println(inf);
            byte[] cmd = machineDb.get(Long.valueOf(new String(lastIndex)).toString().getBytes());
            System.out.println( JSON.parseObject(cmd, LogEntry.class).toString());
        }


    }

}
