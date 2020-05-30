package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.Dictionary;
import java.util.UUID;

public class UpdateLog extends InsertUpdateLog {
    public UpdateLog(UUID id, Dictionary data) {
        super(id, data);
        this.type = LogType.Update;
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
    }
}