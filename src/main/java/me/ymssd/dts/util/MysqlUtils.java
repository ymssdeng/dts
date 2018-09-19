package me.ymssd.dts.util;

import static me.ymssd.dts.sink.AbstractMysqlSinker.KEYWORD_ESCAPE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import me.ymssd.dts.model.ColumnMetaData;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author denghui
 * @create 2018/9/19
 */
public class MysqlUtils {

    public static String getPRIColumnName(DataSource dataSource, String table) throws SQLException {
        List<ColumnMetaData> columnMetaDataList = getColumnsMetaData(dataSource, table);
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            if (columnMetaData.isPrimaryKey()) {
                return columnMetaData.getField();
            }
        }
        return null;
    }

    public static List<ColumnMetaData> getColumnsMetaData(DataSource dataSource, String table) throws SQLException {
        StringBuilder sb = new StringBuilder("SHOW COLUMNS FROM ");
        sb.append(KEYWORD_ESCAPE);
        sb.append(table);
        sb.append(KEYWORD_ESCAPE);
        QueryRunner runner = new QueryRunner(dataSource);
        return runner.query(sb.toString(), rs -> {
            List<ColumnMetaData> cmdList = new ArrayList<>();
            while (rs.next()) {
                ColumnMetaData cmd = new ColumnMetaData();
                cmd.setField(rs.getString("Field"));
                cmd.setType(rs.getString("Type"));
                cmd.setNull(rs.getString("Null"));
                cmd.setKey(rs.getString("Key"));
                cmd.setDefault(rs.getString("Default"));
                cmd.setExtra(rs.getString("Extra"));
                cmdList.add(cmd);
            }
            return cmdList;
        });
    }
}
