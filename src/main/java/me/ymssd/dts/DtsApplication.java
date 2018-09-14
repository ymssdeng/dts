package me.ymssd.dts;

import java.sql.SQLException;
import me.ymssd.dts.AbstractDts.Style;
import me.ymssd.dts.config.DtsConfig;
import org.yaml.snakeyaml.Yaml;

/**
 * @author denghui
 * @create 2018/9/6
 */
public class DtsApplication {

    private static final String CONFIG_FILE = "dts.yaml";

    public static void main(String[] args) throws SQLException {
        DtsApplication app = new DtsApplication();
        Yaml yaml = new Yaml();
        DtsConfig dtsConfig = yaml.loadAs(app.getClass().getClassLoader().getResourceAsStream(CONFIG_FILE),
            DtsConfig.class);

        AbstractDts dts = null;
        if (dtsConfig.getStyle() == Style.Java8) {
            dts = new Java8Dts(dtsConfig);
        } else if (dtsConfig.getStyle() == Style.RxJava) {
            dts = new RxJavaDts(dtsConfig);
        }
        dts.start();
    }
}
