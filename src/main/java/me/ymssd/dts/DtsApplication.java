package me.ymssd.dts;

import me.ymssd.dts.config.DtsConfig;
import org.yaml.snakeyaml.Yaml;

/**
 * @author denghui
 * @create 2018/9/6
 */
public class DtsApplication {

    private static final String CONFIG_FILE = "dts.yaml";

    public static void main(String[] args) {
        DtsApplication app = new DtsApplication();
        Yaml yaml = new Yaml();
        DtsConfig dtsConfig = yaml.loadAs(app.getClass().getClassLoader().getResourceAsStream(CONFIG_FILE),
            DtsConfig.class);

        Dts dts = new Dts(dtsConfig);
        dts.start();
    }

}
