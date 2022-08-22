package org.zalando.nakadi.config;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class TestConfigurationContext {
  private TestConfigurationContext configurationContext;
  private Map<String, Configuration> environments;

  /**
   * This method loads configurations from automation.yml file.
   *
   * @return Configurations for environment
   * @throws IOException If automation.yml file is missing.
   */
  public Configuration load() throws IOException {
    Yaml yaml = new Yaml(new Constructor(TestConfigurationContext.class));
    ClassLoader classLoader = TestConfigurationContext.class.getClassLoader();
    URL automationConfigFile = classLoader.getResource("automation.yml");

    if (Objects.isNull(automationConfigFile)) {
      throw new IOException("automation.yml configuration file is missing");
    }

    try (FileReader automationConfiguration = new FileReader(automationConfigFile.getFile())) {
      configurationContext = yaml.load(automationConfiguration);
      String environmentName = System.getenv("TEST_ENV");
      String url = System.getenv("NAKADI_BASE_URL");
      Configuration configuration = configurationContext.environments.get(environmentName);

      if (Objects.isNull(configuration) || (environmentName.equalsIgnoreCase("review") && url.isBlank())) {
        environmentName = "local";
        configuration = configurationContext.environments.get(environmentName);

      } else if (!environmentName.equalsIgnoreCase("review")) {
        configuration = configurationContext.environments.get(environmentName);

      }
      return configuration;
    }
  }

  public void setConfigurationContext(TestConfigurationContext configurationContext) {
    this.configurationContext = configurationContext;
  }

  public void setEnvironments(Map<String, Configuration> environments) {
    this.environments = environments;
  }
}


