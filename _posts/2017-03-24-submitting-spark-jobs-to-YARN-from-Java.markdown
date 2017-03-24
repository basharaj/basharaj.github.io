*These instructions are for Spark 1.6.2. Since Spark is notorious for breaking backward compatibility. These instructions may or may not work with Spark 2.x*


One of the most common requirements when working with Spark is the ability to submit Spark jobs to YARN from your Java code instead of using [spark-submit](http://spark.apache.org/docs/latest/submitting-applications.html). In this post
I'll show you how to do that.

* **Getting the right dependencies**

At a minimum you are going to need the `spark-core`` and `spark-yarn` dependencies. Those include the Spark classes that you are going to need to submit your Spark job.

If you using Maven the dependencies are:

```
<groupId>org.apache.spark</groupId>
<artifactId>spark-core_2.10</artifactId>
<version.spark>1.6.2</version.spark>

<groupId>org.apache.spark</groupId>
<artifactId>spark-yarn_2.10</artifactId>
<version.spark>1.6.2</version.spark>

```
* **SparkConf**

[SparkConf](http://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/SparkConf.html) is used to set the various Spark configurations which you would normally pass on to `--conf` when using `spark-submit`. For example:

```
SparkConf sparkConf = new SparkConf();
sparkConf.set("spark.yarn.preserve.staging.files", "true");
```

* **The Client Arguments**

One confusing thing about Spark is that there is no single way to configure it. Some configurations are set using configuration properties, some are using command line arguments and some using System properties. [ClientArguments](https://github.com/apache/spark/blob/v1.6.2/core/src/main/scala/org/apache/spark/deploy/ClientArguments.scala) is a Scala class that can be used
to pass the command line arguments that you would normally pass to `spark-submit` command. This for example includes the job name, job class, job Jar..etc. One way to pass those arguments is to create a List of Strings as so:
```
List<String> args = Lists.newArrayList(
  "--name", jobName,
  "--jar", applicationJar,
  "--class", jobClass
);
```
Where we set the job name, the job Uber jar and the job class. You can add additional arguments to that list as so:
```
args.add("--keytab");
args.add(keyTab);

args.add("--executor-cores");
args.add(1);
```

Once you have built your list of args, you need to pass them to `ClientArguments` as so:

```ClientArguments cArgs = new ClientArguments(args.toArray(new String[args.size()]), sparkConf);
```


* **The System properties**

Remember what I said earlier that Spark configuration can be confusing? there is a third place where you have to set Spark properties. System properties. If you are using Spark in YARN mode the only way to configure so is as a System property:

```System.setProperty("SPARK_YARN_MODE", "true");```

Which should be enough for Spark to pick it up at runtime.

* **Putting it all together**

Now you are ready to submit your Job to YARN. Here what our final application will look like:

```

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Submits a job to YARN in cluster mode
 */
public class SparkJobSubmitter  {

  private static final Logger logger = LoggerFactory.getLogger(SparkJobSubmitter.class);

  private String jobName;
  private String jobClass;
  private String applicationJar;
  private String[] additionalJars;
  private String[] files;
  private Properties sparkProperties;

  public SparkJobSubmitter(String jobName, String jobClass, String applicationJar, Properties sparkProperties,
                           String[] additionalJars, String[] files, boolean killRunningJobs, boolean enableHistoryServer, boolean addHiveDelegationToken) {
    this.jobName = jobName;
    this.jobClass = jobClass;
    this.applicationJar = applicationJar;
    this.sparkProperties = sparkProperties;
    this.additionalJars = additionalJars;
    this.files = files;
  }

  public void submit() {

    List<String> args = Lists.newArrayList("--name", jobName, "--jar", applicationJar, "--class", jobClass);

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.yarn.preserve.staging.files", "true");

    if (additionalJars != null && additionalJars.length > 0) {
      args.add("--addJars");
      args.add(StringUtils.join(additionalJars, ","));
    }

    if (files != null && files.length > 0) {
      args.add("--files");
      args.add(StringUtils.join(files, ","));
    }

    if (sparkProperties.getProperty("spark.executor.cores") != null) {
      args.add("--executor-cores");
      args.add(sparkProperties.getProperty("spark.executor.cores"));
    }

    if (sparkProperties.getProperty("spark.executor.memory") != null) {
      args.add("--executor-memory");
      args.add(sparkProperties.getProperty("spark.executor.memory"));
    }

    if (sparkProperties.getProperty("spark.driver.cores") != null) {
      args.add("--driver-cores");
      args.add(sparkProperties.getProperty("spark.driver.cores"));
    }

    if (sparkProperties.getProperty("spark.driver.memory") != null) {
      args.add("--driver-memory");
      args.add(sparkProperties.getProperty("spark.driver.memory"));
    }

    // identify that you will be using Spark as YARN mode
    System.setProperty("SPARK_YARN_MODE", "true");

    for (Map.Entry<Object, Object> e : sparkProperties.entrySet()) {
      sparkConf.set(e.getKey().toString(), e.getValue().toString());
    }

    logger.info("Spark args: {}", Arrays.toString(args.toArray()));
    logger.info("Spark conf settings: {}", Arrays.toString(sparkConf.getAll()));

    ClientArguments cArgs = new ClientArguments(args.toArray(new String[args.size()]), sparkConf);
    Client client = new Client(cArgs, new Configuration(), sparkConf);
    client.run();
  }

}

```
