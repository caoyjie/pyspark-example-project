# PySpark Example Project

This document is designed to be read in parallel with the code in the `pyspark-template-project` repository. Together, these constitute what we consider to be a 'best practices' approach to writing ETL jobs using Apache Spark and its Python ('PySpark') APIs. This project addresses the following topics:

- how to structure ETL code in such a way that it can be easily tested and debugged;
- how to pass configuration parameters to a PySpark job;
- how to handle dependencies on other modules and packages; and,
- what constitutes a 'meaningful' test for an ETL job.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- employees/
 |   |-- | -- employees_report/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

The main Python module containing the ETL job (which will be sent to the Spark cluster), is `jobs/etl_job.py`. Any external configuration parameters required by `etl_job.py` are stored in JSON format in `configs/etl_config.json`. Additional modules that support this job can be kept in the `dependencies` folder (more on this later). In the project's root we include `build_dependencies.sh`, which is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). Unit test modules are kept in the `tests` folder and small chunks of representative input and output data, to be used with the tests, are kept in `tests/test_data` folder.

## Structure of an ETL Job

In order to facilitate easy debugging and testing, we recommend that the 'Transformation' step be isolated from the 'Extract' and 'Load' steps, into its own function - taking input data arguments in the form of DataFrames and returning the transformed data as a single DataFrame. Then, the code that surrounds the use of the transformation function in the `main()` job function, is concerned with Extracting the data, passing it to the transformation function and then Loading (or writing) the results to their ultimate destination. Testing is simplified, as mock or test data can be passed to the transformation function and the results explicitly verified, which would not be possible if all of the ETL code resided in `main()` and referenced production data sources and destinations.

More generally, transformation functions should be designed to be _idempotent_. This is a technical way of saying that the repeated application of the transformation function should have no impact on the fundamental state of output data, until the moment the input data changes. One of the key advantages of idempotent ETL jobs, is that they can be set to run repeatedly (e.g. by using `cron` to trigger the `spark-submit` command above, on a pre-defined schedule), rather than having to factor-in potential dependencies on other ETL jobs completing successfully.

ETL 作业的结构 为了便于调试和测试，我们建议将“转换”步骤与“提取”和“加载”步骤分离，放入一个独立的函数中——该函数以 DataFrame 形式接收输入数据，并返回转换后的数据作为单个 DataFrame。这样，围绕转换函数的代码可以专注于从数据源提取数据，将其传递给转换函数，然后将结果加载（或写入）到最终目的地。测试也变得更简单，因为可以将模拟或测试数据传递给转换函数，并明确验证结果。如果所有 ETL 代码都驻留在 main() 函数中，并引用生产数据源和目的地，则无法实现这种简化的测试方法。

更一般地来说，转换函数应设计为幂等的。技术上讲，幂等意味着重复应用转换函数不会影响输出数据的根本状态，直到输入数据发生变化为止。幂等的 ETL 作业的一个关键优势是，它们可以重复运行（例如，通过使用 cron 在预定义的时间表上触发 spark-submit 命令），而不必考虑其他 ETL 作业成功完成的潜在依赖关系。

## Passing Configuration Parameters to the ETL Job

Although it is possible to pass arguments to `etl_job.py`, as you would for any generic Python module running as a 'main' program  - by specifying them after the module's filename and then parsing these command line arguments - this can get very complicated, very quickly, especially when there are lot of parameters (e.g. credentials for multiple databases, table names, SQL snippets, etc.). This also makes debugging the code from within a Python interpreter extremely awkward, as you don't have access to the command line arguments that would ordinarily be passed to the code, when calling it from the command line.

A much more effective solution is to send Spark a separate file - e.g. using the `--files configs/etl_config.json` flag with `spark-submit` - containing the configuration in JSON format, which can be parsed into a Python dictionary in one line of code with `json.loads(config_file_contents)`. Testing the code from within a Python interactive console session is also greatly simplified, as all one has to do to access configuration parameters for testing, is to copy and paste the contents of the file - e.g.,

虽然可以像运行任何通用的 Python 模块一样，通过在模块文件名后指定参数并解析这些命令行参数，将参数传递给 etl_job.py，但当参数较多时（例如，多个数据库的凭证、表名、SQL 片段等），这种方法很快就会变得非常复杂。这也使得从 Python 解释器中调试代码变得极其不便，因为在调用代码时，你无法访问通常从命令行传递的参数。

一个更有效的解决方案是通过 spark-submit 发送一个单独的文件给 Spark，例如使用 --files configs/etl_config.json 标志，该文件包含 JSON 格式的配置，可以通过 json.loads(config_file_contents) 一行代码解析为 Python 字典。从 Python 交互式控制台会话中测试代码也变得简单得多，因为访问测试配置参数所需要做的仅仅是复制并粘贴文件内容，例如:
```python
import json

config = json.loads("""{"field": "value"}""")
```

For the exact details of how the configuration file is located, opened and parsed, please see the `start_spark()` function in `dependencies/spark.py` (also discussed further below), which in addition to parsing the configuration file sent to Spark (and returning it as a Python dictionary), also launches the Spark driver program (the application) on the cluster and retrieves the Spark logger at the same time.

关于配置文件如何被定位、打开和解析的具体细节，请参见 dependencies/spark.py 中的 start_spark() 函数（下文也有进一步讨论）。该函数不仅解析传递给 Spark 的配置文件（并将其作为 Python 字典返回），还启动集群上的 Spark 驱动程序（应用程序），同时检索 Spark 的日志记录器。

## Packaging ETL Job Dependencies

In this project, functions that can be used across different ETL jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

在这个项目中，可以跨不同 ETL 作业使用的函数保存在一个名为 dependencies 的模块中，并在特定作业模块中引用，例如：

```python
from dependencies.spark import start_spark
```

This package, together with any additional dependencies referenced within it, must be copied to each Spark node for all jobs that use `dependencies` to run. This can be achieved in one of several ways:

1. send all dependencies as a `zip` archive together with the job, using `--py-files` with Spark submit;
2. formally package and upload `dependencies` to somewhere like the `PyPI` archive (or a private version) and then run `pip3 install dependencies` on each node; or,
3. a combination of manually copying new modules (e.g. `dependencies`) to the Python path of each node and using `pip3 install` for additional dependencies (e.g. for `requests`).

Option (1) is by far the easiest and most flexible approach, so we will make use of this for now. To make this task easier, especially when modules such as `dependencies` have additional dependencies (e.g. the `requests` package), we have provided the `build_dependencies.sh` bash script for automating the production of `packages.zip`, given a list of dependencies documented in `Pipfile` and managed by the `pipenv` python application (discussed below).

此包以及其中引用的任何额外依赖项，必须复制到每个 Spark 节点，以便所有使用 dependencies 的作业能够运行。这可以通过以下几种方式实现：

使用 --py-files 标志与 Spark 提交作业时将所有依赖项作为 zip 压缩包一起发送；
正式打包并将 dependencies 上传到类似 PyPI 存储库（或私有版本），然后在每个节点上运行 pip3 install dependencies；
结合手动将新模块（例如 dependencies）复制到每个节点的 Python 路径和使用 pip3 install 安装其他依赖项（例如 requests）。
选项 (1) 是目前最简单且最灵活的方法，因此我们将暂时使用它。为了使此任务更容易，特别是当像 dependencies 这样的模块有额外依赖项（例如 requests 包）时，我们提供了 build_dependencies.sh bash 脚本来自动生成 packages.zip，该脚本根据记录在 Pipfile 中的依赖项列表并由 pipenv Python 应用程序管理（详见下文）。

## Running the ETL job

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--packages 'com.somesparkjar.dependency:1.0.0' \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--packages 'com.somesparkjar.dependency:1.0.0,...'` - Maven coordinates for any JAR dependencies required by the job (e.g. JDBC driver for connecting to a relational database);
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job.py` - the Python module file containing the ETL job to execute.

Full details of all possible options can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html). Note, that we have left some options to be defined within the job (which is actually a Spark application) - e.g. `spark.cores.max` and `spark.executor.memory` are defined in the Python script as it is felt that the job should explicitly contain the requests for the required cluster resources.

简要说明所提供的选项的作用：

- master local[*] - 指定启动作业的 Spark 集群地址。如果你有一个正在运行的 Spark 集群（无论是本地的单执行器模式，还是云端的更大规模集群），并希望将作业发送到那里，可以使用适当的 Spark IP 进行修改 例如 spark://the-clusters-ip-address:7077；
- packages 'com.somesparkjar.dependency:1.0.0,...' - Maven 坐标，用于指定作业所需的任何 JAR 依赖项（例如，用于连接关系数据库的 JDBC 驱动程序）；
- files configs/etl_config.json - （可选）指定 ETL 作业所需的配置文件路径；
- py-files packages.zip - 包含作业引用的 Python 依赖项（模块）的压缩包；
- jobs/etl_job.py - 包含要执行的 ETL 作业的 Python 模块文件。

有关所有可能选项的详细信息可以在 此处 找到。请注意，我们将某些选项留给作业（实际上是 Spark 应用程序）中定义，例如 spark.cores.max 和 spark.executor.memory 是在 Python 脚本中定义的，因为我们认为作业应该明确包含对所需集群资源的请求

## Debugging Spark Jobs Using `start_spark`

It is not practical to test and debug Spark jobs by sending them to a cluster using `spark-submit` and examining stack traces for clues on what went wrong. A more productive workflow is to use an interactive console session (e.g. IPython) or a debugger (e.g. the `pdb` package in the Python standard library or the Python debugger in Visual Studio Code). In practice, however, it can be hard to test and debug Spark jobs in this way, as they implicitly rely on arguments that are sent to `spark-submit`, which are not available in a console or debug session.

We wrote the `start_spark` function - found in `dependencies/spark.py` - to facilitate the development of Spark jobs that are aware of the context in which they are being executed - i.e. as `spark-submit` jobs or within an IPython console, etc. The expected location of the Spark and job configuration parameters required by the job, is contingent on which execution context has been detected. The docstring for `start_spark` gives the precise details,

通过使用 spark-submit 将 Spark 作业发送到集群并检查堆栈跟踪来找出问题并不是一种实用的调试方式。更有效的工作流程是使用交互式控制台会话（例如 IPython）或调试器（例如 Python 标准库中的 pdb 包或 Visual Studio Code 中的 Python 调试器）。然而，实际上以这种方式测试和调试 Spark 作业可能会很困难，因为它们隐式依赖于传递给 spark-submit 的参数，这些参数在控制台或调试会话中不可用。

为了解决这个问题，我们编写了 start_spark 函数（位于 dependencies/spark.py），以方便开发能够识别其执行上下文的 Spark 作业——例如，作业是在 spark-submit 中运行，还是在 IPython 控制台中运行等。作业所需的 Spark 和作业配置参数的预期位置，取决于检测到的执行上下文。有关 start_spark 的精确细节，请参阅它的文档字符串。

```python
def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # ...

    return spark_sess, spark_logger, config_dict
```

For example, the following code snippet,

```python
spark, log, config = start_spark(
    app_name='my_etl_job',
    jar_packages=['com.somesparkjar.dependency:1.0.0'],
    files=['configs/etl_config.json'])
```

Will use the arguments provided to `start_spark` to setup the Spark job if executed from an interactive console session or debugger, but will look for the same arguments sent via `spark-submit` if that is how the job has been executed.

## Automated Testing

In order to test with Spark, we use the `pyspark` Python package, which is bundled with the Spark JARs required to programmatically start-up and tear-down a local Spark instance, on a per-test-suite basis (we recommend using the `setUp` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite). Note, that using `pyspark` to run Spark is an alternative way of developing with Spark as opposed to using the PySpark shell or `spark-submit`.

Given that we have chosen to structure our ETL jobs in such a way as to isolate the 'Transformation' step into its own function (see 'Structure of an ETL job' above), we are free to feed it a small slice of 'real-world' production data that has been persisted locally - e.g. in `tests/test_data` or some easily accessible network directory - and check it against known results (e.g. computed manually or interactively within a Python interactive console session).

To execute the example unit test for this project run,

为了在 Spark 中进行测试，我们使用了 pyspark Python 包，该包包含启动和关闭本地 Spark 实例所需的 Spark JAR 文件，并在每个测试套件的基础上进行（我们建议使用 unittest.TestCase 中的 setUp 和 tearDown 方法来为每个测试套件执行一次此操作）。请注意，使用 pyspark 运行 Spark 是一种与使用 PySpark shell 或 spark-submit 相对的 Spark 开发方式。

鉴于我们选择以将“转换”步骤隔离到其独立函数的方式来构建 ETL 作业（见上文“ETL 作业的结构”），我们可以将一小部分本地持久化的“真实世界”生产数据——例如，保存在 tests/test_data 中或某个易于访问的网络目录中——传递给转换函数，并将其结果与已知的结果进行比较（例如，手动或在 Python 交互控制台中计算出的结果）。

要执行本项目的示例单元测试，请运行：

```bash
pipenv run python -m unittest tests/test_*.py
```

If you're wondering what the `pipenv` command is, then read the next section.

## Managing Project Dependencies using Pipenv

We use [pipenv](https://docs.pipenv.org) for managing project dependencies and Python environments (i.e. virtual environments). All direct packages dependencies (e.g. NumPy may be used in a User Defined Function), as well as all the packages used during development (e.g. PySpark, flake8 for code linting, IPython for interactive console sessions, etc.), are described in the `Pipfile`. Their **precise** downstream dependencies are described in `Pipfile.lock`.

我们使用 pipenv 来管理项目依赖项和 Python 环境（即虚拟环境）。所有直接的包依赖项（例如，NumPy 可能在用户定义函数中使用）以及开发期间使用的所有包（例如，PySpark、用于代码检查的 flake8、用于交互式控制台会话的 IPython 等），都在 Pipfile 中进行了描述。它们的 精确 下游依赖项在 Pipfile.lock 中进行了描述。

### Installing Pipenv

To get started with Pipenv, first of all download it - assuming that there is a global version of Python available on your system and on the PATH, then this can be achieved by running the following command,

```bash
pip3 install pipenv
```

Pipenv is also available to install from many non-Python package managers. For example, on OS X it can be installed using the [Homebrew](https://brew.sh) package manager, with the following terminal command,

```bash
brew install pipenv
```

For more information, including advanced configuration options, see the [official pipenv documentation](https://docs.pipenv.org).

### Installing this Projects' Dependencies

Make sure that you're in the project's root directory (the same one in which the `Pipfile` resides), and then run,

```bash
pipenv install --dev
```

This will install all of the direct project dependencies as well as the development dependencies (the latter a consequence of the `--dev` flag).

### Running Python and IPython from the Project's Virtual Environment

In order to continue development in a Python environment that precisely mimics the one the project was initially developed with, use Pipenv from the command line as follows,

为了在一个准确模拟项目最初开发环境的 Python 环境中继续开发，可以使用以下命令运行 Pipenv：

```bash
pipenv run python3
```

The `python3` command could just as well be `ipython3`, for example,

```bash
pipenv run ipython
```

This will fire-up an IPython console session *where the default Python 3 kernel includes all of the direct and development project dependencies* - this is our preference.

这将启动一个 IPython 控制台会话 ，其中默认的 Python 3 内核包括所有直接和开发项目依赖项——这是我们的首选方式。

### Pipenv Shells

Prepending `pipenv` to every command you want to run within the context of your Pipenv-managed virtual environment can get very tedious. This can be avoided by entering into a Pipenv-managed shell,

在 Pipenv 管理的虚拟环境中运行每个命令时，前缀 pipenv 可能会变得非常繁琐。可以通过进入一个 Pipenv 管理的 shell 来避免这种情况：

```bash
pipenv shell
```

This is equivalent to 'activating' the virtual environment; any command will now be executed within the virtual environment. Use `exit` to leave the shell session.

这相当于“激活”虚拟环境；现在任何命令都将在虚拟环境中执行。使用 exit 退出 shell 会话。

### Automatic Loading of Environment Variables

Pipenv will automatically pick-up and load any environment variables declared in the `.env` file, located in the package's root directory. For example, adding,

Pipenv 会自动加载 .env 文件中声明的任何环境变量，该文件位于包的根目录中。例如，添加：

```bash
SPARK_HOME=applications/spark-2.3.1/bin
DEBUG=1
```

Will enable access to these variables within any Python program -e.g. via a call to `os.environ['SPARK_HOME']`. Note, that if any security credentials are placed here, then this file **must** be removed from source control - i.e. add `.env` to the `.gitignore` file to prevent potential security risks.

将使得在任何 Python 程序中访问这些变量成为可能——例如，通过调用 os.environ['SPARK_HOME']。注意，如果 .env 文件中包含任何安全凭证，则 必须 从源代码控制中移除该文件——即将 .env 添加到 .gitignore 文件中，以防止潜在的安全风险。