Snowflake Task Graph Viewer
===========================

Simple tool in Python to show Snowflake task graphs in static HTML files with generated SVG, and Gantt charts for task graph runs.

# Connect to Snowflake

Create a **profiles_db.conf** copy of the **profiles_db_template.conf** file, and customize it with your own Snowflake connection parameters. Your top [default] profile is the active profile, considered by our tool. Below you may define other personal profiles, that you may override under [default] each time you want to change your active connection.

We connect to Snowflake with the Snowflake Connector for Python. We have code for (a) password-based connection, (b) connecting with a Key Pair, and (c) connecting with SSO. For password-based connection, save your password in a SNOWFLAKE_PASSWORD local environment variable. Never add the password or any other sensitive information to your code or to profile files. All names must be case sensitive, with no quotes.

The database and schema are also mandatory: we always query all tasks from a database schema!

# CLI Executable File

To compile into a CLI executable:

**<code>pip install pyinstaller</code>**  
**<code>pyinstaller --onefile task-graph-viewer.py</code>**  
**<code>dist\task-graph-viewer</code>**  

# 1. Show all Task Graphs from current database schema

Connect with no parameters, to show all root task names in the current database schema. We also generate a DOT graph for all task graphs in the current database schema. The name of the generated HTML file - in the output subfolder - with the previous graph embeded as SVG. We show not just task names and their dependents, but inner information, such as status, id, warehouse etc. Example:
 
**<code>python task-graph-viewer.py</code>**  

Here is an example with two task graphs (T21 and T51 are root task names that you can use later):

![All Task Graphs](/images/schema-tasks.png)

# 2. Show the Topology of one single Task Graph

Connect with a root task name (case-sensitive, that you can get from the previous call), to show the different task graph runs you had for this task graph in the past few days. We also generate a DOT graph in a HTML file for this specific task graph only. Example:

**<code>python task-graph-viewer.py T51</code>**  

![Single Task Graph](/images/task-graph-51.png)

Here is the same graph from Snowflake:

![Single Task Graph in Snowflake](/images/task-graph-snowflake-T51.png.png)

# 3. Show a Gantt Chart for one single Task Graph Run - TODO

Connect with a root task name (case-sensitive!), and a run ID for this task (that you can get from the previous call). We'll generate a Gantt Chart for the execution of all related tasks. Example:

**<code>python task-graph-viewer.py T51 1680032510785</code>**  

![Single Task Graph Run](/images/task-graph-run-51.png)
