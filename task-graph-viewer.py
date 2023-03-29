"""
Created By:    Cristian Scutaru
Creation Date: Mar 2023
Company:       XtractPro Software
"""

import os, sys
import configparser
import snowflake.connector
from pathlib import Path
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# https://stackoverflow.com/questions/18426882/python-time-difference-in-milliseconds-not-working-for-me
def millis_interval(start, end):
    """start and end are datetime instances"""
    diff = end - start
    return int(diff.days * 24 * 60 * 60 * 1000 + diff.seconds * 1000 + diff.microseconds / 1000)

class Task:
    """
    Database task, with predecessors
    """
    
    def __init__(self, name, predecessors):
        self.name = name
        self.predecessors = predecessors

    def hasRootTask(self, tasks, taskName):
        if self.name == taskName: return True
        for parent in self.predecessors:
            if tasks[parent].hasRootTask(tasks, taskName):
                return True
        return False

    def getPredecessors(self):
        s = ''
        for parent in self.predecessors:
            if len(s) > 0: s += ','
            s += parent

        return s

class TaskRun:
    """
    Database task run, for a root task
    """
    
    def __init__(self, id,taskName, state,
                 scheduled_time, query_start_time, completed_time):
        self.id = id
        self.taskName = taskName
        self.state = state

        self.scheduled_time = scheduled_time.replace(tzinfo=None)
        self.query_start_time = query_start_time.replace(tzinfo=None)
        self.completed_time = completed_time.replace(tzinfo=None)

        self.duration1 = millis_interval(self.scheduled_time, self.query_start_time)
        self.duration2 = millis_interval(self.query_start_time, self.completed_time)
        self.percent = int(self.duration1 / (self.duration1 + self.duration2) * 100)

    def getChartData(self, tasks):
        return (f'\n[ "{self.taskName}", "{self.taskName}",'
            + f' new Date("{self.scheduled_time}"), new Date("{self.completed_time}"), null,'
            + f' {self.percent}, "{tasks[self.taskName].getPredecessors()}" ],')

    def dump(self, showTask):
        print(f'   {self.taskName if showTask else self.id}'
            + f' ({self.state}) {self.scheduled_time}'
            + f' -> {self.query_start_time} [{self.duration1} ms]'
            + f' -> {self.completed_time} [{self.duration2} ms]');

def getRootTasks(database, schema, cur):
    """
    Get all root tasks in the database schema
    """

    tasks = []
    query = f"show tasks in schema {database}.{schema}"
    results = cur.execute(query).fetchall()
    for row in results:
        name = str(row[1])
        predecessors = str(row[9])
        if predecessors == '[]': tasks.append(name)
    
    return tasks

def getAllTasks(database, schema, cur):
    """
    Get all tasks in the database schema
    """

    tasks = {}
    query = f"show tasks in schema {database}.{schema}"
    results = cur.execute(query).fetchall()
    for row in results:
        name = str(row[1])
        predecessors = str(row[9]).strip('[] \n').split(',')
        if '' in predecessors:
            predecessors = []
        else:
            predecessors = [parent.strip('" \n').split('.')[-1] for parent in predecessors]
        task = Task(name, predecessors)
        tasks[name] = task

        task.state = str(row[10])
        task.allow_overlap = 'None'
        if str(row[13]) != 'null': task.allow_overlap = bool(row[13])
        task.created_on = str(row[0])
        task.id = str(row[2])
        task.warehouse = str(row[7])
        task.schedule = str(row[8])
    
    return tasks

def getAllTaskRuns(taskName, cur):
    """
    Get all task runs for a task
    """

    runs = []
    query = (f'select run_id, state,\n'
        + f'  scheduled_time, query_start_time, completed_time\n'
        + f'from table(information_schema.task_history(task_name => \'{taskName}\'))\n'
        + f'order by query_start_time desc;')
    results = cur.execute(query).fetchall()
    for row in results:
        runs.append(TaskRun(str(row[0]), taskName, str(row[1]), row[2], row[3], row[4]))
    
    return runs

def getRunHistory(runID, cur):
    """
    Get all run history for a task run
    """

    runs = []
    query = (f'select name, state,\n'
        + f'  scheduled_time, query_start_time, completed_time\n'
        + f'from table(information_schema.task_history())\n'
        + f'where run_id = \'{runID}\'\n'
        + f'order by query_start_time;')
    results = cur.execute(query).fetchall()
    for row in results:
        runs.append(TaskRun(runID, str(row[0]), str(row[1]), row[2], row[3], row[4]))
    
    return runs

def getTaskGraph(tasks):
    """
    Generates and returns a graph of all tasks in DOT notation
    """

    nodes = ""; edges = ""
    for name in tasks:
        task = tasks[name]
        color = "#6c6c6c" if task.state == 'suspended' else 'SkyBlue'
        nodes += (f'  {name} [ color="{color}"\n'
            + f'  label=<<table style="rounded" border="0" cellborder="0" cellspacing="0" cellpadding="1">\n'
            + f'<tr><td bgcolor="#e0e0e0" align="center"><font color="#000000"><b>{name}</b></font></td></tr>\n'
            + f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>state</i>: {task.state}</font></td></tr>\n')
        if task.warehouse != 'None':
            nodes += f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>warehouse</i>: {task.warehouse}</font></td></tr>\n'
        nodes += (f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>id</i>: {task.id}</font></td></tr>\n'
            + f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>created on</i>: {task.created_on}</font></td></tr>\n')
        if task.allow_overlap != 'None':
            nodes += f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>allow overlap</i>: {task.allow_overlap}</font></td></tr>\n'
        if task.schedule != 'None':
            nodes += f'<tr><td align="left"><font color="#000000" point-size="12.0"><i>schedule</i>: {task.schedule}</font></td></tr>\n'
        nodes += f'</table>> ];\n'

        for parent in task.predecessors:
            edges += f'  {parent} -> {name};\n'

    return ('digraph G {\n'
        + f'  graph [ rankdir="LR" bgcolor="#ffffff" ]\n'
        + f'  node [ shape="Mrecord" style="filled" fillcolor="#f5f5f5" penwidth="1" ]\n'
        + f'  edge [ style="solid" color="#6c6c6c" penwidth="1" ]\n\n'
        + f'{nodes}\n{edges}}}')

def saveHtmlGraph(filename, content, title):
    """
    generate HTML file with embedded digraph, using d3-graphviz
    https://bl.ocks.org/magjac/4acffdb3afbc4f71b448a210b5060bca
    https://github.com/magjac/d3-graphviz#creating-a-graphviz-renderer
    """
    
    print(f"Generating {filename} file...")
    with open('./templates/dot-template.html', 'r') as file:
        template = file.read()
    with open(filename, "w") as file:
        file.write(template
            .replace('{{content}}', content)
            .replace('{{title}}', title))

def getTaskGraphRun(tasks, runs, cur):
    """
    Data for a task graph run
    """

    s = ''
    for run in runs: s += run.getChartData(tasks)
    return s

def saveHtmlChart(filename, content, title):
    """
    generate HTML file with embedded Gantt chart, using Google Charts
    https://developers.google.com/chart/interactive/docs/gallery/ganttchart
    """
    
    print(f"Generating {filename} file...")
    with open('./templates/gantt-template.html', 'r') as file:
        template = file.read()
    with open(filename, "w") as file:
        file.write(template
            .replace('{{content}}', content)
            .replace('{{title}}', title))

def connect(connect_mode, account, user, role, warehouse, database, schema):

    # (a) connect to Snowflake with SSO
    if connect_mode == "SSO":
        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            authenticator = "externalbrowser"
        )

    # (b) connect to Snowflake with username/password
    if connect_mode == "PWD":
        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            password = os.getenv('SNOWFLAKE_PASSWORD')
        )

    # (c) connect to Snowflake with key-pair
    if connect_mode == "KEY-PAIR":
        with open(f"{str(Path.home())}/.ssh/id_rsa_snowflake_demo", "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                password = None, # os.environ['SNOWFLAKE_PASSPHRASE'].encode(),
                backend = default_backend()
            )
        pkb = p_key.private_bytes(
            encoding = serialization.Encoding.DER,
            format = serialization.PrivateFormat.PKCS8,
            encryption_algorithm = serialization.NoEncryption())

        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            private_key = pkb
        )

def main():
    """
    Main entry point of the CLI
    """

    # read profiles_db.conf
    parser = configparser.ConfigParser()
    parser.read("profiles_db.conf")
    section = "default"
    account = parser.get(section, "account")
    user = parser.get(section, "user")
    role = parser.get(section, "role")
    warehouse = parser.get(section, "warehouse")
    database = parser.get(section, "database", fallback=None)
    schema = parser.get(section, "schema", fallback=None)

    # change this to connect in a different way: SSO / PWD / KEY-PAIR
    connect_mode = "PWD"
    con = connect(connect_mode, account, user, role, warehouse, database, schema)
    cur = con.cursor()

    # get all root task names in the database schema
    taskNames = getRootTasks(database, schema, cur)
    if len(taskNames) == 0:
        print(f"There are no root tasks in the {database}.{schema} database schema!")
        sys.exit(2)

    taskName = sys.argv[1] if len(sys.argv) >= 2 else None
    runID = sys.argv[2] if taskName != None and len(sys.argv) >= 3 else None

    # single task name?
    if taskName == None:
        # show all root task names
        print(f"The root tasks in the {database}.{schema} database schema:")
        for name in taskNames: print(f"   {name}");
    elif taskName not in taskNames:
        print(f"{taskName} is not a root tasks in the {database}.{schema} database schema!")
        sys.exit(2)
    else:
        taskNames = [taskName]

        # show all root task names
        if runID == None:
            runs = getAllTaskRuns(taskName, cur)
            print(f"Task runs for {taskName}:")
            for run in runs: run.dump(False)

    # get all tasks and remove those with a different root than current root task
    tasks = getAllTasks(database, schema, cur)
    if taskName != None:
        tasks2 = {}
        for parent in tasks:
            task = tasks[parent]
            if task.hasRootTask(tasks, taskName):
                tasks2[parent] = task
        tasks = tasks2

    # save HTML file with DOT digraph or Gantt chart for a task run
    if runID == None:
        title = f"{database}.{schema}"
        if taskName != None: title += f".{taskName}"
        filename = f"output/{account}-{title}.html"
        title = f"Task Graph {title}" if taskName != None else f"All Task Graphs in {title}"
        saveHtmlGraph(filename, getTaskGraph(tasks), title)
    else:
        # show run history for the given task run
        runs = getRunHistory(runID, cur)
        print(f"Run history for {runID} task run:")
        for run in runs: run.dump(True)

        filename = f"output/{account}-{database}.{schema}.{taskName}-{runID}.html"
        saveHtmlChart(filename, getTaskGraphRun(tasks, runs, cur), f"Task Graph Run {runID}")

    con.close()

if __name__ == "__main__":
    main()
