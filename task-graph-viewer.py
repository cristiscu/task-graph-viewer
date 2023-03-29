"""
Created By:    Cristian Scutaru
Creation Date: Mar 2023
Company:       XtractPro Software
"""

import os, sys
import configparser
import snowflake.connector
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

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

def getRootTasks(database, schema, cur):
    """
    Get all root tasks in the database schema
    """

    # get tasks
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
        if str(row[13]) != 'None': task.allow_overlap = bool(row[13])
        task.created_on = str(row[0])
        task.id = str(row[2])
        task.warehouse = str(row[7])
        task.schedule = str(row[8])
    
    return tasks

def getTaskGraph(tasks):
    """
    Generates and returns a graph of all tasks in DOT notation
    """

    nodes = ""; edges = ""
    for name in tasks:
        task = tasks[name]
        nodes += (f'  {name} [ label=<<table style="rounded" border="0" cellborder="0" cellspacing="0" cellpadding="1">\n'
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
        + f'  node [ style="filled" shape="Mrecord" color="SkyBlue" fillcolor="#f5f5f5" color="#6c6c6c" penwidth="1" ]\n'
        + f'  edge [ penwidth="1" color="#696969" dir="forward" style="solid" ]\n\n'
        + f'{nodes}\n{edges}}}\n')

def saveHtml(filename, s):
    """
    save in HTML file using d3-graphviz
    https://bl.ocks.org/magjac/4acffdb3afbc4f71b448a210b5060bca
    https://github.com/magjac/d3-graphviz#creating-a-graphviz-renderer
    """
    s = ('<!DOCTYPE html>\n'
        + '<meta charset="utf-8">\n'
        + '<body>'
        + '<script src="https://d3js.org/d3.v5.min.js"></script>\n'
        + '<script src="https://unpkg.com/@hpcc-js/wasm@0.3.11/dist/index.min.js"></script>\n'
        + '<script src="https://unpkg.com/d3-graphviz@3.0.5/build/d3-graphviz.js"></script>\n'
        + '<div id="graph" style="text-align: center;"></div>\n'
        + '<script>\n'
        + 'var graphviz = d3.select("#graph").graphviz()\n'
        + '   .on("initEnd", () => { graphviz.renderDot(d3.select("#digraph").text()); });\n'
        + '</script>\n'
        + '<textarea id="digraph" style="display:none;">\n'
        + s
        + '</textarea>\n')

    print(f"Generating {filename} file...")
    with open(filename, "w") as file:
        file.write(s)

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

    # single task name?
    taskName = sys.argv[1] if len(sys.argv) >= 2 else None
    if taskName == None:
        # show all root task names
        print(f"The root tasks in the {database}.{schema} database schema:")
        for name in taskNames: print(f"   {name}");
    elif taskName not in taskNames:
        print(f"{taskName} is not a root tasks in the {database}.{schema} database schema!")
        sys.exit(2)
    else:
        taskNames = [taskName]

    tasks = getAllTasks(database, schema, cur)
    con.close()

    # remove tasks with a different root that current root task
    if taskName != None:
        tasks2 = {}
        for parent in tasks:
            task = tasks[parent]
            if task.hasRootTask(tasks, taskName):
                tasks2[parent] = task
        tasks = tasks2

    runID = sys.argv[2] if len(sys.argv) >= 3 else None
    if runID == None:
        # get DOT digraph string
        s = getTaskGraph(tasks)
        print("\nGenerated DOT digraph:")
        print(s)

    # save as HTML file
    filename = f"output/{account}-{database}.{schema}"
    if taskName != None: filename += f".{taskName}"
    if runID != None: filename += f"-{runID}"
    filename += ".html"
    saveHtml(filename, s)

if __name__ == "__main__":
    main()
