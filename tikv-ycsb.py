#!/usr/bin/python

import argparse
import shlex
import subprocess
import time
import sys
import os
import glob
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from fabric import Connection

# we support a few different experiment types
# - disk measurement
# - YCSB

# tikv port number configurations.
@dataclass
class TiKVConfig:
    pd_port: int = 2379
    pd_peer_port: int = 2380
    tikv_addr_port: int = 20160
    tikv_status_port: int = 20180

# important paths on our nodes
@dataclass
class NodeConfig:
    home: str = str(Path.home())
    exe: str = '/software'
    data: str = '/mnt/data'

    @property
    def ycsb_home(self):
        return f'{self.exe}/go-ycsb'

    @property
    def ycsb_exe(self):
        return f'{self.ycsb_home}/bin/go-ycsb'

    @property
    def tikv_exe(self):
        return f'{self.exe}/tikv-xll/target/release/tikv-server'

    @property
    def pd_exe(self):
        return f'{self.exe}/pd/bin/pd-server'

    @property
    def workload_path(self):
        return f'{self.ycsb_home}/workloads/workload'

# launch parameters for go-ycsb. Somewhat simplified... these are the
# ones which actually change per-experiment.
@dataclass
class YcsbConfig:
    pd: str = ""
    threads: int = 0
    fieldcount: int = 0
    fieldlength: int = 0
    operationcount: int = 0
    recordcount: int = 0

# global parameters for this experiment
@dataclass
class ExperimentConfig:
    dbnodes: list[str] = field(default_factory=list)
    monitornode: str = ""
    clientnodes: list[str] = field(default_factory=list)
    expname: str = ""
    valuesize: int = 0
    dbsize: int = 0
    ops: int = 0
    threads: int = 0
    threadsmin: int = 0
    outdirectory: str = ""

# <service>-i => (pid, Connection)
running_pids = {}
tikvconf = TiKVConfig()
nodeconf = NodeConfig()
expconf = ExperimentConfig()

def start_remotely(conn, cmd, name, log_file=None):
    """
Start a remote process using nohup and add its PID to a global dict.
"""
    nohupcmd = f"nohup {cmd} < /dev/null > {log_file} 2>&1 & echo $!"
    print(f'running {nohupcmd}')
    res = conn.run(nohupcmd, hide=True)
    pid = res.stdout.strip()
    running_pids[name] = (pid, conn)
    print(f'added {name} -> {pid}')


def build_cmd(exe, options):
    cmdlist = [exe]
    for key, value in options.items():
        cmdlist.append(f'--{key}=\"{value}\"')
    return ' '.join(cmdlist)

def start_pd(conn):
    print("starting pd...")    
    pd_options = {
        'name': 'pd',
        'data-dir': f'{nodeconf.data}/pd',
        'client-urls': f'http://{expconf.monitornode}:{tikvconf.pd_port}',
        'peer-urls': f'http://{expconf.monitornode}:{tikvconf.pd_peer_port}',
        'log-file': f'{nodeconf.home}/pd.log'
    }

    cmd = build_cmd(nodeconf.pd_exe, pd_options)
    start_remotely(conn, cmd, f'pd-0', f'runpd.log')

def start_tikv(tikvconns):
    print("starting db...")    
    for i, conn in enumerate(tikvconns):
        tikv_options = {
            'pd-endpoints': f'{expconf.monitornode}:{tikvconf.pd_port}',
            'addr': f'{expconf.dbnodes[i]}:{tikvconf.tikv_addr_port}',
            'status-addr': f'{expconf.dbnodes[i]}:{tikvconf.tikv_status_port}',
            'data-dir': f'{nodeconf.data}/tikv-data',
            'log-file': f'{nodeconf.home}/tikv.log'
        }
        cmd = build_cmd(f'sudo systemd-run --scope -p MemoryMax=32G --setenv=RUST_BACKTRACE=1 {nodeconf.tikv_exe}', tikv_options)
        start_remotely(conn, cmd, f'tikv-{i}', f'run-tikv-{i}.log')
        time.sleep(3)

def start_disk_measurement(nodes):
    for i, conn in enumerate(nodes):
        grouppid, tikvconn = running_pids[f'tikv-{i}']
        res = conn.run('pgrep -f tikv-server')
        tikvpid = res.stdout.strip().splitlines()[1]
        print(f'CGROUP PID {grouppid} TIKV-SERVER PID {tikvpid}')
        cmd = f'sudo nsenter -t {tikvpid} -p -n -u -i -C strace -r -e trace=%file,write,fsync -o {expconf.expname}-{expconf.dbsize}.strace -fp {tikvpid}'
        start_remotely(conn, cmd, f'strace-{i}', f'strace-{i}.log')

        cmd = f'sudo blktrace -d /dev/sdb'
        start_remotely(conn, cmd, f'blktrace-{i}', f'blktrace-{i}.summary')

def generate_ycsb_opts():
    opts = YcsbConfig()
    opts.pd = f'{expconf.monitornode}:2379'
    opts.fieldcount = 8
    opts.fieldlength = expconf.valuesize // opts.fieldcount
    size = opts.fieldcount * opts.fieldlength
    opts.recordcount = int(expconf.dbsize // size)
    opts.operationcount = expconf.ops // len(expconf.clientnodes)
    opts.threads = expconf.threads

    return opts

def load_ycsb(clientconns, threads):
    opts = generate_ycsb_opts()
    opts.threads = threads
    
    optlist = [f'tikv.pd={opts.pd}', 'tikv.type=raw', f'threadcount={opts.threads}',
               f'fieldcount={opts.fieldcount}', f'fieldlength={opts.fieldlength}',
               f'operationcount={opts.operationcount}', f'recordcount={opts.recordcount}', 'dotransactions=false']

    # run ycsb on each client node
    basename = f'load_threads_{opts.threads}_client'
    incr = opts.recordcount // len(expconf.clientnodes)
    jobs = []

    # TODO run_in_parallel doesn't support this command type well...
    for i, client in enumerate(clientconns):
        cmd = build_ycsb_cmd('load', f'{nodeconf.workload_path}a', optlist + [f'insertstart={i * incr}', f'insertcount={incr}'])
        job = client.run(f'{" ".join(cmd)} > {nodeconf.home}/{basename}_{i}.ycsb', asynchronous=True)
        jobs.append(job)
        
    # wait for all the clients to finish
    for job in jobs:
        job.join()

    # get output from the client nodes
    for i, client in enumerate(clientconns):
        client.get(f'{nodeconf.home}/{basename}_{i}.ycsb', f'{expconf.outdirectory}/{basename}_{i}.ycsb')

def run_in_parallel(cmd, conns, outfile=None, extension=None):
    """Run cmd on all connections in parallel and return after all are finished."""
    jobs = []
    for i, c in enumerate(conns):
        if not outfile:
            newcmd = cmd
        else: # redirect to an output file if wanted
            newcmd = f'{cmd} > {nodeconf.home}/{outfile}_{i}.{extension}'
            print(newcmd)
            
        job = c.run(newcmd, asynchronous=True, warn=True)
        jobs.append(job)

    for job in jobs:
        job.join()

    # if we have an output file, gather output from each client
    if outfile:
        for i, client in enumerate(conns):
            client.get(f'{nodeconf.home}/{outfile}_{i}.{extension}', f'{expconf.outdirectory}/{outfile}_{i}.{extension}')
    
def run_ycsb_workloads(workloads, conns, clientconns):
    opts = generate_ycsb_opts()

    # add one to force at least once number if threadsmin and threads are the same
    # we use threadsmin for scalability tests. threads are divided amongst client nodes
    for threads in range(expconf.threadsmin, expconf.threads + 1, 8):
        opts.threads = threads // len(expconf.clientnodes)
        optlist = [f'tikv.pd={opts.pd}', 'tikv.type=raw', f'threadcount={opts.threads}',
                   f'fieldcount={opts.fieldcount}', f'fieldlength={opts.fieldlength}',
                   f'operationcount={opts.operationcount}', f'recordcount={opts.recordcount}', 'dotransactions=false']

        # workloads
        for w in workloads:
            # drop caches on TiKV nodes
            run_in_parallel("echo 3 | sudo tee /proc/sys/vm/drop_caches", conns)
            time.sleep(5)

            cmd = build_ycsb_cmd('run', f'{nodeconf.workload_path}{w}', optlist)
            jobs = []

            # run ycsb on each client node
            run_in_parallel(f'{" ".join(cmd)}', clientconns, f'run_{w}_threads_{opts.threads}_client', 'ycsb')

def kill_service(pname):
    for name, info in running_pids.items():
        if name.startswith(pname):
            pid, conn = info
            conn.run(f'sudo pkill -9 {pname}')
            
def shutdown_services():
    # strace and blktrace should fail silently if they were never started
    kill_service("strace")
    kill_service("blktrace")
    kill_service("tikv")
    kill_service("pd")

def collect_output(threads):
    """Consolidate experiment output in local results directory.
Remote output: TiKV logs, strace out, blktrace out.
Local output: ycsb log
We create a subdir for each remote node.
"""
    # files from remote
    for name, info in running_pids.items():
        vals = name.split("-")
        nodeindex = vals[1]
        destdir = f'{expconf.outdirectory}/{nodeindex}'
        os.makedirs(destdir, exist_ok=True)

        # collect logs
        _, conn = info
        if vals[0].startswith("tikv"):
            conn.get(f'{nodeconf.home}/tikv.log', f'{destdir}/tikv-{threads}.log')

            # collect tikv metrics
            tikvip = expconf.dbnodes[int(nodeindex)]
            curlmetrics = ['curl', f'http://{tikvip}:{tikvconf.tikv_status_port}/metrics']
            with open(f'{destdir}/tikv-{threads}.metrics', 'w') as outfile:
                print(f'get metrics: {curlmetrics}')
                p = subprocess.Popen(curlmetrics, stdout=outfile)
                p.wait()
        elif vals[0].startswith("pd"):
            conn.get(f'{nodeconf.home}/pd.log', f'{destdir}/pd.log')
        elif vals[0].startswith("strace"):
            conn.get(f'{nodeconf.home}/{expconf.expname}-{expconf.dbsize}.strace', f'{destdir}/strace.out')
        elif vals[0].startswith("blktrace"):
            for i in range(32):
                conn.get(f'{nodeconf.home}/sdb.blktrace.{i}', f'{destdir}/sdb.blktrace.{i}')
        else:
            print("tried to collect output from unknown service")
    
def cleanup_services():
    for name, info in running_pids.items():
        _, conn = info
        if name.startswith("tikv"):
            conn.run(f'sudo rm {nodeconf.home}/tikv.log')
            conn.run(f'sudo rm -rf {nodeconf.data}/tikv-data')
        elif name.startswith("pd"):
            conn.run(f'sudo rm {nodeconf.home}/pd.log')
            conn.run(f'sudo rm -rf {nodeconf.data}/pd')
        elif name.startswith("strace"):
            conn.run(f'sudo rm {nodeconf.home}/{expconf.expname}-{expconf.dbsize}.strace')
        elif name.startswith("blktrace"):
            conn.run(f'sudo rm {nodeconf.home}/sdb.blktrace.*')
        else:
            print("tried to cleanup unknown service")

def build_ycsb_cmd(cmdtype, workload, opts):
    cmd = [nodeconf.ycsb_exe, cmdtype, 'tikv', '-P', workload]
    for o in opts:
        cmd.extend(['-p', o])

    print(cmd)
    return cmd

# ./measure-writes.py --tikv_nodes 10.10.1.2,10.10.1.3,10.10.1.4 --pd_node 10.10.1.1 --client_node 10.10.1.5 -s $((100 * 1024 * 1024 * 1024)) -v 16384 --workloads a,b,c,d,e,f --name helloworld --experimenttype ycsb
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tikv_nodes", type=str, required=True,
                        help="TiKV node IPs as comma-separated list")
    parser.add_argument("--pd_node", type=str, required=True,
                        help="PD IP")
    parser.add_argument("--client_nodes", type=str, required=True,
                        help="Client IPs as comma-separated list")
    parser.add_argument("-v", "--vsize", type=int, default=1024,
                        help="value size (1KB)")
    parser.add_argument("-s", "--db_size", type=int, default=(10 * 1024 * 1024 * 1024),
                        help="database size (10GB)")
    parser.add_argument("-o", "--ops", type=int, default=5000000,
                        help="workload operations (5 mil)")
    parser.add_argument("-r", "--threads", type=int, default=64,
                        help="go-ycsb threads (64 default)")
    parser.add_argument("--threadsmin", type=int, default=None,
                        help="minimum threads for scalability testing")
    parser.add_argument("-c", "--cleanup", action='store_true',
                        help="cleanup")
    parser.add_argument("-n", "--name", type=str, default='',
                        help="experiment name (used in results directory)")
    parser.add_argument("--experimenttype", type=str, required=True,
                        help="experiment type: disk_measurement OR ycsb")
    parser.add_argument("--workloads", type=str, default=None,
                        help="YCSB workloads by lower-case letter name as comma-separated list")

    return  parser.parse_args()

def main():
    # arguments and setup experiment parameters
    args = get_args()
    experimenttype = args.experimenttype.strip()
    expconf.expname = f'{args.name.strip()}-{args.db_size // (1024 * 1024 * 1024)}GB-{args.vsize // 1024}KB'
    expconf.monitornode = args.pd_node.strip()
    expconf.dbnodes = [ip.strip() for ip in args.tikv_nodes.split(',')]
    expconf.clientnodes = [ip.strip() for ip in args.client_nodes.split(',')]
    expconf.valuesize = args.vsize
    expconf.dbsize = args.db_size
    expconf.threads = args.threads
    expconf.threadsmin = args.threads if not args.threadsmin else args.threadsmin
    expconf.ops = args.ops
    expconf.outdirectory = f'{os.getcwd()}/results/{expconf.expname}/{experimenttype}'
    workloads = [] if not args.workloads else [workload.strip() for workload in args.workloads.split(',')]

    os.makedirs(expconf.outdirectory, exist_ok=True)

    # open connections
    pdconn = Connection(host=expconf.monitornode, user=os.getlogin(), port=22)
    tikvconns = list(map(lambda c:
                         Connection(host=c, user=os.getlogin(), port=22),
                         expconf.dbnodes))
    clientconns = list(map(lambda c:
                           Connection(host=c, user=os.getlogin(), port=22),
                           expconf.clientnodes))

    # really ugly but we need a fresh start each time for writes
    if experimenttype == 'writescalability':
        for threads in range(expconf.threadsmin, expconf.threads + 1, 4):
            # not sure if this matters for write-only workload
            run_in_parallel("echo 3 | sudo tee /proc/sys/vm/drop_caches", tikvconns)
            time.sleep(5)

            clientthreads = threads // len(expconf.clientnodes)
            start_pd(pdconn)
            time.sleep(8)
            
            start_tikv(tikvconns)
            time.sleep(8)

            load_ycsb(clientconns, clientthreads)
            
            collect_output(threads)
            shutdown_services()
            time.sleep(5)
            cleanup_services()
        sys.exit(0)

    start_pd(pdconn)
    time.sleep(8)

    start_tikv(tikvconns)
    time.sleep(8)

    if experimenttype == 'disk_measurement':
        start_disk_measurement(tikvconns)
        time.sleep(2)

    load_ycsb(clientconns, expconf.threads // len(expconf.clientnodes))
    
    if experimenttype == 'ycsb':
        # this will handle a scalability workload if given threadsmin and threads
        run_ycsb_workloads(workloads, tikvconns, clientconns)

    collect_output(0)
    shutdown_services()
    time.sleep(5)
    cleanup_services()
    
if __name__ == "__main__":
    main()
