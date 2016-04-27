import sys
sys.path.append("../../../")

from jpyutils.runner import task_runner

verbose = True
log = None
#log = "log/"

def test_add_job():
    task = task_runner.MultiTaskRunner(log)
    task.add("sleep 1; ls -l", name = "test001")
    task.add("sleep 2; ls -l /Users/zero91", name = "test002", depends="test001")
    task.run(verbose=verbose)

def test_add_jobs_from_file():
    render_arguments = dict()
    render_arguments["LOCAL_ROOT"] = "../"
    render_arguments["HADOOP_BIN"] = "/home/zhangjian09/software/hadoop-client/hadoop/bin/hadoop"
    render_arguments["DATE"] = "2015-03-10"
    render_arguments["REF_DATE"] = "2015-03-18"
    render_arguments["HDFS_JOINED_LOG_DIR"] = "/app/ecom/fcr-opt/kr/zhangjian09/2015/data/join_kr_log"
    render_arguments["HDFS_ORIGIN_LOG_DIR"] = "/app/ecom/fcr-opt/kr/analytics"

    run_all = task_runner.MultiTaskRunner(log, render_arguments, parallel_degree=4)
    run_all.addf("conf/test.jobconf", "utf-8")
    run_all.lists()
    run_all.run(verbose=verbose)

    run_part = task_runner.MultiTaskRunner(log, render_arguments)
    run_part.addf("conf/test.jobconf", "utf-8")
    run_part.run("2,3,5-7,10-11", verbose=verbose)

if __name__ == '__main__':
    test_add_job()
    test_add_jobs_from_file()

