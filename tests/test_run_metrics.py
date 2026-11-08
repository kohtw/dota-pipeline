import run_metrics as rm


def run(dag: str, state: str, run_type: str = "scheduled", duration: float | None = 10.0) -> dict:
    return {"dag_id": dag, "state": state, "run_type": run_type, "duration": duration}


def ti(task: str, state: str = "success", duration: float | None = 1.0, try_number: int = 1) -> dict:
    return {"dag_id": "d", "task_id": task, "state": state, "duration": duration, "try_number": try_number}


def test_percentile_nearest_rank():
    assert rm.percentile([], 95) is None
    assert rm.percentile([5.0], 95) == 5.0
    assert rm.percentile([float(i) for i in range(1, 101)], 95) == 95.0
    assert rm.percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.0


def test_run_success_counts_only_finished_runs_and_splits_scheduled():
    runs = [run("d", "success"), run("d", "failed"), run("d", "success", "manual"),
            run("d", "running", duration=None), run("e", "success", duration=4.0)]
    d, e = rm.summarize_runs(runs)
    assert (d.finished, d.succeeded, d.failed) == (3, 2, 1)
    assert d.success_rate == 2 / 3
    assert (d.scheduled_succeeded, d.scheduled_finished) == (1, 2)
    assert d.scheduled_success_rate == 0.5
    assert (e.dag_id, e.max_s) == ("e", 4.0)


def test_task_stats_pool_mapped_instances_and_ignore_skips():
    tis = [ti("parse", duration=4.0), ti("parse", duration=6.0, try_number=2), ti("parse", "failed", 1.0, 3),
           ti("parse", "skipped", None), ti("only_skipped", "skipped", None)]
    (parse,) = rm.summarize_tasks(tis)
    assert (parse.runs, parse.failed, parse.retried) == (3, 1, 3)
    assert (parse.median_s, parse.max_s) == (4.0, 6.0)


def test_markdown_has_one_row_per_dag_and_task():
    out = rm.markdown(rm.summarize_runs([run("d", "success")]), rm.summarize_tasks([ti("parse")]),
                      rm.dt.datetime(2026, 11, 1, tzinfo=rm.dt.timezone.utc))
    assert "| d | 1 | 100.0% | 100.0% (1/1) |" in out
    assert "| d | parse | 1 | 0 | 0 |" in out
