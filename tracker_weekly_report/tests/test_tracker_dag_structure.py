"""Tests for DAG structure, schedule, and task dependencies."""
import pytest


class TestDAGExists:
    def test_dag_object_exists(self, tracker_module):
        assert hasattr(tracker_module, "dag")

    def test_dag_id(self, tracker_module):
        assert tracker_module.dag.dag_id == "weekly_timesheet_review"


class TestDAGConfig:
    def test_schedule(self, tracker_module):
        assert tracker_module.dag.schedule_interval == "30 2-6 * * 1"

    def test_catchup_disabled(self, tracker_module):
        assert tracker_module.dag.catchup is False

    def test_tags(self, tracker_module):
        expected_tags = ["timesheet", "weekly", "review", "mantis", "timezone"]
        for tag in expected_tags:
            assert tag in tracker_module.dag.tags

    def test_description(self, tracker_module):
        assert "timesheet" in tracker_module.dag.description.lower()


class TestDAGTasks:
    # Module-level variable names that correspond to task objects
    TASK_VARS = [
        ("load_users_task", "load_whitelisted_users"),
        ("calc_week_task", "calculate_week_range"),
        ("filter_timezone_task", "filter_users_by_timezone"),
        ("fetch_timesheets_task", "fetch_all_timesheets"),
        ("enrich_timesheets_task", "enrich_timesheets_with_issue_details"),
        ("analyze_timesheets_task", "analyze_all_timesheets_with_ai"),
        ("send_emails_task", "compose_and_send_all_emails"),
        ("log_summary_task", "log_execution_summary"),
    ]

    def _get_tasks(self, tracker_module):
        """Return list of task objects from module-level variables."""
        return [getattr(tracker_module, var) for var, _ in self.TASK_VARS]

    def test_task_count(self, tracker_module):
        tasks = self._get_tasks(tracker_module)
        assert len(tasks) == 8

    def test_all_task_ids_present(self, tracker_module):
        for var_name, expected_id in self.TASK_VARS:
            task = getattr(tracker_module, var_name)
            assert task.task_id == expected_id, f"{var_name}.task_id should be {expected_id}"

    def test_linear_dependency_chain(self, tracker_module):
        """Verify the linear chain: load → calc → filter → fetch → enrich → analyze → email → summary."""
        tasks = self._get_tasks(tracker_module)
        for i in range(len(tasks) - 1):
            upstream = tasks[i]
            downstream = tasks[i + 1]
            assert downstream.task_id in upstream.downstream_task_ids, (
                f"{upstream.task_id} should have {downstream.task_id} as downstream"
            )
            assert upstream.task_id in downstream.upstream_task_ids, (
                f"{downstream.task_id} should have {upstream.task_id} as upstream"
            )

    def test_first_task_has_no_upstream(self, tracker_module):
        first_task = getattr(tracker_module, "load_users_task")
        assert len(first_task.upstream_task_ids) == 0
