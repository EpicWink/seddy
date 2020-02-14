"""SWF decisions making."""

import typing as t


def make_decisions_dag(  # TODO: unit-test
    task: t.Dict[str, t.Any], spec: t.Dict[str, t.Any],
) -> t.List[t.Dict[str, t.Any]]:
    """Build decisions from workflow history using DAG-type workflow.

    Args:
        task: decision task
        spec: DAG workflow specification

    Returns:
        workflow decisions
    """

    def get(item_id, items, id_key):
        return next(item for item in items if item[id_key] == item_id)

    def schedule_task():
        assert task["events"][-1]["eventType"] == "WorkflowExecutionStarted"
        decision_attributes = {
            "activityId": activity_task["id"],
            "activityType": activity_task["type"],
            "input": task["events"][-1]["input"][activity_task["id"]],
        }
        if "heartbeat" in activity_task:
            decision_attributes["heartbeatTimeout"] = activity_task["hearbeat"]
        if "timeout" in activity_task:
            decision_attributes["startToCloseTimeout"] = activity_task["timeout"]
        if "task_list" in activity_task:
            decision_attributes["taskList"] = activity_task["task_list"]
        if "priority" in activity_task:
            decision_attributes["taskPriority"] = activity_task["priority"]
        decisions.append(
            {
                "decisionType": "ScheduleActivityTask",
                "scheduleActivityTaskDecisionAttributes": decision_attributes,
            }
        )

    # Build each activities' dependants
    dependants = {}
    for activity_task in spec["tasks"]:
        dependants_task = []
        for other_activity_task in spec["tasks"]:
            if activity_task["id"] in other_activity_task.get("dependencies", []):
                dependants_task.append(other_activity_task["id"])
        dependants[activity_task["id"]] = dependants_task

    # Build activity task scheduled map
    scheduled = {}
    at_attr_keys = {
        "ActivityTaskCompleted": "ActivityTaskCompletedEventAttributes",
        "ActivityTaskFailed": "ActivityTaskFailedEventAttributes",
        "ActivityTaskTimedOut": "ActivityTaskTimedOutEventAttributes",
        "ActivityTaskScheduled": "ActivityTaskScheduledEventAttributes",
        "ActivityTaskStarted": "ActivityTaskStartedEventAttributes",
    }
    for event in task["events"]:
        if event["eventType"] in at_attr_keys:
            if event["eventType"] == "ActivityTaskScheduled":
                scheduled_event = event
            else:
                scheduled_event = get(
                    event[at_attr_keys[event["eventId"]]]["scheduledEventId"],
                    task["events"],
                    "eventId",
                )
            scheduled[event[at_attr_keys[event["eventId"]]]] = scheduled_event

    # Build workflow status from events
    activity_task_events = {at["id"]: [] for at in spec["tasks"]}
    for event in reversed(task["events"]):  # events are newest-first
        if event["eventType"] in at_attr_keys:
            scheduled_event = scheduled[event["eventId"]]
            activity_task_events[scheduled_event["activityId"]].append(event)

    # Process new events
    assert task["events"][0]["eventType"] == "DecisionTaskScheduled"
    decisions = []
    for event in task["events"][1:]:
        if event["eventType"] == "DecisionTaskStarted":
            break  # all older events are already processed
        elif event["eventType"] == "ActivityTaskCompleted":
            # Schedule dependants
            scheduled_event = scheduled[event["eventId"]]
            dependants_task = dependants[scheduled_event["activityId"]]
            for activity_task_id in dependants_task:
                assert not activity_task_events[activity_task_id]
                activity_task = get(activity_task_id, spec["tasks"], "id")
                # Check dependencies satisfied
                for dependency_activity_task_id in activity_task["dependencies"]:
                    events = activity_task_events[dependency_activity_task_id]
                    if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                        break
                else:
                    schedule_task()
            # Complete workflow
            for events in activity_task_events.values():
                if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                    break
            else:
                result = {}
                for activity_id, events in activity_task_events.items():
                    assert events and events[-1]["eventType"] == "ActivityTaskCompleted"
                    result[activity_id] = (
                        events[-1]["ActivityTaskCompletedEventAttributes"]["result"]
                    )
                return [
                    {
                        "decisionType": "CompleteWorkflowExecution",
                        "CompleteWorkflowExecutionDecisionAttributes": {
                            "result": result
                        },
                    },
                ]
        elif event["eventType"] == "ActivityTaskFailed":
            return [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityFailure",
                        "details": "Activity '%s' failed" % (
                            scheduled[event["eventId"]]["activityId"]
                        ),
                    },
                },
            ]
        elif event["eventType"] == "ActivityTaskTimedOut":
            return [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityTimeOut",
                        "details": "Activity '%s' timed-out" % (
                            scheduled[event["eventId"]]["activityId"]
                        ),
                    },
                },
            ]
        elif event["eventType"] == "WorkflowExecutionCancelRequested":
            decisions = []
            for activity_task in spec["tasks"]:
                events = activity_task_events[activity_task["id"]]
                if events and events[-1]["eventType"] in (
                    "ActivityTaskScheduled", "ActivityTaskStarted"
                ):
                    decisions.append(
                        {
                            "decisionType": "RequestCancelActivityTask",
                            "RequestCancelActivityTaskDecisionAttributes": {
                                "activityId": activity_task["id"]
                            }
                        }
                    )
            decisions.append({"decisionType": "CancelWorkflowExecution"})
            return decisions
        elif event["eventType"] == "WorkflowExecutionStarted":
            assert event is task["events"][1]
            for activity_task in spec["tasks"]:  # schedule new activity tasks
                assert not activity_task_events[activity_task["id"]]
                if not activity_task["dependencies"]:
                    schedule_task()
    return decisions
