import multiprocessing
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple

import six

from dagster import check
from dagster.core.errors import DagsterUnmetExecutorRequirementsError
from dagster.core.serdes import whitelist_for_serdes
from dagster.core.utils import make_new_run_id
from dagster.utils import merge_dicts

EXECUTION_TIME_KEY = 'execution_epoch_time'


class RunConfig(
    namedtuple('_RunConfig', 'run_id tags reexecution_config step_keys_to_execute mode')
):
    '''
    Configuration that controls the details of how Dagster will execute a pipeline.

    Args:
        run_id (Optional[str]): The ID to use for this run. If not provided a new UUID will
            be created using `uuid4`.
        tags (Optional[dict[str, str]]): Key value pairs that will be added to logs.
        rexecution_config (Optional[RexecutionConfig]): Information about a previous run to allow
            for subset rexecution.
        step_keys_to_execute (Optional[list[str]]): The subset of steps from a pipeline to execute
            this run.
        mode (Optional[str]): The name of the mode in which to execute the pipeline.
    '''

    def __new__(
        cls, run_id=None, tags=None, reexecution_config=None, step_keys_to_execute=None, mode=None
    ):

        check.opt_list_param(step_keys_to_execute, 'step_keys_to_execute', of_type=str)

        tags = check.opt_dict_param(tags, 'tags', key_type=str)

        if EXECUTION_TIME_KEY in tags:
            tags[EXECUTION_TIME_KEY] = float(tags[EXECUTION_TIME_KEY])
        else:
            tags[EXECUTION_TIME_KEY] = time.time()

        return super(RunConfig, cls).__new__(
            cls,
            run_id=check.str_param(run_id, 'run_id') if run_id else make_new_run_id(),
            tags=tags,
            reexecution_config=check.opt_inst_param(
                reexecution_config, 'reexecution_config', ReexecutionConfig
            ),
            step_keys_to_execute=step_keys_to_execute,
            mode=check.opt_str_param(mode, 'mode'),
        )

    def with_tags(self, **new_tags):
        new_tags = merge_dicts(self.tags, new_tags)
        return RunConfig(**merge_dicts(self._asdict(), {'tags': new_tags}))

    def with_mode(self, mode):
        return RunConfig(**merge_dicts(self._asdict(), {'mode': mode}))


class ExecutorConfig(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    @abstractmethod
    def check_requirements(self, instance, system_storage_def):
        '''(void): Whether this executor config is valid given the instance and system storage'''

    @abstractmethod
    def get_engine(self):
        '''(IEngine): Return the configured engine.'''


class InProcessExecutorConfig(ExecutorConfig):
    def check_requirements(self, _instance, _system_storage_def):
        pass

    def get_engine(self):
        from dagster.core.engine.engine_inprocess import InProcessEngine

        return InProcessEngine


class MultiprocessExecutorConfig(ExecutorConfig):
    def __init__(self, handle, max_concurrent=None):
        from dagster import ExecutionTargetHandle

        # TODO: These gnomic process boundary/execution target handle exceptions should link to
        # a fuller explanation in the docs.
        # https://github.com/dagster-io/dagster/issues/1649
        self.handle = check.inst_param(
            handle,
            'handle',
            ExecutionTargetHandle,
            additional_message='Multiprocessing can only be configured when a pipeline is executed '
            'from an ExecutionTargetHandle: do not pass a pure in-memory pipeline definition.',
        )

        max_concurrent = max_concurrent if max_concurrent else multiprocessing.cpu_count()
        self.max_concurrent = check.int_param(max_concurrent, 'max_concurrent')

    def check_requirements(self, instance, system_storage_def):
        check_persistent_storage_requirement(system_storage_def)
        check_non_ephemeral_instance(instance)

    def get_engine(self):
        from dagster.core.engine.engine_multiprocess import MultiprocessEngine

        return MultiprocessEngine


@whitelist_for_serdes
class ReexecutionConfig(namedtuple('_ReexecutionConfig', 'previous_run_id step_output_handles')):
    @staticmethod
    def from_previous_run(previous_run_result):
        from .results import PipelineExecutionResult
        from .plan.objects import StepOutputHandle
        from dagster.core.events import DagsterEventType, ObjectStoreOperationType

        check.opt_inst_param(previous_run_result, 'previous_run_result', PipelineExecutionResult)

        step_output_handles = []
        for step_key, events in previous_run_result.events_by_step_key.items():
            if not events or not any([e.is_step_success for e in events]):
                continue

            step_output_handles += [
                StepOutputHandle(step_key, event.event_specific_data.value_name)
                for event in events
                if (
                    event.event_type_value == DagsterEventType.OBJECT_STORE_OPERATION.value
                    and event.event_specific_data.op == ObjectStoreOperationType.SET_OBJECT.value
                )
            ]
        return ReexecutionConfig(previous_run_result.run_id, step_output_handles)


def check_persistent_storage_requirement(system_storage_def):
    if not system_storage_def.is_persistent:
        raise DagsterUnmetExecutorRequirementsError(
            (
                'You have attempted use a multi process executor while using system '
                'storage {storage_name} which does not persist intermediates. '
                'This means there would be no way to move data between different '
                'processes. Please configure your pipeline in the storage config '
                'section to use persistent system storage such as the filesystem.'
            ).format(storage_name=system_storage_def.name)
        )


def check_non_ephemeral_instance(instance):
    if instance.is_ephemeral:
        raise DagsterUnmetExecutorRequirementsError(
            'You have attempted to use a multi process executor with an ephemeral DagsterInstance. '
            'A non-ephermal instance is needed to coordinate execution between multiple processes. '
            'You can configure your default instance via $DAGSTER_HOME or ensure a valid one is '
            'passed when invoking the python APIs.'
        )
