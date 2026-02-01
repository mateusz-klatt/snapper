"""Tests for process registration and discovery system."""

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import discover_processes
from snapper.application.process_manager.registry import get_process_metadata
from snapper.application.process_manager.registry import get_registered_processes
from snapper.application.process_manager.registry import register_process


class TestRegisterProcess:
    """Test cases for process registration decorator."""

    def test_register_basic_process(self) -> None:
        """Verify basic process registration with minimal parameters.

        Given: A class decorated with @register_process,
        When: Metadata is retrieved for the registered process,
        Then: Default values are applied and class is properly registered.
        """

        @register_process(name="test_basic", description="Basic test process")
        class BasicProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for BasicProcess test stub."""
                pass

        metadata = get_process_metadata("test_basic")
        assert metadata is not None
        assert metadata["class_ref"] == BasicProcess
        assert metadata["method"] == "start"
        assert metadata["description"] == "Basic test process"
        assert metadata["priority"] == 50
        assert metadata["lifecycle"] == ProcessLifecycleEnum.LONG_RUNNING
        assert metadata["role"] == ProcessRoleEnum.CORE
        assert metadata["enabled"] is False
        assert metadata["mode"] == "thread"
        assert metadata["args"] == []

    def test_register_with_custom_parameters(self) -> None:
        """Verify process registration with all custom parameters.

        Given: A class decorated with full parameter set,
        When: Metadata is retrieved,
        Then: All custom values are preserved in metadata.
        """

        @register_process(
            name="test_custom",
            method="run",
            description="Custom test process",
            priority=10,
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.TASK,
            tags=["tag1", "tag2"],
            parameters_schema={"param1": "value1"},
            enabled=True,
            mode="process",
            args=["arg1", "arg2"],
        )
        class CustomProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for CustomProcess test stub."""
                pass

            async def run(self) -> None:
                """No-op run for CustomProcess test stub."""
                pass

        metadata = get_process_metadata("test_custom")
        assert metadata is not None
        assert metadata["class_ref"] == CustomProcess
        assert metadata["method"] == "run"
        assert metadata["description"] == "Custom test process"
        assert metadata["priority"] == 10
        assert metadata["lifecycle"] == ProcessLifecycleEnum.ONE_SHOT
        assert metadata["role"] == ProcessRoleEnum.TASK
        assert metadata["tags"] == ("tag1", "tag2")
        assert metadata["parameters_schema"] == {"param1": "value1"}
        assert metadata["enabled"] is True
        assert metadata["mode"] == "process"
        assert metadata["args"] == ["arg1", "arg2"]

    def test_register_with_string_enums(self) -> None:
        """Verify registration accepts string enum values.

        Given: Lifecycle and role as string values,
        When: Process is registered,
        Then: Strings are converted to proper enum instances.
        """

        @register_process(
            name="test_string_enums",
            lifecycle="one_shot",
            role="task",
        )
        class StringEnumProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for StringEnumProcess test stub."""
                pass

        metadata = get_process_metadata("test_string_enums")
        assert metadata is not None
        assert metadata["lifecycle"] == ProcessLifecycleEnum.ONE_SHOT
        assert metadata["role"] == ProcessRoleEnum.TASK

    def test_register_with_none_tags(self) -> None:
        """Verify registration handles None tags gracefully.

        Given: Registration with tags=None,
        When: Metadata is retrieved,
        Then: Tags defaults to empty tuple.
        """

        @register_process(name="test_no_tags", tags=None)
        class NoTagsProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for NoTagsProcess test stub."""
                pass

        metadata = get_process_metadata("test_no_tags")
        assert metadata is not None
        assert metadata["tags"] == ()

    def test_register_with_none_args(self) -> None:
        """Verify registration handles None args gracefully.

        Given: Registration with args=None,
        When: Metadata is retrieved,
        Then: Args defaults to empty list.
        """

        @register_process(name="test_no_args", args=None)
        class NoArgsProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for NoArgsProcess test stub."""
                pass

        metadata = get_process_metadata("test_no_args")
        assert metadata is not None
        assert metadata["args"] == []

    def test_class_path_generation(self) -> None:
        """Verify automatic class path generation from module and class.

        Given: A registered process class,
        When: Metadata is retrieved,
        Then: Class path contains module and class name.
        """

        @register_process(name="test_class_path")
        class ClassPathProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for ClassPathProcess test stub."""
                pass

        metadata = get_process_metadata("test_class_path")
        assert metadata is not None
        assert metadata["class_path"].endswith(
            "test_registry.TestRegisterProcess.test_class_path_generation.<locals>.ClassPathProcess"
        )


class TestGetRegisteredProcesses:
    """Test cases for retrieving registered processes."""

    def test_get_all_processes_returns_dict(self) -> None:
        """Verify get_registered_processes returns dictionary.

        Given: Process registry with registered processes,
        When: get_registered_processes is called,
        Then: Dictionary of process metadata is returned.
        """
        processes = get_registered_processes()
        assert isinstance(processes, dict)

    def test_get_all_processes_returns_copy(self) -> None:
        """Verify get_registered_processes returns a copy.

        Given: Process registry with registered processes,
        When: get_registered_processes is called twice,
        Then: Equal but distinct dictionaries are returned.
        """

        @register_process(name="test_copy")
        class CopyTestProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for CopyTestProcess test stub."""
                pass

        processes1 = get_registered_processes()
        processes2 = get_registered_processes()
        assert processes1 == processes2
        assert processes1 is not processes2

    def test_registered_processes_contains_metadata(self) -> None:
        """Verify registered process contains all expected metadata keys.

        Given: A registered process,
        When: Metadata is retrieved,
        Then: All expected keys are present.
        """

        @register_process(name="test_metadata_keys")
        class MetadataKeysProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for MetadataKeysProcess test stub."""
                pass

        processes = get_registered_processes()
        assert "test_metadata_keys" in processes
        metadata = processes["test_metadata_keys"]
        expected_keys = {
            "class_ref",
            "class_path",
            "method",
            "description",
            "priority",
            "lifecycle",
            "role",
            "tags",
            "parameters_schema",
            "enabled",
            "mode",
            "args",
        }
        assert set(metadata.keys()) == expected_keys


class TestGetProcessMetadata:
    """Test cases for retrieving process metadata."""

    def test_get_existing_process(self) -> None:
        """Verify get_process_metadata returns data for existing process.

        Given: A registered process,
        When: get_process_metadata is called with its name,
        Then: Correct metadata with class_ref is returned.
        """

        @register_process(name="test_existing")
        class ExistingProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for ExistingProcess test stub."""
                pass

        metadata = get_process_metadata("test_existing")
        assert metadata is not None
        assert metadata["class_ref"] == ExistingProcess

    def test_get_nonexistent_process(self) -> None:
        """Verify get_process_metadata returns None for unknown process.

        Given: No process registered with given name,
        When: get_process_metadata is called,
        Then: None is returned.
        """
        metadata = get_process_metadata("nonexistent_process_xyz")
        assert metadata is None


class TestDiscoverProcesses:
    """Test cases for automatic process discovery."""

    def test_discover_processes_runs_without_error(self) -> None:
        """Verify discover_processes executes without exceptions.

        Given: Application with registered process modules,
        When: discover_processes is called,
        Then: No exceptions are raised.
        """
        discover_processes()

    def test_discover_processes_finds_real_processes(self) -> None:
        """Verify discover_processes finds processes in application.

        Given: Initial set of registered processes,
        When: discover_processes is called,
        Then: Process count is equal or greater than before.
        """
        initial_processes = set(get_registered_processes().keys())
        discover_processes()
        final_processes = set(get_registered_processes().keys())
        assert len(final_processes) >= len(initial_processes)
