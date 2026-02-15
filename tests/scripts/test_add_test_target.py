"""Tests for add_test_target.py script."""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts.add_test_target import BUILD_CONFIG_LIST_UUID
from scripts.add_test_target import DEBUG_CONFIG_UUID
from scripts.add_test_target import DEPENDENCY_UUID
from scripts.add_test_target import FRAMEWORKS_PHASE_UUID
from scripts.add_test_target import PROXY_UUID
from scripts.add_test_target import RELEASE_CONFIG_UUID
from scripts.add_test_target import RESOURCES_PHASE_UUID
from scripts.add_test_target import SOURCES_PHASE_UUID
from scripts.add_test_target import TEST_GROUP_UUID
from scripts.add_test_target import TEST_PRODUCT_UUID
from scripts.add_test_target import TEST_TARGET_UUID
from scripts.add_test_target import ProjectError
from scripts.add_test_target import add_build_configurations
from scripts.add_test_target import add_configuration_list
from scripts.add_test_target import add_container_item_proxy
from scripts.add_test_target import add_file_reference
from scripts.add_test_target import add_frameworks_phase
from scripts.add_test_target import add_native_target
from scripts.add_test_target import add_resources_phase
from scripts.add_test_target import add_sources_phase
from scripts.add_test_target import add_target_dependency
from scripts.add_test_target import add_test_group
from scripts.add_test_target import add_test_target
from scripts.add_test_target import find_main_target_uuid
from scripts.add_test_target import find_section_bounds
from scripts.add_test_target import generate_uuid
from scripts.add_test_target import main
from scripts.add_test_target import normalize_scheme
from scripts.add_test_target import normalize_to_xcode_format
from scripts.add_test_target import read_pbxproj
from scripts.add_test_target import update_products_group
from scripts.add_test_target import update_project_targets
from scripts.add_test_target import update_scheme
from scripts.add_test_target import write_pbxproj


class TestGenerateUuid:
    """Tests for generate_uuid function."""

    def test_deterministic_output(self) -> None:
        """Same seed produces same UUID."""
        uuid1 = generate_uuid("test")
        uuid2 = generate_uuid("test")
        assert uuid1 == uuid2

    def test_different_seeds_produce_different_uuids(self) -> None:
        """Different seeds produce different UUIDs."""
        uuid1 = generate_uuid("seed1")
        uuid2 = generate_uuid("seed2")
        assert uuid1 != uuid2

    def test_uuid_format(self) -> None:
        """UUID is 24 uppercase hex characters."""
        uuid = generate_uuid("test")
        assert len(uuid) == 24
        assert uuid.isupper()
        assert all(c in "0123456789ABCDEF" for c in uuid)


class TestReadPbxproj:
    """Tests for read_pbxproj function."""

    def test_reads_existing_file(self, tmp_path: Path) -> None:
        """Reads content from existing project.pbxproj."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("test content")

        result = read_pbxproj(project_path)

        assert result == "test content"

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        """Raises ProjectError if file doesn't exist."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()

        with pytest.raises(ProjectError, match="Project file not found"):
            read_pbxproj(project_path)


class TestWritePbxproj:
    """Tests for write_pbxproj function."""

    def test_writes_content(self, tmp_path: Path) -> None:
        """Writes content to project.pbxproj."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("original")

        write_pbxproj(project_path, "new content")

        assert pbxproj.read_text() == "new content"


class TestFindMainTargetUuid:
    """Tests for find_main_target_uuid function."""

    def test_finds_main_target(self) -> None:
        """Finds main app target UUID."""
        content = """
		ABCD1234567890ABCDEF1234 /* Snapper */ = {
			isa = PBXNativeTarget;
		};
"""
        result = find_main_target_uuid(content)

        assert result == "ABCD1234567890ABCDEF1234"

    def test_raises_when_not_found(self) -> None:
        """Raises ProjectError if target not found."""
        content = "no target here"

        with pytest.raises(ProjectError, match="Could not find main app target"):
            find_main_target_uuid(content)


class TestFindSectionBounds:
    """Tests for find_section_bounds function."""

    def test_finds_section(self) -> None:
        """Finds section start and end positions."""
        content = """
/* Begin TestSection section */
content here
/* End TestSection section */
"""
        start, end = find_section_bounds(content, "TestSection")

        assert start > 0
        assert end > start
        assert "content here" in content[start:end]

    def test_raises_when_section_not_found(self) -> None:
        """Raises ProjectError if section not found."""
        content = "no section here"

        with pytest.raises(ProjectError, match="Could not find MissingSection section"):
            find_section_bounds(content, "MissingSection")

    def test_raises_when_end_marker_missing(self) -> None:
        """Raises ProjectError if end marker missing."""
        content = "/* Begin TestSection section */ content"

        with pytest.raises(ProjectError, match="Could not find TestSection section"):
            find_section_bounds(content, "TestSection")


class TestAddFileReference:
    """Tests for add_file_reference function."""

    def test_adds_file_reference(self) -> None:
        """Adds test product file reference."""
        content = """/* Begin PBXFileReference section */
		AAAA0000000000000000AAAA /* App.swift */ = {isa = PBXFileReference;};
		DDDD0000000000000000DDDD /* Other.swift */ = {isa = PBXFileReference;};
/* End PBXFileReference section */"""

        result = add_file_reference(content)

        assert TEST_PRODUCT_UUID in result
        assert "SnapperTests.xctest" in result

    def test_inserts_in_sorted_order(self) -> None:
        """Inserts file reference in sorted UUID order."""
        content = """/* Begin PBXFileReference section */
		AAAA0000000000000000AAAA /* First */ = {isa = PBXFileReference;};
		FFFF0000000000000000FFFF /* Last */ = {isa = PBXFileReference;};
/* End PBXFileReference section */"""

        result = add_file_reference(content)

        aaaa_pos = result.find("AAAA")
        test_pos = result.find(TEST_PRODUCT_UUID)
        ffff_pos = result.find("FFFF")
        assert aaaa_pos < test_pos < ffff_pos


class TestAddTestGroup:
    """Tests for add_test_group function."""

    def test_adds_to_existing_section(self) -> None:
        """Adds test group to existing section."""
        content = """/* Begin PBXFileSystemSynchronizedRootGroup section */
		EXISTING0000000000000000 /* Existing */ = {isa = PBXFileSystemSynchronizedRootGroup;};
/* End PBXFileSystemSynchronizedRootGroup section */"""

        result = add_test_group(content)

        assert TEST_GROUP_UUID in result
        assert "SnapperTests" in result

    def test_creates_new_section(self) -> None:
        """Creates new section if not exists."""
        content = """/* Begin PBXFrameworksBuildPhase section */
		EXISTING /* Frameworks */ = {isa = PBXFrameworksBuildPhase;};
/* End PBXFrameworksBuildPhase section */"""

        result = add_test_group(content)

        assert "/* Begin PBXFileSystemSynchronizedRootGroup section */" in result
        assert "/* End PBXFileSystemSynchronizedRootGroup section */" in result
        assert TEST_GROUP_UUID in result

    def test_creates_section_before_group_section(self) -> None:
        """Creates new section before PBXGroup section if no frameworks."""
        content = """/* Begin PBXGroup section */
		EXISTING /* Group */ = {isa = PBXGroup;};
/* End PBXGroup section */"""

        result = add_test_group(content)

        sync_section_pos = result.find("/* Begin PBXFileSystemSynchronizedRootGroup section */")
        group_section_pos = result.find("/* Begin PBXGroup section */")
        assert sync_section_pos < group_section_pos


class TestUpdateProductsGroup:
    """Tests for update_products_group function."""

    def test_adds_test_entries(self) -> None:
        """Adds test folder and product to Products group."""
        content = """		39B66CEE8EC32FBF47CADDFD /* Products */ = {
			isa = PBXGroup;
			children = (
				40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */,
			);
		};"""

        result = update_products_group(content)

        assert TEST_GROUP_UUID in result
        assert TEST_PRODUCT_UUID in result

    def test_returns_unchanged_if_already_present(self) -> None:
        """Returns unchanged if entries already exist."""
        content = f"""		39B66CEE8EC32FBF47CADDFD /* Products */ = {{
			isa = PBXGroup;
			children = (
				{TEST_GROUP_UUID} /* SnapperTests */,
				{TEST_PRODUCT_UUID} /* SnapperTests.xctest */,
			);
		}};"""

        result = update_products_group(content)

        assert result == content

    def test_returns_unchanged_if_no_products_group(self) -> None:
        """Returns unchanged if Products group not found."""
        content = "no products group here"

        result = update_products_group(content)

        assert result == content


class TestAddFrameworksPhase:
    """Tests for add_frameworks_phase function."""

    def test_adds_to_existing_section(self) -> None:
        """Adds frameworks phase to existing section."""
        content = """/* Begin PBXFrameworksBuildPhase section */
		EXISTING /* Frameworks */ = {isa = PBXFrameworksBuildPhase;};
/* End PBXFrameworksBuildPhase section */"""

        result = add_frameworks_phase(content)

        assert FRAMEWORKS_PHASE_UUID in result

    def test_creates_new_section(self) -> None:
        """Creates new section if not exists."""
        content = """/* Begin PBXGroup section */
		EXISTING /* Group */ = {isa = PBXGroup;};
/* End PBXGroup section */"""

        result = add_frameworks_phase(content)

        assert "/* Begin PBXFrameworksBuildPhase section */" in result
        assert FRAMEWORKS_PHASE_UUID in result


class TestAddNativeTarget:
    """Tests for add_native_target function."""

    def test_adds_native_target(self) -> None:
        """Adds test native target."""
        content = """/* Begin PBXNativeTarget section */
		EXISTING /* Snapper */ = {isa = PBXNativeTarget;};
/* End PBXNativeTarget section */"""

        result = add_native_target(content)

        assert TEST_TARGET_UUID in result
        assert "SnapperTests" in result
        assert "com.apple.product-type.bundle.unit-test" in result


class TestUpdateProjectTargets:
    """Tests for update_project_targets function."""

    def test_adds_test_target(self) -> None:
        """Adds test target to project targets list."""
        content = """			targets = (
				EXISTING /* Snapper */,
			);"""

        result = update_project_targets(content)

        assert TEST_TARGET_UUID in result

    def test_returns_unchanged_if_no_targets(self) -> None:
        """Returns unchanged if targets not found."""
        content = "no targets here"

        result = update_project_targets(content)

        assert result == content


class TestAddContainerItemProxy:
    """Tests for add_container_item_proxy function."""

    def test_adds_proxy_section(self) -> None:
        """Adds container item proxy section."""
        content = """		ROOTUUID12345678901234 = {
			isa = PBXProject;
		}; /* Project object */
/* Begin PBXFileReference section */
		FILE /* File */ = {isa = PBXFileReference;};
/* End PBXFileReference section */"""

        result = add_container_item_proxy(content, "MAIN_TARGET_UUID")

        assert "/* Begin PBXContainerItemProxy section */" in result
        assert PROXY_UUID in result
        assert "MAIN_TARGET_UUID" in result

    def test_returns_unchanged_if_exists(self) -> None:
        """Returns unchanged if section already exists."""
        content = """/* Begin PBXContainerItemProxy section */
		EXISTING /* PBXContainerItemProxy */ = {isa = PBXContainerItemProxy;};
/* End PBXContainerItemProxy section */"""

        result = add_container_item_proxy(content, "MAIN_TARGET_UUID")

        assert result == content


class TestAddResourcesPhase:
    """Tests for add_resources_phase function."""

    def test_adds_resources_phase(self) -> None:
        """Adds resources build phase."""
        content = """/* Begin PBXResourcesBuildPhase section */
		EXISTING /* Resources */ = {isa = PBXResourcesBuildPhase;};
/* End PBXResourcesBuildPhase section */"""

        result = add_resources_phase(content)

        assert RESOURCES_PHASE_UUID in result


class TestAddSourcesPhase:
    """Tests for add_sources_phase function."""

    def test_adds_sources_phase_sorted(self) -> None:
        """Adds sources phase in sorted order."""
        content = """/* Begin PBXSourcesBuildPhase section */
		DDDDDDDDDDDDDDDDDDDDDDD /* Sources */ = {isa = PBXSourcesBuildPhase;};
/* End PBXSourcesBuildPhase section */"""

        result = add_sources_phase(content)

        sources_pos = result.find(SOURCES_PHASE_UUID)
        ddd_pos = result.find("DDDDDDDDDDDDDDDDDDDDDDD")
        assert sources_pos < ddd_pos

    def test_adds_sources_phase_at_end(self) -> None:
        """Adds sources phase at end if UUID is largest."""
        content = """/* Begin PBXSourcesBuildPhase section */
		AAAAAAAAAAAAAAAAAAAAAAA /* Sources */ = {isa = PBXSourcesBuildPhase;};
/* End PBXSourcesBuildPhase section */"""

        result = add_sources_phase(content)

        assert SOURCES_PHASE_UUID in result


class TestAddTargetDependency:
    """Tests for add_target_dependency function."""

    def test_adds_dependency_section(self) -> None:
        """Adds target dependency section."""
        content = """/* Begin XCBuildConfiguration section */
		CONFIG /* Debug */ = {isa = XCBuildConfiguration;};
/* End XCBuildConfiguration section */"""

        result = add_target_dependency(content, "MAIN_TARGET_UUID")

        assert "/* Begin PBXTargetDependency section */" in result
        assert DEPENDENCY_UUID in result
        assert "MAIN_TARGET_UUID" in result

    def test_returns_unchanged_if_exists(self) -> None:
        """Returns unchanged if section already exists."""
        content = """/* Begin PBXTargetDependency section */
		EXISTING /* PBXTargetDependency */ = {isa = PBXTargetDependency;};
/* End PBXTargetDependency section */"""

        result = add_target_dependency(content, "MAIN_TARGET_UUID")

        assert result == content


class TestAddBuildConfigurations:
    """Tests for add_build_configurations function."""

    def test_adds_debug_and_release_configs(self) -> None:
        """Adds debug and release build configurations."""
        content = """/* Begin XCBuildConfiguration section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Debug */ = {isa = XCBuildConfiguration;};
		DAD0000000000000000000AA /* Release */ = {isa = XCBuildConfiguration;};
/* End XCBuildConfiguration section */"""

        result = add_build_configurations(content)

        assert DEBUG_CONFIG_UUID in result
        assert RELEASE_CONFIG_UUID in result
        assert "ie.klatt.snapper.tests" in result
        assert "IPHONEOS_DEPLOYMENT_TARGET = 26.0" in result

    def test_adds_release_at_end_if_no_dad(self) -> None:
        """Adds release config at section end if no DAD UUID found."""
        content = """/* Begin XCBuildConfiguration section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Debug */ = {isa = XCBuildConfiguration;};
		BBBBBBBBBBBBBBBBBBBBBBBB /* Release */ = {isa = XCBuildConfiguration;};
/* End XCBuildConfiguration section */"""

        result = add_build_configurations(content)

        assert RELEASE_CONFIG_UUID in result


class TestAddConfigurationList:
    """Tests for add_configuration_list function."""

    def test_adds_config_list_sorted(self) -> None:
        """Adds configuration list in sorted order."""
        content = """/* Begin XCConfigurationList section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Build configuration list */ = {isa = XCConfigurationList;};
		C2CB0000000000000000AAAA /* Build configuration list */ = {isa = XCConfigurationList;};
/* End XCConfigurationList section */"""

        result = add_configuration_list(content)

        bb_pos = result.find(BUILD_CONFIG_LIST_UUID)
        c2cb_pos = result.find("C2CB")
        assert bb_pos < c2cb_pos

    def test_adds_config_list_at_end(self) -> None:
        """Adds configuration list at end if no C2CB UUID."""
        content = """/* Begin XCConfigurationList section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Build configuration list */ = {isa = XCConfigurationList;};
/* End XCConfigurationList section */"""

        result = add_configuration_list(content)

        assert BUILD_CONFIG_LIST_UUID in result


class TestNormalizeToXcodeFormat:
    """Tests for normalize_to_xcode_format function."""

    def test_changes_object_version(self) -> None:
        """Changes objectVersion from 77 to 70."""
        content = "objectVersion = 77;"

        result = normalize_to_xcode_format(content)

        assert "objectVersion = 70;" in result

    def test_removes_preferred_project_version(self) -> None:
        """Removes preferredProjectObjectVersion line."""
        content = "some content\n\t\t\t\tpreferredProjectObjectVersion = 77;\nmore content"

        result = normalize_to_xcode_format(content)

        assert "preferredProjectObjectVersion" not in result

    def test_changes_file_type(self) -> None:
        """Changes lastKnownFileType to explicitFileType."""
        content = (
            "40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */ = "
            "{isa = PBXFileReference; lastKnownFileType = wrapper.application; };"
        )

        result = normalize_to_xcode_format(content)

        assert "explicitFileType" in result

    def test_reorders_include_in_index(self) -> None:
        """Reorders includeInIndex after explicitFileType."""
        content = (
            "40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */ = "
            "{isa = PBXFileReference; includeInIndex = 0; explicitFileType = wrapper.application; };"
        )

        result = normalize_to_xcode_format(content)

        explicit_pos = result.find("explicitFileType")
        include_pos = result.find("includeInIndex")
        assert explicit_pos < include_pos

    def test_removes_empty_development_team(self) -> None:
        """Removes DevelopmentTeam attribute lines regardless of value."""
        content = 'some content\n\t\t\t\tDevelopmentTeam = "";\nmore content'

        result = normalize_to_xcode_format(content)

        assert "DevelopmentTeam" not in result

    def test_removes_nonempty_development_team_attribute(self) -> None:
        """Removes DevelopmentTeam attribute lines with non-empty value."""
        content = "some content\n\t\t\t\tDevelopmentTeam = 26MP7QQP95;\nmore content"

        result = normalize_to_xcode_format(content)

        assert "DevelopmentTeam" not in result

    def test_sets_development_team(self) -> None:
        """Sets DEVELOPMENT_TEAM to specific value."""
        content = 'DEVELOPMENT_TEAM = "";'

        result = normalize_to_xcode_format(content)

        assert "DEVELOPMENT_TEAM = 26MP7QQP95;" in result

    def test_removes_extra_newline(self) -> None:
        """Removes extra newline after minimizedProjectReferenceProxies."""
        content = 'minimizedProjectReferenceProxies = 1;\n\n\t\t\tprojectDirPath = "";'

        result = normalize_to_xcode_format(content)

        assert "minimizedProjectReferenceProxies = 1;\n\t\t\tprojectDirPath" in result

    def test_strips_infoplist_key_cfbundledisplayname(self) -> None:
        """Strips INFOPLIST_KEY_CFBundleDisplayName line."""
        content = "before\n\t\t\t\tINFOPLIST_KEY_CFBundleDisplayName = Snapper;\nafter"

        result = normalize_to_xcode_format(content)

        assert "INFOPLIST_KEY_CFBundleDisplayName" not in result
        assert "before\nafter" in result

    def test_strips_supported_platforms(self) -> None:
        """Strips SUPPORTED_PLATFORMS line."""
        content = 'before\n\t\t\t\tSUPPORTED_PLATFORMS = "iphoneos iphonesimulator";\nafter'

        result = normalize_to_xcode_format(content)

        assert "SUPPORTED_PLATFORMS" not in result
        assert "before\nafter" in result

    def test_strips_supports_maccatalyst(self) -> None:
        """Strips SUPPORTS_MACCATALYST line."""
        content = "before\n\t\t\t\tSUPPORTS_MACCATALYST = NO;\nafter"

        result = normalize_to_xcode_format(content)

        assert "SUPPORTS_MACCATALYST" not in result
        assert "before\nafter" in result

    def test_strips_supports_mac_designed_for_iphone(self) -> None:
        """Strips SUPPORTS_MAC_DESIGNED_FOR_IPHONE_IPAD line."""
        content = "before\n\t\t\t\tSUPPORTS_MAC_DESIGNED_FOR_IPHONE_IPAD = NO;\nafter"

        result = normalize_to_xcode_format(content)

        assert "SUPPORTS_MAC_DESIGNED_FOR_IPHONE_IPAD" not in result
        assert "before\nafter" in result

    def test_strips_supports_xr_designed_for_iphone(self) -> None:
        """Strips SUPPORTS_XR_DESIGNED_FOR_IPHONE_IPAD line."""
        content = "before\n\t\t\t\tSUPPORTS_XR_DESIGNED_FOR_IPHONE_IPAD = NO;\nafter"

        result = normalize_to_xcode_format(content)

        assert "SUPPORTS_XR_DESIGNED_FOR_IPHONE_IPAD" not in result
        assert "before\nafter" in result


class TestAddTestTarget:
    """Tests for add_test_target function."""

    def test_returns_early_if_test_target_exists(self, tmp_path: Path, capsys: Any) -> None:
        """Returns early if SnapperTests already in project."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("SnapperTests already here")

        result = add_test_target(project_path)

        assert result == TEST_TARGET_UUID
        captured = capsys.readouterr()
        assert "Test target already exists" in captured.out

    def test_adds_test_target(self, tmp_path: Path, capsys: Any) -> None:
        """Adds test target to project."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("""
		ABCD1234567890ABCDEF1234 /* Snapper */ = {
			isa = PBXNativeTarget;
		}; /* Project object */
/* Begin PBXFileReference section */
		AAAA0000000000000000AAAA /* App.swift */ = {isa = PBXFileReference;};
/* End PBXFileReference section */
/* Begin PBXFrameworksBuildPhase section */
		EXISTING /* Frameworks */ = {isa = PBXFrameworksBuildPhase;};
/* End PBXFrameworksBuildPhase section */
/* Begin PBXGroup section */
		39B66CEE8EC32FBF47CADDFD /* Products */ = {
			isa = PBXGroup;
			children = (
				40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */,
			);
		};
/* End PBXGroup section */
/* Begin PBXNativeTarget section */
		EXISTING /* Snapper */ = {isa = PBXNativeTarget;};
/* End PBXNativeTarget section */
/* Begin PBXResourcesBuildPhase section */
		EXISTING /* Resources */ = {isa = PBXResourcesBuildPhase;};
/* End PBXResourcesBuildPhase section */
/* Begin PBXSourcesBuildPhase section */
		DDDDDDDDDDDDDDDDDDDDDDD /* Sources */ = {isa = PBXSourcesBuildPhase;};
/* End PBXSourcesBuildPhase section */
/* Begin XCBuildConfiguration section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Debug */ = {isa = XCBuildConfiguration;};
		DAD0000000000000000000AA /* Release */ = {isa = XCBuildConfiguration;};
/* End XCBuildConfiguration section */
/* Begin XCConfigurationList section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Build configuration list */ = {isa = XCConfigurationList;};
/* End XCConfigurationList section */
			targets = (
				EXISTING /* Snapper */,
			);
""")

        result = add_test_target(project_path)

        assert result == TEST_TARGET_UUID
        content = pbxproj.read_text()
        assert TEST_TARGET_UUID in content
        captured = capsys.readouterr()
        assert "Added SnapperTests target to project" in captured.out


class TestNormalizeScheme:
    """Tests for normalize_scheme function."""

    def test_downgrades_scheme_version(self) -> None:
        """Changes scheme version from 1.7 to 1.3."""
        content = '<Scheme version = "1.7">'

        result = normalize_scheme(content)

        assert 'version = "1.3"' in result

    def test_removes_run_post_actions_on_failure(self) -> None:
        """Removes runPostActionsOnFailure attribute from BuildAction."""
        content = (
            "   <BuildAction\n"
            '      parallelizeBuildables = "YES"\n'
            '      buildImplicitDependencies = "YES"\n'
            '      runPostActionsOnFailure = "NO">'
        )

        result = normalize_scheme(content)

        assert "runPostActionsOnFailure" not in result
        assert 'buildImplicitDependencies = "YES">' in result

    def test_removes_only_generate_coverage(self) -> None:
        """Removes onlyGenerateCoverageForSpecifiedTargets attribute."""
        content = (
            '      shouldUseLaunchSchemeArgsEnv = "YES"\n'
            '      onlyGenerateCoverageForSpecifiedTargets = "NO">'
        )

        result = normalize_scheme(content)

        assert "onlyGenerateCoverageForSpecifiedTargets" not in result

    def test_removes_empty_command_line_arguments(self) -> None:
        """Removes empty CommandLineArguments elements."""
        content = (
            "   </TestAction>\n"
            "      <CommandLineArguments>\n"
            "      </CommandLineArguments>\n"
            "   <LaunchAction>"
        )

        result = normalize_scheme(content)

        assert "CommandLineArguments" not in result

    def test_preserves_unrelated_content(self) -> None:
        """Leaves unrelated scheme content unchanged."""
        content = '<Scheme version = "1.3"><BuildAction>test</BuildAction></Scheme>'

        result = normalize_scheme(content)

        assert result == content


class TestUpdateScheme:
    """Tests for update_scheme function."""

    def test_skips_if_scheme_not_found(self, tmp_path: Path, capsys: Any) -> None:
        """Skips if scheme file doesn't exist."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()

        update_scheme(project_path, TEST_TARGET_UUID)

        captured = capsys.readouterr()
        assert "Scheme file not found, skipping" in captured.out

    def test_skips_if_no_test_action(self, tmp_path: Path, capsys: Any) -> None:
        """Skips if TestAction not found in scheme."""
        project_path = tmp_path / "Test.xcodeproj"
        scheme_dir = project_path / "xcshareddata" / "xcschemes"
        scheme_dir.mkdir(parents=True)
        scheme_path = scheme_dir / "Snapper.xcscheme"
        scheme_path.write_text("<Scheme></Scheme>")

        update_scheme(project_path, TEST_TARGET_UUID)

        captured = capsys.readouterr()
        assert "TestAction not found in scheme" in captured.out

    def test_normalizes_when_test_target_exists(self, tmp_path: Path, capsys: Any) -> None:
        """Normalizes scheme even when test target already present."""
        project_path = tmp_path / "Test.xcodeproj"
        scheme_dir = project_path / "xcshareddata" / "xcschemes"
        scheme_dir.mkdir(parents=True)
        scheme_path = scheme_dir / "Snapper.xcscheme"
        scheme_path.write_text(
            '<Scheme version = "1.7"><TestAction>'
            "<Testables><TestableReference>"
            '<BuildableReference BlueprintName="SnapperTests"/>'
            "</TestableReference></Testables></TestAction></Scheme>"
        )

        update_scheme(project_path, TEST_TARGET_UUID)

        content = scheme_path.read_text()
        assert 'version = "1.3"' in content
        captured = capsys.readouterr()
        assert "Test target already in scheme" in captured.out

    def test_adds_test_target_to_empty_testables(self, tmp_path: Path, capsys: Any) -> None:
        """Adds test target reference into empty Testables element."""
        project_path = tmp_path / "Test.xcodeproj"
        scheme_dir = project_path / "xcshareddata" / "xcschemes"
        scheme_dir.mkdir(parents=True)
        scheme_path = scheme_dir / "Snapper.xcscheme"
        scheme_path.write_text(
            '<Scheme version = "1.7">\n'
            "   <TestAction>\n"
            "      <Testables>\n"
            "      </Testables>\n"
            "   </TestAction>\n"
            "</Scheme>"
        )

        update_scheme(project_path, TEST_TARGET_UUID)

        content = scheme_path.read_text()
        assert TEST_TARGET_UUID in content
        assert "SnapperTests" in content
        assert 'version = "1.3"' in content
        captured = capsys.readouterr()
        assert "Added test target to scheme" in captured.out

    def test_produces_xcode_compatible_testable_format(self, tmp_path: Path) -> None:
        """Generated TestableReference uses Xcode multi-line attribute format."""
        project_path = tmp_path / "Test.xcodeproj"
        scheme_dir = project_path / "xcshareddata" / "xcschemes"
        scheme_dir.mkdir(parents=True)
        scheme_path = scheme_dir / "Snapper.xcscheme"
        scheme_path.write_text(
            "<Scheme>\n"
            "   <TestAction>\n"
            "      <Testables>\n"
            "      </Testables>\n"
            "   </TestAction>\n"
            "</Scheme>"
        )

        update_scheme(project_path, TEST_TARGET_UUID)

        content = scheme_path.read_text()
        assert "         <TestableReference\n" in content
        assert '            skipped = "NO">' in content
        assert "            <BuildableReference\n" in content


class TestMain:
    """Tests for main function."""

    def test_returns_1_if_project_not_found(
        self, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns 1 if project doesn't exist."""
        project_path = tmp_path / "NonExistent.xcodeproj"
        monkeypatch.setattr(sys, "argv", ["prog", str(project_path)])

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Project not found" in captured.out

    def test_returns_1_on_project_error(
        self, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns 1 on ProjectError."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("invalid content without main target")
        monkeypatch.setattr(sys, "argv", ["prog", str(project_path)])

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.out

    def test_returns_0_on_success(
        self, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns 0 on successful completion."""
        project_path = tmp_path / "Test.xcodeproj"
        project_path.mkdir()
        pbxproj = project_path / "project.pbxproj"
        pbxproj.write_text("""
		ABCD1234567890ABCDEF1234 /* Snapper */ = {
			isa = PBXNativeTarget;
		}; /* Project object */
/* Begin PBXFileReference section */
		AAAA0000000000000000AAAA /* App.swift */ = {isa = PBXFileReference;};
/* End PBXFileReference section */
/* Begin PBXFrameworksBuildPhase section */
		EXISTING /* Frameworks */ = {isa = PBXFrameworksBuildPhase;};
/* End PBXFrameworksBuildPhase section */
/* Begin PBXGroup section */
		39B66CEE8EC32FBF47CADDFD /* Products */ = {
			isa = PBXGroup;
			children = (
				40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */,
			);
		};
/* End PBXGroup section */
/* Begin PBXNativeTarget section */
		EXISTING /* Snapper */ = {isa = PBXNativeTarget;};
/* End PBXNativeTarget section */
/* Begin PBXResourcesBuildPhase section */
		EXISTING /* Resources */ = {isa = PBXResourcesBuildPhase;};
/* End PBXResourcesBuildPhase section */
/* Begin PBXSourcesBuildPhase section */
		DDDDDDDDDDDDDDDDDDDDDDD /* Sources */ = {isa = PBXSourcesBuildPhase;};
/* End PBXSourcesBuildPhase section */
/* Begin XCBuildConfiguration section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Debug */ = {isa = XCBuildConfiguration;};
		DAD0000000000000000000AA /* Release */ = {isa = XCBuildConfiguration;};
/* End XCBuildConfiguration section */
/* Begin XCConfigurationList section */
		AAAAAAAAAAAAAAAAAAAAAAAA /* Build configuration list */ = {isa = XCConfigurationList;};
/* End XCConfigurationList section */
			targets = (
				EXISTING /* Snapper */,
			);
""")
        monkeypatch.setattr(sys, "argv", ["prog", str(project_path)])

        result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Test target added successfully!" in captured.out

    def test_uses_default_path_when_no_args(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Uses default project path when no CLI args provided."""
        monkeypatch.setattr(sys, "argv", ["prog"])
        with (
            patch("scripts.add_test_target.add_test_target") as mock_add,
            patch("scripts.add_test_target.update_scheme"),
            patch.object(Path, "exists", return_value=True),
        ):
            mock_add.return_value = TEST_TARGET_UUID

            result = main()

            call_args = mock_add.call_args
            assert "Snapper.xcodeproj" in str(call_args)
            assert result == 0

    def test_cli_with_argument(
        self, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test CLI with project path argument."""
        project_path = tmp_path / "Test.xcodeproj"
        monkeypatch.setattr(sys, "argv", ["prog", str(project_path)])

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Project not found" in captured.out


class TestUpdateProjectTargetsWithNoComma:
    """Test edge case where targets list has no comma."""

    def test_returns_unchanged_if_no_comma_in_targets(self) -> None:
        """Returns unchanged if no comma found in targets."""
        content = """targets = (
            SINGLE_TARGET
        );"""

        result = update_project_targets(content)

        assert result == content


class TestUpdateProductsGroupPartialEntries:
    """Tests for update_products_group with partial entries."""

    def test_adds_only_product_if_folder_exists(self) -> None:
        """Adds only test product if folder already exists."""
        content = f"""		39B66CEE8EC32FBF47CADDFD /* Products */ = {{
			isa = PBXGroup;
			children = (
				{TEST_GROUP_UUID} /* SnapperTests */,
				40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */,
			);
		}};"""

        result = update_products_group(content)

        assert TEST_PRODUCT_UUID in result
        assert result.count(TEST_GROUP_UUID) == 1

    def test_adds_only_folder_if_product_exists(self) -> None:
        """Adds only test folder if product already exists."""
        content = f"""		39B66CEE8EC32FBF47CADDFD /* Products */ = {{
			isa = PBXGroup;
			children = (
				{TEST_PRODUCT_UUID} /* SnapperTests.xctest */,
				40BE9EE10BEF49D1DCA0F3C5 /* Snapper.app */,
			);
		}};"""

        result = update_products_group(content)

        assert TEST_GROUP_UUID in result
        assert result.count(TEST_PRODUCT_UUID) == 1


class TestNormalizeSchemeAppliedDuringUpdate:
    """Tests for normalize_scheme integration in update_scheme."""

    def test_removes_command_line_arguments_on_insert(self, tmp_path: Path) -> None:
        """Normalization strips CommandLineArguments when adding test target."""
        project_path = tmp_path / "Test.xcodeproj"
        scheme_dir = project_path / "xcshareddata" / "xcschemes"
        scheme_dir.mkdir(parents=True)
        scheme_path = scheme_dir / "Snapper.xcscheme"
        scheme_path.write_text(
            '<Scheme version = "1.7">\n'
            "   <TestAction>\n"
            "      <Testables>\n"
            "      </Testables>\n"
            "      <CommandLineArguments>\n"
            "      </CommandLineArguments>\n"
            "   </TestAction>\n"
            "</Scheme>"
        )

        update_scheme(project_path, TEST_TARGET_UUID)

        content = scheme_path.read_text()
        assert "CommandLineArguments" not in content
        assert TEST_TARGET_UUID in content
