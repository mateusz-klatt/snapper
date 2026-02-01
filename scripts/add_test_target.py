"""Add SnapperTests unit test target to Xcode project.

This script modifies the project.pbxproj file to add a test target
and updates the scheme to include the test target.
"""

import hashlib
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


class ProjectError(Exception):
    """Raised when project file operations fail."""


def generate_uuid(seed: str) -> str:
    """Generate a deterministic UUID from a seed string.

    Args:
        seed: Input string used to generate the UUID.

    Returns:
        A 24-character uppercase hexadecimal UUID.
    """
    hash_obj = hashlib.sha256(seed.encode())
    return hash_obj.hexdigest()[:24].upper()


TEST_TARGET_UUID = "BA27489833F50CB3926D5BA4"
TEST_PRODUCT_UUID = "CB3EEEA02ED3924700201EC3"
TEST_GROUP_UUID = "9206EDF09F2CDB645A09870C"
SOURCES_PHASE_UUID = "C2A737232074BB32F44BEA6E"
FRAMEWORKS_PHASE_UUID = "B760070C4F61AD655EFAB579"
RESOURCES_PHASE_UUID = "B80C7DC65837D75E47976626"
DEPENDENCY_UUID = "54715C6ED473081B4DBF5AD7"
PROXY_UUID = "087BF7A615718E5178C80351"
BUILD_CONFIG_LIST_UUID = "BB90EA452F52163343C4E98A"
DEBUG_CONFIG_UUID = "002E01F0D8D9D3FC0FE25882"
RELEASE_CONFIG_UUID = "6E137B1328CC255A3F3262B6"


def read_pbxproj(project_path: Path) -> str:
    """Read project.pbxproj content.

    Args:
        project_path: Path to the .xcodeproj directory.

    Returns:
        The contents of the project.pbxproj file.
    """
    pbxproj = project_path / "project.pbxproj"
    if not pbxproj.exists():
        raise ProjectError(f"Project file not found: {pbxproj}")
    return pbxproj.read_text()


def write_pbxproj(project_path: Path, content: str) -> None:
    """Write content to project.pbxproj.

    Args:
        project_path: Path to the .xcodeproj directory.
        content: The content to write to the file.
    """
    pbxproj = project_path / "project.pbxproj"
    pbxproj.write_text(content)


def find_main_target_uuid(content: str) -> str:
    """Find the main app target UUID.

    Args:
        content: The project.pbxproj file content.

    Returns:
        The UUID of the main Snapper app target.
    """
    main_target_match = re.search(
        r"([A-F0-9]{24}) /\* Snapper \*/ = \{[^}]*isa = PBXNativeTarget", content
    )
    if not main_target_match:
        raise ProjectError("Could not find main app target")
    return main_target_match.group(1)


def find_section_bounds(content: str, section_name: str) -> tuple[int, int]:
    """Find the start and end positions of a section.

    Args:
        content: The project.pbxproj file content.
        section_name: Name of the section to find.

    Returns:
        A tuple of (start_position, end_position) for the section.
    """
    start_marker = f"/* Begin {section_name} section */"
    end_marker = f"/* End {section_name} section */"
    start = content.find(start_marker)
    end = content.find(end_marker)
    if start == -1 or end == -1:
        raise ProjectError(f"Could not find {section_name} section")
    return start, end


def add_file_reference(content: str) -> str:
    """Add test product file reference.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the test product file reference added.
    """
    start, end = find_section_bounds(content, "PBXFileReference")
    test_product_ref = (
        f"\t\t{TEST_PRODUCT_UUID} /* SnapperTests.xctest */ = "
        f"{{isa = PBXFileReference; explicitFileType = wrapper.cfbundle; "
        f"includeInIndex = 0; path = SnapperTests.xctest; sourceTree = BUILT_PRODUCTS_DIR; }};\n"
    )
    file_ref_content = content[start:end]
    matches = list(re.finditer(r"\n\t\t([A-F0-9]+) /\*", file_ref_content))
    insert_pos = end
    for match in matches:
        uuid = match.group(1)
        if uuid > TEST_PRODUCT_UUID:
            insert_pos = start + match.start() + 1
            break
    return content[:insert_pos] + test_product_ref + content[insert_pos:]


def add_test_group(content: str) -> str:
    """Add test group to PBXFileSystemSynchronizedRootGroup section.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the test group added.
    """
    test_group_entry = (
        f"\t\t{TEST_GROUP_UUID} /* SnapperTests */ = "
        f"{{isa = PBXFileSystemSynchronizedRootGroup; explicitFileTypes = {{}}; "
        f'explicitFolders = (); path = SnapperTests; sourceTree = "<group>"; }};\n'
    )
    if "/* Begin PBXFileSystemSynchronizedRootGroup section */" not in content:
        insert_point = content.find("/* Begin PBXFrameworksBuildPhase section */")
        if insert_point == -1:
            insert_point = content.find("/* Begin PBXGroup section */")
        test_group_section = (
            f"/* Begin PBXFileSystemSynchronizedRootGroup section */\n"
            f"{test_group_entry}"
            f"/* End PBXFileSystemSynchronizedRootGroup section */\n\n"
        )
        return content[:insert_point] + test_group_section + content[insert_point:]
    section_end = content.find("/* End PBXFileSystemSynchronizedRootGroup section */")
    return content[:section_end] + test_group_entry + content[section_end:]


def update_products_group(content: str) -> str:
    """Add test folder and product to Products group.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with test entries added to the Products group.
    """
    products_match = re.search(
        r"(39B66CEE8EC32FBF47CADDFD /\* Products \*/ = \{[^}]*children = \()([^)]*)\)",
        content,
        re.DOTALL,
    )
    if not products_match:
        return content
    children_content = products_match.group(2)
    has_test_folder = TEST_GROUP_UUID in children_content
    has_test_product = TEST_PRODUCT_UUID in children_content
    if has_test_folder and has_test_product:
        return content
    new_children = children_content.rstrip(",\n \t")
    if not has_test_folder:
        new_children += f",\n\t\t\t\t{TEST_GROUP_UUID} /* SnapperTests */"
    if not has_test_product:
        new_children += f",\n\t\t\t\t{TEST_PRODUCT_UUID} /* SnapperTests.xctest */"
    new_children += ",\n\t\t\t"
    return content[: products_match.start(2)] + new_children + content[products_match.end(2) :]


def add_frameworks_phase(content: str) -> str:
    """Add frameworks build phase.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the frameworks build phase added.
    """
    frameworks_phase = (
        f"\t\t{FRAMEWORKS_PHASE_UUID} /* Frameworks */ = {{\n"
        f"\t\t\tisa = PBXFrameworksBuildPhase;\n"
        f"\t\t\tbuildActionMask = 2147483647;\n"
        f"\t\t\tfiles = (\n"
        f"\t\t\t);\n"
        f"\t\t\trunOnlyForDeploymentPostprocessing = 0;\n"
        f"\t\t}};\n"
    )
    section_end = content.find("/* End PBXFrameworksBuildPhase section */")
    if section_end == -1:
        insert_point = content.find("/* Begin PBXGroup section */")
        frameworks_section = (
            f"/* Begin PBXFrameworksBuildPhase section */\n"
            f"{frameworks_phase}"
            f"/* End PBXFrameworksBuildPhase section */\n\n"
        )
        return content[:insert_point] + frameworks_section + content[insert_point:]
    return content[:section_end] + frameworks_phase + content[section_end:]


def add_native_target(content: str) -> str:
    """Add test native target.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the SnapperTests native target added.
    """
    native_target_end = content.find("/* End PBXNativeTarget section */")
    test_target = (
        f"\t\t{TEST_TARGET_UUID} /* SnapperTests */ = {{\n"
        f"\t\t\tisa = PBXNativeTarget;\n"
        f"\t\t\tbuildConfigurationList = {BUILD_CONFIG_LIST_UUID} "
        f'/* Build configuration list for PBXNativeTarget "SnapperTests" */;\n'
        f"\t\t\tbuildPhases = (\n"
        f"\t\t\t\t{SOURCES_PHASE_UUID} /* Sources */,\n"
        f"\t\t\t\t{FRAMEWORKS_PHASE_UUID} /* Frameworks */,\n"
        f"\t\t\t\t{RESOURCES_PHASE_UUID} /* Resources */,\n"
        f"\t\t\t);\n"
        f"\t\t\tbuildRules = (\n"
        f"\t\t\t);\n"
        f"\t\t\tdependencies = (\n"
        f"\t\t\t\t{DEPENDENCY_UUID} /* PBXTargetDependency */,\n"
        f"\t\t\t);\n"
        f"\t\t\tfileSystemSynchronizedGroups = (\n"
        f"\t\t\t\t{TEST_GROUP_UUID} /* SnapperTests */,\n"
        f"\t\t\t);\n"
        f"\t\t\tname = SnapperTests;\n"
        f"\t\t\tproductName = SnapperTests;\n"
        f"\t\t\tproductReference = {TEST_PRODUCT_UUID} /* SnapperTests.xctest */;\n"
        f'\t\t\tproductType = "com.apple.product-type.bundle.unit-test";\n'
        f"\t\t}};\n"
    )
    return content[:native_target_end] + test_target + content[native_target_end:]


def update_project_targets(content: str) -> str:
    """Add test target to project targets list.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the test target added to the targets list.
    """
    targets_match = re.search(r"targets = \(([^)]*)\);", content)
    if not targets_match:
        return content
    targets_content = targets_match.group(1)
    last_comma_pos = targets_content.rfind(",")
    if last_comma_pos == -1:
        return content
    new_target_entry = f"\n\t\t\t\t{TEST_TARGET_UUID} /* SnapperTests */,"
    new_targets_content = (
        targets_content[: last_comma_pos + 1]
        + new_target_entry
        + targets_content[last_comma_pos + 1 :]
    )
    return content[: targets_match.start(1)] + new_targets_content + content[targets_match.end(1) :]


def add_container_item_proxy(content: str, main_target_uuid: str) -> str:
    """Add container item proxy for target dependency.

    Args:
        content: The project.pbxproj file content.
        main_target_uuid: UUID of the main app target.

    Returns:
        Updated content with the container item proxy added.
    """
    if "/* Begin PBXContainerItemProxy section */" in content:
        return content
    insert_point = content.find("/* Begin PBXFileReference section */")
    project_uuid = content.split("/* Project object */")[0].strip().split()[-1]
    proxy_section = (
        f"/* Begin PBXContainerItemProxy section */\n"
        f"\t\t{PROXY_UUID} /* PBXContainerItemProxy */ = {{\n"
        f"\t\t\tisa = PBXContainerItemProxy;\n"
        f"\t\t\tcontainerPortal = {project_uuid} /* Project object */;\n"
        f"\t\t\tproxyType = 1;\n"
        f"\t\t\tremoteGlobalIDString = {main_target_uuid};\n"
        f"\t\t\tremoteInfo = Snapper;\n"
        f"\t\t}};\n"
        f"/* End PBXContainerItemProxy section */\n\n"
    )
    return content[:insert_point] + proxy_section + content[insert_point:]


def add_resources_phase(content: str) -> str:
    """Add resources build phase.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the resources build phase added.
    """
    resources_section_end = content.find("/* End PBXResourcesBuildPhase section */")
    resources_phase = (
        f"\t\t{RESOURCES_PHASE_UUID} /* Resources */ = {{\n"
        f"\t\t\tisa = PBXResourcesBuildPhase;\n"
        f"\t\t\tbuildActionMask = 2147483647;\n"
        f"\t\t\tfiles = (\n"
        f"\t\t\t);\n"
        f"\t\t\trunOnlyForDeploymentPostprocessing = 0;\n"
        f"\t\t}};\n"
    )
    return content[:resources_section_end] + resources_phase + content[resources_section_end:]


def add_sources_phase(content: str) -> str:
    """Add sources build phase.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the sources build phase added.
    """
    sources_section_start, sources_section_end = find_section_bounds(
        content, "PBXSourcesBuildPhase"
    )
    sources_content = content[sources_section_start:sources_section_end]
    first_entry_match = re.search(r"\n\t\t([A-F0-9]+) /\* Sources \*/", sources_content)
    sources_phase = (
        f"\t\t{SOURCES_PHASE_UUID} /* Sources */ = {{\n"
        f"\t\t\tisa = PBXSourcesBuildPhase;\n"
        f"\t\t\tbuildActionMask = 2147483647;\n"
        f"\t\t\tfiles = (\n"
        f"\t\t\t);\n"
        f"\t\t\trunOnlyForDeploymentPostprocessing = 0;\n"
        f"\t\t}};\n"
    )
    if first_entry_match and first_entry_match.group(1) > SOURCES_PHASE_UUID:
        insert_pos = sources_section_start + first_entry_match.start() + 1
        return content[:insert_pos] + sources_phase + content[insert_pos:]
    return content[:sources_section_end] + "\n" + sources_phase + content[sources_section_end:]


def add_target_dependency(content: str, main_target_uuid: str) -> str:
    """Add target dependency section.

    Args:
        content: The project.pbxproj file content.
        main_target_uuid: UUID of the main app target.

    Returns:
        Updated content with the target dependency section added.
    """
    if "/* Begin PBXTargetDependency section */" in content:
        return content
    insert_point = content.find("/* Begin XCBuildConfiguration section */")
    dependency_section = (
        f"/* Begin PBXTargetDependency section */\n"
        f"\t\t{DEPENDENCY_UUID} /* PBXTargetDependency */ = {{\n"
        f"\t\t\tisa = PBXTargetDependency;\n"
        f"\t\t\ttarget = {main_target_uuid} /* Snapper */;\n"
        f"\t\t\ttargetProxy = {PROXY_UUID} /* PBXContainerItemProxy */;\n"
        f"\t\t}};\n"
        f"/* End PBXTargetDependency section */\n\n"
    )
    return content[:insert_point] + dependency_section + content[insert_point:]


def add_build_configurations(content: str) -> str:
    """Add debug and release build configurations for test target.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with debug and release build configurations added.
    """
    build_config_section_start = content.find("/* Begin XCBuildConfiguration section */")
    test_debug_config = (
        f"\t\t{DEBUG_CONFIG_UUID} /* Debug */ = {{\n"
        f"\t\t\tisa = XCBuildConfiguration;\n"
        f"\t\t\tbuildSettings = {{\n"
        f'\t\t\t\tBUNDLE_LOADER = "$(TEST_HOST)";\n'
        f"\t\t\t\tCODE_SIGN_STYLE = Automatic;\n"
        f"\t\t\t\tDEVELOPMENT_TEAM = 26MP7QQP95;\n"
        f"\t\t\t\tGENERATE_INFOPLIST_FILE = YES;\n"
        f"\t\t\t\tPRODUCT_BUNDLE_IDENTIFIER = ie.klatt.snapper.tests;\n"
        f'\t\t\t\tPRODUCT_NAME = "$(TARGET_NAME)";\n'
        f'\t\t\t\tTEST_HOST = "$(BUILT_PRODUCTS_DIR)/Snapper.app/'
        f'$(BUNDLE_EXECUTABLE_FOLDER_PATH)/Snapper";\n'
        f"\t\t\t}};\n"
        f"\t\t\tname = Debug;\n"
        f"\t\t}};\n"
    )
    test_release_config = (
        f"\t\t{RELEASE_CONFIG_UUID} /* Release */ = {{\n"
        f"\t\t\tisa = XCBuildConfiguration;\n"
        f"\t\t\tbuildSettings = {{\n"
        f'\t\t\t\tBUNDLE_LOADER = "$(TEST_HOST)";\n'
        f"\t\t\t\tCODE_SIGN_STYLE = Automatic;\n"
        f"\t\t\t\tDEVELOPMENT_TEAM = 26MP7QQP95;\n"
        f"\t\t\t\tGENERATE_INFOPLIST_FILE = YES;\n"
        f"\t\t\t\tPRODUCT_BUNDLE_IDENTIFIER = ie.klatt.snapper.tests;\n"
        f'\t\t\t\tPRODUCT_NAME = "$(TARGET_NAME)";\n'
        f'\t\t\t\tTEST_HOST = "$(BUILT_PRODUCTS_DIR)/Snapper.app/'
        f'$(BUNDLE_EXECUTABLE_FOLDER_PATH)/Snapper";\n'
        f"\t\t\t}};\n"
        f"\t\t\tname = Release;\n"
        f"\t\t}};\n"
    )
    section_header_end = content.find("\n", build_config_section_start) + 1
    content = content[:section_header_end] + test_debug_config + content[section_header_end:]
    build_config_end = content.find("/* End XCBuildConfiguration section */")
    dad_match = re.search(
        r"\n\t\t(DAD[A-F0-9]+) /\* Release \*/",
        content[build_config_section_start:build_config_end],
    )
    if dad_match:
        insert_pos = build_config_section_start + dad_match.start() + 1
        return content[:insert_pos] + test_release_config + content[insert_pos:]
    return content[:build_config_end] + test_release_config + content[build_config_end:]


def add_configuration_list(content: str) -> str:
    """Add build configuration list for test target.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Updated content with the build configuration list added.
    """
    config_list_section_start = content.find("/* Begin XCConfigurationList section */")
    config_list_end = content.find("/* End XCConfigurationList section */")
    test_config_list = (
        f"\t\t{BUILD_CONFIG_LIST_UUID} "
        f'/* Build configuration list for PBXNativeTarget "SnapperTests" */ = {{\n'
        f"\t\t\tisa = XCConfigurationList;\n"
        f"\t\t\tbuildConfigurations = (\n"
        f"\t\t\t\t{DEBUG_CONFIG_UUID} /* Debug */,\n"
        f"\t\t\t\t{RELEASE_CONFIG_UUID} /* Release */,\n"
        f"\t\t\t);\n"
        f"\t\t\tdefaultConfigurationIsVisible = 0;\n"
        f"\t\t\tdefaultConfigurationName = Debug;\n"
        f"\t\t}};\n"
    )
    c2cb_match = re.search(
        r"\n\t\t(C2CB[A-F0-9]+) /\* Build configuration",
        content[config_list_section_start:config_list_end],
    )
    if c2cb_match:
        insert_pos = config_list_section_start + c2cb_match.start() + 1
        return content[:insert_pos] + test_config_list + content[insert_pos:]
    return content[:config_list_end] + test_config_list + content[config_list_end:]


def normalize_to_xcode_format(content: str) -> str:
    """Normalize project.pbxproj to match Xcode 16.1 canonical format.

    Args:
        content: The project.pbxproj file content.

    Returns:
        Normalized content matching Xcode 16.1 format.
    """
    content = content.replace("objectVersion = 77;", "objectVersion = 70;")
    content = "\n".join(
        "" if "preferredProjectObjectVersion" in line else line for line in content.split("\n")
    )
    content = re.sub(
        r"(40BE9EE10BEF49D1DCA0F3C5 /\* Snapper\.app \*/ = "
        r"\{isa = PBXFileReference; [^}]*?)lastKnownFileType",
        r"\1explicitFileType",
        content,
    )
    content = re.sub(
        r"(40BE9EE10BEF49D1DCA0F3C5 /\* Snapper\.app \*/ = "
        r"\{isa = PBXFileReference; )includeInIndex = 0; "
        r"(explicitFileType = wrapper\.application;)",
        r"\1\2 includeInIndex = 0;",
        content,
    )
    content = re.sub(
        r'\s*DevelopmentTeam = "";\n',
        "\n",
        content,
    )
    content = content.replace(
        'DEVELOPMENT_TEAM = "";',
        "DEVELOPMENT_TEAM = 26MP7QQP95;",
    )
    content = re.sub(
        r'(minimizedProjectReferenceProxies = 1;)\n\n(\s+projectDirPath = "";)',
        r"\1\n\2",
        content,
    )
    return content


def add_test_target(project_path: Path) -> str:
    """Add test target to Xcode project.

    Args:
        project_path: Path to the .xcodeproj directory

    Returns:
        The test target UUID

    Raises:
        ProjectError: If project file operations fail
    """
    content = read_pbxproj(project_path)
    if "SnapperTests" in content:
        print("Test target already exists")
        return TEST_TARGET_UUID
    main_target_uuid = find_main_target_uuid(content)
    content = add_file_reference(content)
    content = add_test_group(content)
    content = update_products_group(content)
    content = add_frameworks_phase(content)
    content = add_native_target(content)
    content = update_project_targets(content)
    content = add_container_item_proxy(content, main_target_uuid)
    content = add_resources_phase(content)
    content = add_sources_phase(content)
    content = add_target_dependency(content, main_target_uuid)
    content = add_build_configurations(content)
    content = add_configuration_list(content)
    content = normalize_to_xcode_format(content)
    write_pbxproj(project_path, content)
    print("Added SnapperTests target to project")
    return TEST_TARGET_UUID


def update_scheme(project_path: Path, test_target_uuid: str) -> None:
    """Add test target to Snapper.xcscheme.

    Args:
        project_path: Path to the .xcodeproj directory
        test_target_uuid: UUID of the test target
    """
    scheme_path = project_path / "xcshareddata" / "xcschemes" / "Snapper.xcscheme"
    if not scheme_path.exists():
        print("Scheme file not found, skipping")
        return
    ET.register_namespace("", "")
    tree = ET.parse(scheme_path)
    root = tree.getroot()
    test_action = root.find("TestAction")
    if test_action is None:
        print("TestAction not found in scheme")
        return
    testables = test_action.find("Testables")
    if testables is None:
        testables = ET.SubElement(test_action, "Testables")
    for testable in testables.findall("TestableReference"):
        buildable = testable.find("BuildableReference")
        if buildable is not None and buildable.get("BlueprintName") == "SnapperTests":
            print("Test target already in scheme")
            return
    testable_ref = ET.SubElement(testables, "TestableReference")
    testable_ref.set("skipped", "NO")
    buildable_ref = ET.SubElement(testable_ref, "BuildableReference")
    buildable_ref.set("BuildableIdentifier", "primary")
    buildable_ref.set("BlueprintIdentifier", test_target_uuid)
    buildable_ref.set("BuildableName", "SnapperTests.xctest")
    buildable_ref.set("BlueprintName", "SnapperTests")
    buildable_ref.set("ReferencedContainer", "container:Snapper.xcodeproj")
    tree.write(scheme_path, encoding="utf-8", xml_declaration=True)
    print("Added test target to scheme")


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    if len(sys.argv) > 1:
        project_path = Path(sys.argv[1])
    else:
        project_path = Path(__file__).parent.parent / "ios" / "Snapper.xcodeproj"
    if not project_path.exists():
        print(f"Error: Project not found at {project_path}")
        return 1
    try:
        test_target_uuid = add_test_target(project_path)
        update_scheme(project_path, test_target_uuid)
        print("\nTest target added successfully!")
        print("   Tests are ready to run with: xcodebuild test -scheme Snapper")
        return 0
    except ProjectError as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
