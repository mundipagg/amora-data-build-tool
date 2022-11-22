from pathlib import Path

import feffery_antd_components as antd
from dash.development.base_component import Component

from amora.config import settings


def _make_tree(root: Path) -> dict:
    """
    Generates a tree from a path.
    Node schema compatible with ant.design's TreeSelect component

    More on: https://ant.design/components/tree-select
    """
    if not root.is_dir() and not root.stem.startswith("_"):
        return {
            "title": root.name,
            "key": root.as_posix(),
            "icon": "antd-file",
            "children": [],
        }
    else:
        return {
            "title": root.name,
            "key": root.as_posix(),
            "icon": "antd-folder",
            "disabled": True,
            "children": [
                _make_tree(file)
                for file in root.iterdir()
                if not file.stem.startswith("_")
            ],
        }


component_id = "file-browser-tree"


def layout() -> Component:
    return antd.AntdTree(
        id=component_id,
        defaultExpandAll=True,
        showIcon=True,
        treeData=[_make_tree(settings.models_path)],
    )
