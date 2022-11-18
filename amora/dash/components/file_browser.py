import os
from pathlib import Path

import feffery_antd_components as antd
from dash.development.base_component import Component

from amora.config import settings


def make_tree(root: Path) -> dict:
    if root.is_dir():
        children = root.iterdir()
    else:
        children = []
    return {"title": Path(root).stem, "key": root.as_posix(), "children": children}


component_id = "file-browser-tree"


def layout() -> Component:
    tree = list(os.walk(settings.models_path))
    tree_data = [
        {
            "title": Path(dirpath).stem,
            "key": dirpath,
            "children": [
                {
                    "title": file,
                    "key": Path(dirpath).joinpath(file).as_posix(),
                    "icon": "antd-table",
                }
                for file in filenames
            ],
        }
        for (dirpath, dirnames, filenames) in tree
    ]

    return antd.AntdTree(
        treeData=tree_data,
        id=component_id,
        defaultExpandAll=True,
    )
