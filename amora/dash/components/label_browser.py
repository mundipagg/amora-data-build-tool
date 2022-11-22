import feffery_antd_components as antd
from dash.development.base_component import Component

from amora.models import labels_to_models_dict

component_id = "label-browser-tree"


def layout() -> Component:
    tree_data = [
        {
            "key": "labels",
            "title": "🏷 Labels",
            "disabled": True,
            "children": [
                {
                    "key": str(label),
                    "title": str(label),
                    "disabled": True,
                    "children": [
                        {
                            "key": path.as_posix(),
                            "title": path.name,
                            "icon": "antd-file",
                        }
                        for model, path in models
                    ],
                }
                for label, models in labels_to_models_dict().items()
            ],
        }
    ]

    return antd.AntdTree(
        treeData=tree_data,
        id=component_id,
        defaultExpandAll=True,
    )
