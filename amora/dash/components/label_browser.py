import feffery_antd_components as antd
from dash.development.base_component import Component

from amora.models import labels_to_models_dict

component_id = "label-browser-tree"


def layout() -> Component:
    tree_data = [
        {
            "key": "labels",
            "title": "🏷 Labels",
            "children": [
                {
                    "key": str(label),
                    "title": str(label),
                    "children": [
                        {"key": path.as_posix(), "title": path.stem}
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
