from typing import Dict

from feast import FeatureView

from amora.models import Model

FEATURE_REGISTRY: Dict[Model, FeatureView] = {}
