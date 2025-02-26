# This file is maintained for backward compatibility
# It imports and re-exports the ONNXMiniLM_L6_V2 from the new location

from chromadb.embedding_functions.onnx_mini_lm_l6_v2 import ONNXMiniLM_L6_V2

# Re-export everything for backward compatibility
__all__ = ["ONNXMiniLM_L6_V2"]
