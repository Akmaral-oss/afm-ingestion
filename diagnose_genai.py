import os
import sys

try:
    import google.genai
    from google.genai import types
    from pydantic import ValidationError
    import inspect

    print(f"SDK Version: {google.genai.__version__}")
    
    print("\n--- ThinkingConfig Inspection ---")
    try:
        # Check if thinking_level exists in the schema
        fields = types.ThinkingConfig.model_fields
        print(f"Available fields in ThinkingConfig: {list(fields.keys())}")
        
        if 'thinking_level' in fields:
            print("SUCCESS: 'thinking_level' is found in the class definition.")
            # Check the expected type
            field_info = fields['thinking_level']
            print(f"Field type: {field_info.annotation}")
        else:
            print("FAILURE: 'thinking_level' is MISSING from ThinkingConfig.")
            
    except AttributeError:
        print("Could not access model_fields (maybe not a Pydantic v2 model?)")
        # Fallback to inspect
        members = [m[0] for m in inspect.getmembers(types.ThinkingConfig)]
        print(f"Class members: {members}")

    print("\n--- Validation Test ---")
    try:
        # Attempt to initialize with different values
        config = types.ThinkingConfig(thinking_level="LOW")
        print("Test with string 'LOW': SUCCESS")
    except Exception as e:
        print(f"Test with string 'LOW': FAILED - {e}")

    try:
        # Check if the Enum exists
        if hasattr(types, 'ThinkingLevel'):
            print(f"ThinkingLevel Enum found. Options: {[e.name for e in types.ThinkingLevel]}")
            config = types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)
            print("Test with Enum types.ThinkingLevel.LOW: SUCCESS")
        else:
            print("ThinkingLevel Enum NOT found in types.")
    except Exception as e:
        print(f"Test with Enum: FAILED - {e}")

except ImportError:
    print("google-genai is not installed in the environment.")
except Exception as e:
    print(f"Unexpected error: {e}")
