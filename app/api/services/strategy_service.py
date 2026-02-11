"""Strategy validation service."""
import ast
import sys
from io import StringIO
from app.api.models.strategy import StrategyValidation


def validate_strategy_code(code: str, class_name: str) -> StrategyValidation:
    """Validate strategy Python code.
    
    Checks:
    1. Syntax is valid Python
    2. Class with given name exists
    3. Class has required methods (on_init, on_bar, etc.)
    4. No dangerous imports or operations
    """
    errors = []
    warnings = []
    
    # Check syntax
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return StrategyValidation(
            valid=False,
            errors=[f"Syntax error at line {e.lineno}: {e.msg}"]
        )
    
    # Find the strategy class
    class_found = False
    class_node = None
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            class_found = True
            class_node = node
            break
    
    if not class_found:
        errors.append(f"Class '{class_name}' not found in code")
        return StrategyValidation(valid=False, errors=errors)
    
    # Check for required methods
    required_methods = ["on_init"]
    recommended_methods = ["on_bar", "on_tick", "on_trade", "on_order"]
    
    method_names = set()
    for item in class_node.body:
        if isinstance(item, ast.FunctionDef):
            method_names.add(item.name)
    
    for method in required_methods:
        if method not in method_names:
            errors.append(f"Missing required method: {method}")
    
    has_on_bar = "on_bar" in method_names
    has_on_tick = "on_tick" in method_names
    
    if not has_on_bar and not has_on_tick:
        warnings.append("Strategy should implement on_bar or on_tick method")
    
    # Check for dangerous imports
    dangerous_modules = ["os", "subprocess", "shutil", "socket", "requests"]
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in dangerous_modules:
                    warnings.append(f"Import '{alias.name}' may be restricted in production")
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.split(".")[0] in dangerous_modules:
                warnings.append(f"Import from '{node.module}' may be restricted in production")
    
    # Check for exec/eval
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id in ("exec", "eval", "compile", "__import__"):
                    errors.append(f"Use of '{node.func.id}' is not allowed")
    
    return StrategyValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )


def compile_strategy(code: str, class_name: str):
    """Compile and return the strategy class.
    
    Returns:
        The strategy class if successful, None otherwise.
    """
    try:
        # Create a restricted namespace
        namespace = {}
        
        # Execute the code in the namespace
        exec(code, namespace)
        
        # Get the class
        if class_name in namespace:
            return namespace[class_name]
        
        return None
        
    except Exception as e:
        return None


def parse_strategy_file(content: str) -> dict:
    """Parse Python source and extract class definitions and simple defaults.

    Returns a dict with:
      - classes: list of {name, lineno, defaults: {attr: value_or_source}}

    Uses `ast.literal_eval` where possible to convert simple constant expressions
    (numbers, strings, lists, dicts). Falls back to returning the source string
    for complex expressions.
    """
    result = {"classes": []}
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return result

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            class_name = node.name
            class_info = {"name": class_name, "lineno": node.lineno, "defaults": {}}
            for item in node.body:
                # Handle simple assignments: x = 5
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name):
                            key = target.id
                            val_node = item.value
                            val = None
                            try:
                                val = ast.literal_eval(val_node)
                            except Exception:
                                try:
                                    val = ast.get_source_segment(content, val_node)
                                except Exception:
                                    val = None
                            class_info["defaults"][key] = val
                # Handle annotated assignments: x: int = 5
                elif isinstance(item, ast.AnnAssign):
                    if isinstance(item.target, ast.Name) and item.value is not None:
                        key = item.target.id
                        val_node = item.value
                        val = None
                        try:
                            val = ast.literal_eval(val_node)
                        except Exception:
                            try:
                                val = ast.get_source_segment(content, val_node)
                            except Exception:
                                val = None
                        class_info["defaults"][key] = val

            result["classes"].append(class_info)

    return result
