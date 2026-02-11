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
    """Parse Python source and extract class definitions with parameter defaults.

    For VNPy strategies, extracts the 'parameters' list and returns defaults
    ONLY for attributes listed in that array. Falls back to all class attributes
    if no parameters list is found.

    Returns:
        dict with 'classes' list, each containing:
            - name: class name
            - lineno: line number
            - defaults: {param_name: default_value} for parameters only
    """
    result = {"classes": []}
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return result

    # Helper to extract value from AST node
    def extract_value(val_node):
        try:
            return ast.literal_eval(val_node)
        except Exception:
            try:
                return ast.get_source_segment(content, val_node)
            except Exception:
                return None

    # Step 1: Collect module-level assignments (for defaults lookup)
    module_defaults = {}
    module_parameters = None
    
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    key = target.id
                    if key in ("parameters", "parameter"):
                        # Extract module-level parameters list
                        module_parameters = extract_value(node.value)
                    else:
                        # Store other module-level assignments
                        module_defaults[key] = extract_value(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            key = node.target.id
            if node.value and key not in ("parameters", "parameter", "variables"):
                module_defaults[key] = extract_value(node.value)

    # Step 2: Process each class
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
            
        class_name = node.name
        class_info = {"name": class_name, "lineno": node.lineno, "defaults": {}}
        
        # Step 2a: Find class-level parameters list
        class_parameters = None
        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name) and target.id in ("parameters", "parameter"):
                        class_parameters = extract_value(item.value)
                        break
                if class_parameters is not None:
                    break
        
        # Use class parameters if found, otherwise fall back to module
        parameters_list = class_parameters if class_parameters is not None else module_parameters
        
        # Step 2b: Collect ALL available defaults (class attrs + self assignments)
        available_defaults = {}
        
        # Collect class-level attribute assignments
        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        key = target.id
                        if key not in ("parameters", "parameter", "variables"):
                            available_defaults[key] = extract_value(item.value)
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                key = item.target.id
                if item.value and key not in ("parameters", "parameter", "variables"):
                    available_defaults[key] = extract_value(item.value)
            elif isinstance(item, ast.FunctionDef):
                # Extract self.x = value assignments from methods
                for sub in ast.walk(item):
                    if isinstance(sub, ast.Assign):
                        for t in sub.targets:
                            if (isinstance(t, ast.Attribute) and 
                                isinstance(t.value, ast.Name) and 
                                t.value.id == 'self'):
                                available_defaults[t.attr] = extract_value(sub.value)
        
        # Merge module defaults (class-level overrides module-level)
        for key, value in module_defaults.items():
            if key not in available_defaults:
                available_defaults[key] = value
        
        # Step 2c: Determine parameter order and build defaults in that order
        parameter_order = []
        if parameters_list is not None:
            # Explicit parameters specified in the class/module
            if isinstance(parameters_list, dict):
                # Dict format: {param: default}
                parameter_order = list(parameters_list.keys())
                for name in parameter_order:
                    # Use explicit default from the dict when present, else fall back
                    class_info["defaults"][name] = parameters_list.get(name, available_defaults.get(name, None))
            elif isinstance(parameters_list, (list, tuple)):
                for entry in parameters_list:
                    if isinstance(entry, str):
                        parameter_order.append(entry)
                        class_info["defaults"][entry] = available_defaults.get(entry, None)
                    elif isinstance(entry, (list, tuple)) and len(entry) >= 1:
                        param_name = entry[0]
                        param_default = entry[1] if len(entry) > 1 else available_defaults.get(param_name, None)
                        parameter_order.append(param_name)
                        class_info["defaults"][param_name] = param_default
        else:
            # No explicit parameters list - preserve the order of discovered defaults
            parameter_order = list(available_defaults.keys())
            for name in parameter_order:
                class_info["defaults"][name] = available_defaults.get(name)

        # Expose the explicit parameter order for consumers that need sequence
        class_info["parameter_order"] = parameter_order
        
        result["classes"].append(class_info)

    return result
